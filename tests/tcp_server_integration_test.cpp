#include "snf/net/socket_options.hpp"
#include "snf/net/termination_signal.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/game_server.hpp"

#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
#include <numeric>
#include <optional>
#include <poll.h>
#include <span>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace
{
    using namespace std::chrono_literals;

    class RecordingFrameIngress final : public snf::server::FrameIngress
    {
    public:
        [[nodiscard]] snf::server::FramePostResult tryPost(snf::server::FrameEnvelope) override
        {
            return snf::server::FramePostResult::Closed;
        }

        [[nodiscard]] snf::server::PostResult
        tryPostConnectionClosed(snf::server::ConnectionClosed closed) override
        {
            connection_closes.push_back(closed);
            const std::size_t attempt = lifecycle_attempts.fetch_add(1);
            return attempt < lifecycle_results.size() ? lifecycle_results[attempt]
                                                      : lifecycle_fallback;
        }

        void close() noexcept override
        {
            closed = true;
        }

        void cancel() noexcept override
        {
            cancelled = true;
        }

        bool closed{false};
        bool cancelled{false};
        std::vector<snf::server::PostResult> lifecycle_results;
        snf::server::PostResult lifecycle_fallback{snf::server::PostResult::Accepted};
        std::vector<snf::server::ConnectionClosed> connection_closes;
        std::atomic<std::size_t> lifecycle_attempts{0};
    };

    snf::net::UniqueFileDescriptor make_eventfd()
    {
        const int descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        assert(descriptor != -1);
        return snf::net::UniqueFileDescriptor{descriptor};
    }

    class RunningServer
    {
    public:
        explicit RunningServer(
            const int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD)
            : RunningServer(
                  snf::server::GameServerConfig{
                      .port = 0,
                      .shutdown_grace_period = 200ms,
                      .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                      .client_send_buffer_size = std::nullopt,
                  },
                  termination_signal_descriptor)
        {
        }

        explicit RunningServer(
            snf::server::GameServerConfig config,
            const int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD)
            : _server(std::move(config))
            , _termination_signal_descriptor(termination_signal_descriptor)
            , _thread(
                  [this]
                  {
                      try
                      {
                          _server.run(_termination_signal_descriptor);
                      }
                      catch (...)
                      {
                          _server_error = std::current_exception();
                      }
                  })
        {
        }

        ~RunningServer()
        {
            if (_thread.joinable())
            {
                _server.requestStop();
                _thread.join();
            }
        }

        RunningServer(const RunningServer&) = delete;
        RunningServer& operator=(const RunningServer&) = delete;

        [[nodiscard]] std::uint16_t getPort() const noexcept
        {
            return _server.getPort();
        }

        [[nodiscard]] const snf::server::GameServerStats& getStats() const noexcept
        {
            return _server.getStats();
        }

        [[nodiscard]] snf::runtime::ActorRuntimeStats getActorRuntimeStats() const
        {
            return _server.getActorRuntimeStats();
        }

        // Reads reactor state, so tests may only call it once the reactor thread
        // has been joined.
        [[nodiscard]] snf::server::ServerMetricsSnapshot getMetricsSnapshot() const
        {
            return _server.getMetricsSnapshot();
        }

        void stop()
        {
            _server.requestStop();
            join();
        }

        void join()
        {
            _thread.join();

            if (_server_error)
            {
                std::rethrow_exception(_server_error);
            }
        }

    private:
        snf::server::GameServer _server;
        int _termination_signal_descriptor;
        std::exception_ptr _server_error;
        std::thread _thread;
    };

    snf::net::UniqueFileDescriptor
    connect_client(const std::uint16_t port,
                   const std::optional<int> receive_buffer_size = std::nullopt)
    {
        snf::net::UniqueFileDescriptor client_socket{
            ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
        assert(client_socket.isValid());

        timeval timeout{
            .tv_sec = 2,
            .tv_usec = 0,
        };

        assert(::setsockopt(client_socket.getDescriptor(),
                            SOL_SOCKET,
                            SO_RCVTIMEO,
                            &timeout,
                            sizeof(timeout)) == 0);

        if (receive_buffer_size)
        {
            assert(::setsockopt(client_socket.getDescriptor(),
                                SOL_SOCKET,
                                SO_RCVBUF,
                                &*receive_buffer_size,
                                sizeof(*receive_buffer_size)) == 0);
        }
        assert(::setsockopt(client_socket.getDescriptor(),
                            SOL_SOCKET,
                            SO_SNDTIMEO,
                            &timeout,
                            sizeof(timeout)) == 0);

        snf::net::enable_tcp_no_delay(client_socket.getDescriptor());

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(port);
        assert(::inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) == 1);

        assert(::connect(client_socket.getDescriptor(),
                         reinterpret_cast<const sockaddr*>(&server_address),
                         sizeof(server_address)) == 0);

        return client_socket;
    }

    void send_all(const int socket_descriptor, const std::span<const std::byte> bytes)
    {
        std::size_t sent_byte_count = 0;

        while (sent_byte_count < bytes.size())
        {
            const auto result = ::send(socket_descriptor,
                                       bytes.data() + sent_byte_count,
                                       bytes.size() - sent_byte_count,
                                       MSG_NOSIGNAL);

            if (result > 0)
            {
                sent_byte_count += static_cast<std::size_t>(result);
                continue;
            }

            if (result == -1 && errno == EINTR)
            {
                continue;
            }

            assert(false);
        }
    }

    void send_until_complete_or_closed(const int socket_descriptor,
                                       const std::span<const std::byte> bytes)
    {
        std::size_t sent_byte_count = 0;

        while (sent_byte_count < bytes.size())
        {
            const auto result = ::send(socket_descriptor,
                                       bytes.data() + sent_byte_count,
                                       bytes.size() - sent_byte_count,
                                       MSG_NOSIGNAL);

            if (result > 0)
            {
                sent_byte_count += static_cast<std::size_t>(result);
                continue;
            }

            if (result == -1 && errno == EINTR)
            {
                continue;
            }

            if (result == -1 && (errno == EPIPE || errno == ECONNRESET))
            {
                return;
            }

            assert(false);
        }
    }

    std::vector<std::byte> receive_exact(const int socket_descriptor,
                                         const std::size_t expected_byte_count)
    {
        std::vector<std::byte> received_bytes(expected_byte_count);
        std::size_t received_byte_count = 0;

        while (received_byte_count < expected_byte_count)
        {
            const auto result = ::recv(socket_descriptor,
                                       received_bytes.data() + received_byte_count,
                                       expected_byte_count - received_byte_count,
                                       0);

            if (result > 0)
            {
                received_byte_count += static_cast<std::size_t>(result);
                continue;
            }

            if (result == -1 && errno == EINTR)
            {
                continue;
            }

            // Names itself, because a receive that times out here is otherwise
            // indistinguishable from any other failed assertion in the suite.
            assert(false && "receive_exact: recv failed or timed out");
        }

        return received_bytes;
    }

    void assert_pong(const std::vector<std::byte>& response, const snf::protocol::Frame& request)
    {
        snf::protocol::FrameDecoder decoder;
        const auto result = decoder.append(response);

        assert(result.ok());
        assert(result.frames.size() == 1);
        assert(result.frames[0].type == snf::protocol::MessageType::Pong);
        assert(result.frames[0].request_id == request.request_id);
        assert(result.frames[0].payload == request.payload);
    }

    std::vector<std::byte> player_id_payload(const std::uint64_t value)
    {
        std::vector<std::byte> payload(8);
        std::uint64_t remaining = value;
        for (std::size_t index = payload.size(); index > 0; --index)
        {
            payload[index - 1] = static_cast<std::byte>(remaining & 0xFFU);
            remaining >>= 8U;
        }
        return payload;
    }

    snf::protocol::Frame authentication_frame(const std::uint32_t request_id,
                                              const std::uint64_t player_id)
    {
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Authenticate,
            .request_id = request_id,
            .payload = player_id_payload(player_id),
        };
    }

    void assert_authenticated(const std::vector<std::byte>& response,
                              const std::uint32_t request_id,
                              const std::uint64_t player_id)
    {
        snf::protocol::FrameDecoder decoder;
        const auto result = decoder.append(response);

        assert(result.ok());
        assert(result.frames.size() == 1);
        assert(result.frames[0].type == snf::protocol::MessageType::Authenticated);
        assert(result.frames[0].request_id == request_id);
        assert(result.frames[0].payload == player_id_payload(player_id));
    }

    void receive_until_closed(const int socket_descriptor)
    {
        std::array<std::byte, 65536> receive_buffer{};

        while (true)
        {
            const auto result =
                ::recv(socket_descriptor, receive_buffer.data(), receive_buffer.size(), 0);

            if (result > 0)
            {
                continue;
            }

            if (result == 0 || (result == -1 && errno == ECONNRESET))
            {
                return;
            }

            if (result == -1 && errno == EINTR)
            {
                continue;
            }

            assert(false);
        }
    }

    std::size_t actor_count(const snf::runtime::ActorRuntimeStats& stats)
    {
        return std::accumulate(
            stats.workers.begin(),
            stats.workers.end(),
            std::size_t{0},
            [](const std::size_t total, const snf::runtime::ActorRuntimeWorkerStats& worker)
            { return total + worker.actor_count; });
    }

    std::uint64_t evicted_actor_count(const snf::runtime::ActorRuntimeStats& stats)
    {
        return std::accumulate(
            stats.workers.begin(),
            stats.workers.end(),
            std::uint64_t{0},
            [](const std::uint64_t total, const snf::runtime::ActorRuntimeWorkerStats& worker)
            { return total + worker.evicted_actors; });
    }

    std::uint64_t queue_wait_sample_count(const snf::runtime::ActorRuntimeStats& stats)
    {
        return std::accumulate(
            stats.workers.begin(),
            stats.workers.end(),
            std::uint64_t{0},
            [](const std::uint64_t total, const snf::runtime::ActorRuntimeWorkerStats& worker)
            { return total + worker.queue_wait_nanoseconds.sample_count; });
    }

    std::uint64_t suspended_command_count(const snf::runtime::ActorRuntimeStats& stats)
    {
        return std::accumulate(
            stats.workers.begin(),
            stats.workers.end(),
            std::uint64_t{0},
            [](const std::uint64_t total, const snf::runtime::ActorRuntimeWorkerStats& worker)
            { return total + worker.suspended_commands; });
    }

    void test_saturated_outbound_answers_every_request_and_still_drains()
    {
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 2s,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
            .actor_worker_count = 1,
            .actor_queue_capacity_per_worker = 64,
            .outbound_queue_capacity = 1,
        }};
        const auto client = connect_client(server.getPort());

        // Kept small on purpose: every request needs its own reactor turn to be
        // granted, and the client's receive timeout bounds each wait.
        constexpr std::uint32_t REQUEST_COUNT = 8;
        std::vector<std::byte> bundled_requests;
        for (std::uint32_t request_id = 1; request_id <= REQUEST_COUNT; ++request_id)
        {
            const auto encoded = snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::Ping,
                .request_id = request_id,
                .payload = {},
            });
            bundled_requests.insert(bundled_requests.end(), encoded.begin(), encoded.end());
        }

        send_all(client.getDescriptor(), bundled_requests);

        // A channel with one slot makes nearly every command wait for capacity. Every
        // response still arrives, in order: the actor suspends and the reactor grants
        // as it drains, so no response is dropped and no Worker blocks.
        for (std::uint32_t request_id = 1; request_id <= REQUEST_COUNT; ++request_id)
        {
            const snf::protocol::Frame request{
                .type = snf::protocol::MessageType::Ping,
                .request_id = request_id,
                .payload = {},
            };
            assert_pong(
                receive_exact(client.getDescriptor(), snf::protocol::encode_frame(request).size()),
                request);
        }

        server.stop();

        const auto metrics = server.getMetricsSnapshot();
        assert(metrics.counters.outbound_admission_failures == 0);
        assert(metrics.counters.actor_queue_overflows == 0);
        // Graceful shutdown completed even though actor drain depended on the reactor
        // continuing to consume and grant.
        assert(metrics.network.pending_outbound_reservations == 0);
        assert(metrics.network.reserved_outbound_slots == 0);
        assert(metrics.network.current_outbound_queue_depth == 0);
        assert(actor_count(server.getActorRuntimeStats()) == 0);
        assert(suspended_command_count(server.getActorRuntimeStats()) > 0);
        // One terminal per command, including the ones that had to wait for capacity.
        assert(metrics.command_terminals == REQUEST_COUNT);
    }

    void test_collects_baseline_saturation_metrics_for_a_round_trip()
    {
        RunningServer server;
        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 1,
            .payload = {std::byte{0x01}},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);
        send_all(client.getDescriptor(), encoded_request);
        assert_pong(receive_exact(client.getDescriptor(), encoded_request.size()), request);

        server.stop();

        const auto metrics = server.getMetricsSnapshot();
        assert(metrics.counters.received_frames == 1);
        assert(metrics.network.reactor_turn_nanoseconds.sample_count > 0);
        assert(metrics.network.reactor_turn_nanoseconds.max >=
               metrics.network.reactor_turn_nanoseconds.p99);
        // One drain observation for the PONG hand-off and one pending send sample
        // for the frame it enqueued.
        assert(metrics.network.outbound_queue_depth.sample_count > 0);
        assert(metrics.network.session_pending_send_bytes.sample_count == 1);
        // The single hand-off from the Logic Worker to the reactor.
        assert(metrics.network.outbound_queue_wait_nanoseconds.sample_count == 1);
        assert(metrics.network.outbound_queue_wait_nanoseconds.max > 0);
        assert(metrics.network.session_pending_send_bytes.max == encoded_request.size());
        assert(metrics.network.outbound_queue_high_water_mark >= 1);
        // Every session is closed before the reactor loop returns.
        assert(metrics.network.session_count == 0);
        assert(metrics.network.sessions_with_pending_send == 0);
        assert(metrics.network.total_pending_send_bytes == 0);
        assert(queue_wait_sample_count(metrics.actor_runtime) == 1);
        assert(metrics.command_terminals == 1);
        // Nothing waited for capacity, so the round trip started no async operation.
        assert(suspended_command_count(metrics.actor_runtime) == 0);
        assert(metrics.network.reserved_outbound_slots == 0);
        assert(metrics.network.pending_outbound_reservations == 0);
    }

    void test_reports_metrics_periodically_while_running()
    {
        std::atomic<std::size_t> report_count{0};
        std::atomic<std::uint64_t> reported_reactor_turns{0};
        std::atomic<std::size_t> reported_sessions{0};

        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .metrics_report_interval = 5ms,
            .metrics_reporter =
                [&](const snf::server::ServerMetricsSnapshot& metrics)
            {
                reported_reactor_turns.store(metrics.network.reactor_turn_nanoseconds.sample_count);
                reported_sessions.store(metrics.network.session_count);
                report_count.fetch_add(1);
            },
        }};

        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 7,
            .payload = {std::byte{0x02}},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);
        send_all(client.getDescriptor(), encoded_request);
        assert_pong(receive_exact(client.getDescriptor(), encoded_request.size()), request);

        // An idle reactor must still reach its report deadline, so this waits on
        // the interval rather than on further traffic.
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (report_count.load() < 3 && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(1ms);
        }

        assert(report_count.load() >= 3);
        assert(reported_reactor_turns.load() > 0);
        assert(reported_sessions.load() == 1);

        server.stop();
    }

    void test_returns_pong_for_ping()
    {
        RunningServer server;
        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 1,
            .payload = {std::byte{0xAA}, std::byte{0xBB}},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);
        send_all(client.getDescriptor(), encoded_request);

        const auto response = receive_exact(client.getDescriptor(), encoded_request.size());
        assert_pong(response, request);

        server.stop();

        const auto& stats = server.getStats();
        assert(stats.accepted_connections == 1);
        assert(stats.closed_connections == 1);
        assert(stats.received_frames == 1);
        assert(stats.sent_frames == 1);
        assert(stats.protocol_errors == 0);
    }

    void test_authenticates_one_session_and_allows_reconnect_after_passivation()
    {
        RunningServer server;
        constexpr std::uint64_t player_id = 77;

        auto first = connect_client(server.getPort());
        const auto first_auth = authentication_frame(100, player_id);
        const auto first_auth_bytes = snf::protocol::encode_frame(first_auth);
        send_all(first.getDescriptor(), first_auth_bytes);
        assert_authenticated(receive_exact(first.getDescriptor(), first_auth_bytes.size()),
                             first_auth.request_id,
                             player_id);

        const auto ping = snf::protocol::Frame{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 101,
            .payload = {std::byte{0xAA}},
        };
        const auto ping_bytes = snf::protocol::encode_frame(ping);
        send_all(first.getDescriptor(), ping_bytes);
        assert_pong(receive_exact(first.getDescriptor(), ping_bytes.size()), ping);

        const auto duplicate = connect_client(server.getPort());
        const auto duplicate_auth = authentication_frame(102, player_id);
        send_all(duplicate.getDescriptor(), snf::protocol::encode_frame(duplicate_auth));
        receive_until_closed(duplicate.getDescriptor());

        first.init();
        const auto passivation_deadline = std::chrono::steady_clock::now() + 1s;
        while (actor_count(server.getActorRuntimeStats()) != 0 &&
               std::chrono::steady_clock::now() < passivation_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(actor_count(server.getActorRuntimeStats()) == 0);

        const auto reconnected = connect_client(server.getPort());
        const auto reconnect_auth = authentication_frame(103, player_id);
        const auto reconnect_bytes = snf::protocol::encode_frame(reconnect_auth);
        send_all(reconnected.getDescriptor(), reconnect_bytes);
        assert_authenticated(receive_exact(reconnected.getDescriptor(), reconnect_bytes.size()),
                             reconnect_auth.request_id,
                             player_id);

        server.stop();
    }

    void test_peer_disconnect_evicts_the_player_actor()
    {
        RunningServer server;
        {
            const auto client = connect_client(server.getPort());
            const snf::protocol::Frame request{
                .type = snf::protocol::MessageType::Ping,
                .request_id = 11,
                .payload = {},
            };
            const auto encoded_request = snf::protocol::encode_frame(request);
            send_all(client.getDescriptor(), encoded_request);
            assert_pong(receive_exact(client.getDescriptor(), encoded_request.size()), request);
        }

        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (actor_count(server.getActorRuntimeStats()) != 0 &&
               std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(5ms);
        }

        const auto stats = server.getActorRuntimeStats();
        assert(actor_count(stats) == 0);
        assert(evicted_actor_count(stats) == 1);
        server.stop();
    }

    void test_decodes_ping_sent_one_byte_at_a_time()
    {
        RunningServer server;
        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 2,
            .payload = {std::byte{0x10}, std::byte{0x20}, std::byte{0x30}},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);

        for (const std::byte byte : encoded_request)
        {
            send_all(client.getDescriptor(), std::span<const std::byte>{&byte, 1});
        }

        const auto response = receive_exact(client.getDescriptor(), encoded_request.size());
        assert_pong(response, request);

        server.stop();
    }

    void test_decodes_multiple_pings_from_one_send()
    {
        RunningServer server;
        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame first_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 3,
            .payload = {std::byte{0x01}},
        };
        const snf::protocol::Frame second_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 4,
            .payload = {std::byte{0x02}, std::byte{0x03}},
        };

        const auto first_encoded_request = snf::protocol::encode_frame(first_request);
        const auto second_encoded_request = snf::protocol::encode_frame(second_request);

        std::vector<std::byte> bundled_requests = first_encoded_request;
        bundled_requests.insert(
            bundled_requests.end(), second_encoded_request.begin(), second_encoded_request.end());

        send_all(client.getDescriptor(), bundled_requests);

        const auto response = receive_exact(client.getDescriptor(), bundled_requests.size());
        snf::protocol::FrameDecoder decoder;
        const auto result = decoder.append(response);

        assert(result.ok());
        assert(result.frames.size() == 2);
        assert(result.frames[0].type == snf::protocol::MessageType::Pong);
        assert(result.frames[0].request_id == first_request.request_id);
        assert(result.frames[0].payload == first_request.payload);
        assert(result.frames[1].type == snf::protocol::MessageType::Pong);
        assert(result.frames[1].request_id == second_request.request_id);
        assert(result.frames[1].payload == second_request.payload);

        server.stop();
    }

    void test_survives_client_close_during_partial_frame()
    {
        RunningServer server;

        {
            const auto partial_client = connect_client(server.getPort());
            const snf::protocol::Frame incomplete_request{
                .type = snf::protocol::MessageType::Ping,
                .request_id = 5,
                .payload = {std::byte{0xAA}},
            };

            const auto encoded_request = snf::protocol::encode_frame(incomplete_request);
            send_all(partial_client.getDescriptor(),
                     std::span<const std::byte>{encoded_request}.first(5));
        }

        const auto next_client = connect_client(server.getPort());
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 6,
            .payload = {},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);
        send_all(next_client.getDescriptor(), encoded_request);
        const auto response = receive_exact(next_client.getDescriptor(), encoded_request.size());
        assert_pong(response, request);

        server.stop();
    }

    void test_closes_connection_for_an_unregistered_message()
    {
        RunningServer server;
        const auto invalid_client = connect_client(server.getPort());
        const snf::protocol::Frame invalid_request{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 7,
            .payload = {},
        };

        send_all(invalid_client.getDescriptor(), snf::protocol::encode_frame(invalid_request));
        receive_until_closed(invalid_client.getDescriptor());

        const auto healthy_client = connect_client(server.getPort());
        const snf::protocol::Frame healthy_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 8,
            .payload = {},
        };
        const auto healthy_encoded_request = snf::protocol::encode_frame(healthy_request);
        send_all(healthy_client.getDescriptor(), healthy_encoded_request);
        assert_pong(receive_exact(healthy_client.getDescriptor(), healthy_encoded_request.size()),
                    healthy_request);

        server.stop();
        assert(server.getStats().protocol_errors >= 1);
        // Worker-owned wrappers are destroyed before the Logic Runtime worker
        // exits, so a completed shutdown retains no inactive Player slot.
        assert(actor_count(server.getActorRuntimeStats()) == 0);
    }

    void test_overflowed_actor_queue_closes_only_that_connection()
    {
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
            .actor_worker_count = 2,
            .actor_queue_capacity_per_worker = 1,
            .outbound_queue_capacity = 1,
        }};
        const auto overloaded_client = connect_client(server.getPort());
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 9,
            .payload = {},
        };
        const auto encoded_request = snf::protocol::encode_frame(request);
        std::vector<std::byte> bundled_requests;
        constexpr int REQUEST_COUNT = 32;
        bundled_requests.reserve(encoded_request.size() * REQUEST_COUNT);
        for (int request_index = 0; request_index < REQUEST_COUNT; ++request_index)
        {
            bundled_requests.insert(
                bundled_requests.end(), encoded_request.begin(), encoded_request.end());
        }

        send_all(overloaded_client.getDescriptor(), bundled_requests);
        receive_until_closed(overloaded_client.getDescriptor());

        const auto healthy_client = connect_client(server.getPort());
        const snf::protocol::Frame healthy_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 10,
            .payload = {std::byte{0x01}},
        };
        const auto healthy_encoded_request = snf::protocol::encode_frame(healthy_request);
        send_all(healthy_client.getDescriptor(), healthy_encoded_request);
        assert_pong(receive_exact(healthy_client.getDescriptor(), healthy_encoded_request.size()),
                    healthy_request);

        server.stop();
        assert(server.getStats().actor_queue_overflows >= 1);
        assert(server.getStats().stale_outbound_actions >= 1);
    }

    void test_request_stop_closes_listener_and_active_sessions()
    {
        RunningServer server;
        const auto port = server.getPort();
        const auto client = connect_client(port);

        const auto start_time = std::chrono::steady_clock::now();
        server.stop();
        const auto elapsed_time = std::chrono::steady_clock::now() - start_time;

        assert(elapsed_time < 1s);

        snf::net::UniqueFileDescriptor connection_attempt{
            ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
        assert(connection_attempt.isValid());

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(port);
        assert(::inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) == 1);

        assert(::connect(connection_attempt.getDescriptor(),
                         reinterpret_cast<const sockaddr*>(&server_address),
                         sizeof(server_address)) == -1);
    }

    void test_closes_slow_client_when_send_queue_exceeds_limit()
    {
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 7,
            .payload = std::vector<std::byte>(snf::protocol::MAX_PAYLOAD_SIZE, std::byte{0xAA}),
        };
        const auto encoded_request = snf::protocol::encode_frame(request);

        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = encoded_request.size(),
            .client_send_buffer_size = 1024,
        }};
        const auto slow_client = connect_client(server.getPort(), 1024);

        std::vector<std::byte> bundled_requests;
        constexpr int REQUEST_COUNT = 32;
        bundled_requests.reserve(encoded_request.size() * REQUEST_COUNT);
        for (int request_index = 0; request_index < REQUEST_COUNT; ++request_index)
        {
            bundled_requests.insert(
                bundled_requests.end(), encoded_request.begin(), encoded_request.end());
        }
        send_until_complete_or_closed(slow_client.getDescriptor(), bundled_requests);
        receive_until_closed(slow_client.getDescriptor());

        const auto healthy_client = connect_client(server.getPort());
        const snf::protocol::Frame healthy_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 8,
            .payload = {},
        };
        const auto healthy_encoded_request = snf::protocol::encode_frame(healthy_request);
        send_all(healthy_client.getDescriptor(), healthy_encoded_request);
        const auto response =
            receive_exact(healthy_client.getDescriptor(), healthy_encoded_request.size());
        assert_pong(response, healthy_request);

        server.stop();
    }

    void test_shutdown_forces_slow_client_closed_after_grace_period()
    {
        constexpr auto shutdown_grace_period = 150ms;
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = shutdown_grace_period,
            .max_pending_send_bytes = 8 * 1024 * 1024,
            .client_send_buffer_size = 1024,
        }};
        const auto slow_client = connect_client(server.getPort(), 1024);

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 9,
            .payload = std::vector<std::byte>(snf::protocol::MAX_PAYLOAD_SIZE, std::byte{0xBB}),
        };
        const auto encoded_request = snf::protocol::encode_frame(request);
        std::vector<std::byte> bundled_requests;
        constexpr int REQUEST_COUNT = 64;
        bundled_requests.reserve(encoded_request.size() * REQUEST_COUNT);

        for (int request_index = 0; request_index < REQUEST_COUNT; ++request_index)
        {
            bundled_requests.insert(
                bundled_requests.end(), encoded_request.begin(), encoded_request.end());
        }

        send_all(slow_client.getDescriptor(), bundled_requests);

        pollfd response_poll{
            .fd = slow_client.getDescriptor(),
            .events = POLLIN,
            .revents = 0,
        };
        assert(::poll(&response_poll, 1, 1000) == 1);
        assert((response_poll.revents & POLLIN) != 0);

        const auto stop_started_at = std::chrono::steady_clock::now();
        server.stop();
        const auto stop_duration = std::chrono::steady_clock::now() - stop_started_at;

        assert(stop_duration >= shutdown_grace_period / 2);
        assert(stop_duration < 1s);
    }

    void test_termination_signal_stops_server(const int signal_number)
    {
        const auto termination_signal = snf::net::create_termination_signal_listener();
        RunningServer server{termination_signal.getDescriptor()};

        assert(::kill(::getpid(), signal_number) == 0);
        server.join();
    }

    void test_actor_runtime_failure_aborts_without_waiting_for_grace_period()
    {
        RecordingFrameIngress ingress;
        const auto outbound_event = make_eventfd();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 8, .max_slots_per_connection = 8},
            outbound_event.getDescriptor()};
        snf::runtime::RuntimeCompletionCoordinator runtime_completion{
            snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic),
            outbound_event.getDescriptor()};
        snf::server::TcpServer server{
            snf::server::TcpServerConfig{
                .port = 0,
                .shutdown_grace_period = 5s,
                .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                .client_send_buffer_size = std::nullopt,
            },
            ingress,
            outbound,
            runtime_completion,
            outbound_event.getDescriptor()};

        std::promise<void> server_finished;
        const auto finished = server_finished.get_future();
        std::exception_ptr server_error;
        std::thread server_thread{[&]
                                  {
                                      try
                                      {
                                          server.run();
                                      }
                                      catch (...)
                                      {
                                          server_error = std::current_exception();
                                      }
                                      server_finished.set_value();
                                  }};

        runtime_completion.notifyFailed(snf::runtime::RuntimeId::Logic);
        assert(finished.wait_for(1s) == std::future_status::ready);
        server_thread.join();

        assert(server_error == nullptr);
        assert(ingress.closed);
        assert(ingress.cancelled);
    }

    void test_retries_a_full_connection_closed_post_without_duplicate_after_acceptance()
    {
        RecordingFrameIngress ingress;
        ingress.lifecycle_results = {
            snf::server::PostResult::Full,
            snf::server::PostResult::Full,
            snf::server::PostResult::Accepted,
        };
        const auto outbound_event = make_eventfd();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 8, .max_slots_per_connection = 8},
            outbound_event.getDescriptor()};
        snf::runtime::RuntimeCompletionCoordinator runtime_completion{
            snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic),
            outbound_event.getDescriptor()};
        snf::server::TcpServer server{
            snf::server::TcpServerConfig{
                .port = 0,
                .shutdown_grace_period = 200ms,
                .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                .client_send_buffer_size = std::nullopt,
            },
            ingress,
            outbound,
            runtime_completion,
            outbound_event.getDescriptor()};

        std::exception_ptr server_error;
        std::thread server_thread{[&]
                                  {
                                      try
                                      {
                                          server.run();
                                      }
                                      catch (...)
                                      {
                                          server_error = std::current_exception();
                                      }
                                  }};
        {
            const auto client = connect_client(server.getPort());
        }

        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (ingress.lifecycle_attempts.load() != 3 &&
               std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(ingress.lifecycle_attempts.load() == 3);

        runtime_completion.notifyDrained(snf::runtime::RuntimeId::Logic);
        server.requestStop();
        server_thread.join();

        assert(server_error == nullptr);
        assert(ingress.connection_closes.size() == 3);
        const auto& first = ingress.connection_closes.front();
        for (const auto& closed : ingress.connection_closes)
        {
            assert(closed.connection == first.connection);
            assert(closed.cause == snf::server::ConnectionCloseCause::PeerClosed);
        }
    }

    void test_bounds_pending_connection_closes_and_rejects_new_connections_at_capacity()
    {
        RecordingFrameIngress ingress;
        ingress.lifecycle_fallback = snf::server::PostResult::Full;
        const auto outbound_event = make_eventfd();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 8, .max_slots_per_connection = 8},
            outbound_event.getDescriptor()};
        snf::runtime::RuntimeCompletionCoordinator runtime_completion{
            snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic),
            outbound_event.getDescriptor()};
        snf::server::TcpServer server{
            snf::server::TcpServerConfig{
                .port = 0,
                .shutdown_grace_period = 200ms,
                .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                .client_send_buffer_size = std::nullopt,
                .connection_lifecycle_capacity = 2,
            },
            ingress,
            outbound,
            runtime_completion,
            outbound_event.getDescriptor()};

        std::exception_ptr server_error;
        std::thread server_thread{[&]
                                  {
                                      try
                                      {
                                          server.run();
                                      }
                                      catch (...)
                                      {
                                          server_error = std::current_exception();
                                      }
                                  }};

        for (std::size_t close_index = 0; close_index < 2; ++close_index)
        {
            {
                const auto client = connect_client(server.getPort());
            }

            const auto deadline = std::chrono::steady_clock::now() + 1s;
            while (ingress.lifecycle_attempts.load() < close_index + 1 &&
                   std::chrono::steady_clock::now() < deadline)
            {
                std::this_thread::sleep_for(1ms);
            }
            assert(ingress.lifecycle_attempts.load() >= close_index + 1);
        }

        const auto rejected_client = connect_client(server.getPort());
        receive_until_closed(rejected_client.getDescriptor());

        runtime_completion.notifyDrained(snf::runtime::RuntimeId::Logic);
        server.requestStop();
        server_thread.join();

        assert(server_error == nullptr);
        assert(server.getStats().accepted_connections == 2);
        assert(server.getStats().closed_connections == 2);
        assert(server.getStats().connection_lifecycle_rejections == 1);
        assert(server.getStats().pending_connection_closes_high_water_mark == 2);
    }
}

int main()
{
    test_returns_pong_for_ping();
    test_authenticates_one_session_and_allows_reconnect_after_passivation();
    test_collects_baseline_saturation_metrics_for_a_round_trip();
    test_saturated_outbound_answers_every_request_and_still_drains();
    test_reports_metrics_periodically_while_running();
    test_peer_disconnect_evicts_the_player_actor();
    test_decodes_ping_sent_one_byte_at_a_time();
    test_decodes_multiple_pings_from_one_send();
    test_survives_client_close_during_partial_frame();
    test_closes_connection_for_an_unregistered_message();
    test_overflowed_actor_queue_closes_only_that_connection();
    test_request_stop_closes_listener_and_active_sessions();
    test_closes_slow_client_when_send_queue_exceeds_limit();
    test_shutdown_forces_slow_client_closed_after_grace_period();
    test_termination_signal_stops_server(SIGINT);
    test_termination_signal_stops_server(SIGTERM);
    test_actor_runtime_failure_aborts_without_waiting_for_grace_period();
    test_retries_a_full_connection_closed_post_without_duplicate_after_acceptance();
    test_bounds_pending_connection_closes_and_rejects_new_connections_at_capacity();
}
