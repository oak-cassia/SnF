#include "snf/net/socket_options.hpp"
#include "snf/net/termination_signal.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/server/game_server.hpp"

#include <arpa/inet.h>
#include <array>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <future>
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

    class RecordingCommandIngress final : public snf::server::CommandIngress
    {
    public:
        [[nodiscard]] snf::server::PostResult
        tryPost(snf::server::InboundCommand) override
        {
            return snf::server::PostResult::Closed;
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

            assert(false);
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
        assert(server.getStats().stale_network_actions >= 1);
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
        RecordingCommandIngress ingress;
        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{8};
        const auto outbound_event = make_eventfd();
        snf::server::TcpServer server{
            snf::server::TcpServerConfig{
                .port = 0,
                .shutdown_grace_period = 5s,
                .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                .client_send_buffer_size = std::nullopt,
            },
            ingress,
            outbound,
            outbound_event.getDescriptor()};

        std::promise<void> server_finished;
        const auto finished = server_finished.get_future();
        std::exception_ptr server_error;
        std::thread server_thread{
            [&]
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

        assert(outbound.tryPush(snf::server::ActorRuntimeFailed{}));
        constexpr std::uint64_t wakeup_value = 1;
        assert(::write(outbound_event.getDescriptor(), &wakeup_value, sizeof(wakeup_value)) ==
               sizeof(wakeup_value));
        assert(finished.wait_for(1s) == std::future_status::ready);
        server_thread.join();

        assert(server_error == nullptr);
        assert(ingress.closed);
        assert(ingress.cancelled);
    }
}

int main()
{
    test_returns_pong_for_ping();
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
}
