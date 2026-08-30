#include "snf/net/tcp_listener.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/worker.hpp"
#include "snf/worker/worker_group.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <utility>
#include <vector>

namespace
{
    using namespace std::chrono_literals;
    using namespace snf::worker;
    using snf::protocol::Frame;
    using snf::protocol::MessageType;

    enum class SinkMode
    {
        Accept,
        RespondAndGracefulClose,
        RespondOverflow,
        Invalid,
        Rejected,
    };

    struct SinkState
    {
        std::mutex mutex;
        std::condition_variable changed;
        std::vector<Frame> requests;
        std::vector<ConnectionRef> connections;
        std::vector<SendResult> send_results;
        std::vector<bool> close_results;
        std::vector<CloseReason> close_reasons;
    };

    template <class Predicate> [[nodiscard]] bool waitFor(SinkState& state, Predicate&& predicate, const std::chrono::milliseconds timeout = 2s)
    {
        std::unique_lock lock{state.mutex};
        return state.changed.wait_for(
            lock,
            timeout,
            [&state, &predicate]
            {
                return predicate(state);
            }
        );
    }

    class TestRequestSink final : public RequestSink
    {
    public:
        TestRequestSink(std::shared_ptr<SinkState> state, const SinkMode mode, Worker* worker = nullptr)
            : _state(std::move(state))
            , _mode(mode)
            , _worker(worker)
        {
        }

        void setWorker(Worker& worker) noexcept
        {
            _worker = &worker;
        }

        [[nodiscard]] RequestPostResult tryPost(ConnectionRef connection, Frame&& frame) override
        {
            {
                std::lock_guard lock{_state->mutex};
                _state->connections.push_back(connection);
                _state->requests.push_back(frame);
            }
            _state->changed.notify_all();

            if (_mode == SinkMode::RespondAndGracefulClose || _mode == SinkMode::RespondOverflow)
            {
                assert(_worker != nullptr);
                Frame response{
                    .type = MessageType::Pong,
                    .request_id = frame.request_id,
                    .payload = std::vector<std::byte>{std::byte{0x42}},
                };
                const SendResult send_result = _worker->send(connection, std::move(response));
                const bool close_result =
                    _mode == SinkMode::RespondAndGracefulClose && _worker->closeConnection(connection, CloseReason::Application, true);
                {
                    std::lock_guard lock{_state->mutex};
                    _state->send_results.push_back(send_result);
                    _state->close_results.push_back(close_result);
                }
                _state->changed.notify_all();
                return RequestPostResult::Accepted;
            }

            if (_mode == SinkMode::Invalid)
            {
                return RequestPostResult::Invalid;
            }
            if (_mode == SinkMode::Rejected)
            {
                return RequestPostResult::Rejected;
            }
            return RequestPostResult::Accepted;
        }

        void onConnectionClosed(ConnectionRef, CloseReason reason) override
        {
            {
                std::lock_guard lock{_state->mutex};
                _state->close_reasons.push_back(reason);
            }
            _state->changed.notify_all();
        }

    private:
        std::shared_ptr<SinkState> _state;
        SinkMode _mode;
        Worker* _worker;
    };

    [[nodiscard]] Frame pingFrame(const std::uint32_t request_id)
    {
        return Frame{
            .type = MessageType::Ping,
            .request_id = request_id,
            .payload = std::vector<std::byte>{std::byte{0x10}, std::byte{0x20}},
        };
    }

    [[nodiscard]] std::uint16_t portOf(const int descriptor)
    {
        sockaddr_in address{};
        socklen_t address_size = sizeof(address);
        assert(::getsockname(descriptor, reinterpret_cast<sockaddr*>(&address), &address_size) == 0);
        return ntohs(address.sin_port);
    }

    void setReceiveTimeout(const int descriptor)
    {
        timeval timeout{.tv_sec = 2, .tv_usec = 0};
        assert(::setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
    }

    [[nodiscard]] snf::net::UniqueFileDescriptor connectClient(const std::uint16_t port)
    {
        const int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
        assert(descriptor != -1);
        snf::net::UniqueFileDescriptor client{descriptor};
        setReceiveTimeout(descriptor);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        assert(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);

        int result = ::connect(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
        while (result == -1 && errno == EINTR)
        {
            result = ::connect(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
        }
        assert(result == 0);
        return client;
    }

    void sendAll(const int descriptor, const std::vector<std::byte>& bytes)
    {
        std::size_t offset = 0;
        while (offset < bytes.size())
        {
            const ssize_t sent = ::send(descriptor, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
            if (sent == -1 && errno == EINTR)
            {
                continue;
            }
            assert(sent > 0);
            offset += static_cast<std::size_t>(sent);
        }
    }

    [[nodiscard]] std::vector<std::byte> receiveExact(const int descriptor, const std::size_t byte_count)
    {
        std::vector<std::byte> bytes(byte_count);
        std::size_t offset = 0;
        while (offset < byte_count)
        {
            const ssize_t received = ::recv(descriptor, bytes.data() + offset, byte_count - offset, 0);
            if (received == -1 && errno == EINTR)
            {
                continue;
            }
            assert(received > 0);
            offset += static_cast<std::size_t>(received);
        }
        return bytes;
    }

    [[nodiscard]] bool receivesEof(const int descriptor)
    {
        std::byte byte{};
        const ssize_t received = ::recv(descriptor, &byte, sizeof(byte), 0);
        if (received == 0)
        {
            return true;
        }
        if (received == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT))
        {
            return false;
        }
        assert(false && "unexpected bytes while waiting for EOF");
        return false;
    }

    [[nodiscard]] WorkerNetworkConfig testNetworkConfig(const std::size_t connection_capacity = 8)
    {
        WorkerNetworkConfig config;
        config.table.capacity = connection_capacity;
        config.poll_registration_capacity = connection_capacity + 1;
        config.max_accepts_per_poll = 8;
        config.receive_chunk_bytes = 1024;
        return config;
    }

    [[nodiscard]] WorkerBudgets testBudgets()
    {
        WorkerBudgets budgets = WorkerBudgets::defaults();
        budgets.max_poll_timeout = 5s;
        return budgets;
    }

    void test_worker_round_trip_and_graceful_write_drain()
    {
        auto state = std::make_shared<SinkState>();
        TestRequestSink sink{state, SinkMode::RespondAndGracefulClose};
        WorkerBudgets budgets = testBudgets();
        budgets.writes.max_bytes = 1;
        Worker owner_worker(WorkerId{0}, 1, budgets, WorkerInboxConfig{}, testNetworkConfig(), sink);
        sink.setWorker(owner_worker);

        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        owner_worker.attachListener(std::move(listener));

        std::thread thread{[&owner_worker]
                           {
                               owner_worker.run();
                           }};
        auto client = connectClient(port);
        const Frame request = pingFrame(11);
        const auto encoded_request = snf::protocol::encode_frame(request);
        sendAll(client.getDescriptor(), encoded_request);

        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.requests.size() == 1 && observed.send_results.size() == 1 && observed.close_results.size() == 1;
            }
        ));

        const Frame expected_response{
            .type = MessageType::Pong,
            .request_id = request.request_id,
            .payload = std::vector<std::byte>{std::byte{0x42}},
        };
        const auto encoded_response = receiveExact(client.getDescriptor(), snf::protocol::encode_frame(expected_response).size());
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(encoded_response);
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        assert(decoded.frames.front() == expected_response);
        assert(receivesEof(client.getDescriptor()));

        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.close_reasons.size() == 1;
            }
        ));

        owner_worker.requestStop();
        thread.join();

        std::lock_guard lock{state->mutex};
        assert(state->requests.front() == request);
        assert(state->send_results.front() == SendResult::Accepted);
        assert(state->close_results.front());
        assert(state->close_reasons.front() == CloseReason::Application);
        assert(owner_worker.metrics().network.accepted_connections == 1);
        assert(owner_worker.metrics().network.sent_frames == 1);
        assert(owner_worker.metrics().network.graceful_closes == 1);
        assert(owner_worker.metrics().network.immediate_closes == 0);
        assert(owner_worker.metrics().network.write_budget_stops >= 1);
    }

    void test_decode_frame_budget_requeues_buffered_frames()
    {
        auto state = std::make_shared<SinkState>();
        TestRequestSink sink{state, SinkMode::Accept};
        WorkerBudgets budgets = testBudgets();
        budgets.poll.max_frames = 1;
        budgets.poll.max_duration = 1s;
        Worker worker(WorkerId{0}, 1, budgets, WorkerInboxConfig{}, testNetworkConfig(), sink);

        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));

        std::thread thread{[&worker]
                           {
                               worker.run();
                           }};
        auto client = connectClient(port);
        const Frame first = pingFrame(41);
        const Frame second = pingFrame(42);
        auto encoded = snf::protocol::encode_frame(first);
        const auto encoded_second = snf::protocol::encode_frame(second);
        encoded.insert(encoded.end(), encoded_second.begin(), encoded_second.end());
        sendAll(client.getDescriptor(), encoded);

        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.requests.size() == 2;
            }
        ));

        worker.requestStop();
        thread.join();

        std::lock_guard lock{state->mutex};
        assert(state->requests[0] == first);
        assert(state->requests[1] == second);
        assert(worker.metrics().network.read_budget_stops >= 1);
    }

    void test_remote_critical_send_and_graceful_close_preserve_semantics()
    {
        auto target_state = std::make_shared<SinkState>();
        auto source_state = std::make_shared<SinkState>();
        TestRequestSink target_sink{target_state, SinkMode::Accept};
        TestRequestSink source_sink{source_state, SinkMode::Accept};

        WorkerNetworkConfig target_network = testNetworkConfig();
        target_network.table.limits.write_soft_watermark_bytes = 5;
        target_network.table.limits.write_hard_limit_bytes = 1024;
        Worker target_worker(WorkerId{0}, 2, testBudgets(), WorkerInboxConfig{}, target_network, target_sink);
        Worker source_worker(WorkerId{1}, 2, testBudgets(), WorkerInboxConfig{}, testNetworkConfig(), source_sink);

        auto remote_target_port = target_worker.bindInboxSource(WorkerId{1});
        source_worker.bindRemoteTarget(WorkerId{0}, std::move(remote_target_port));
        auto trigger_port = source_worker.bindInboxSource(WorkerId{0});

        source_worker.setEventHandler(
            [&source_worker, target_state, source_state](WorkerEvent&&)
            {
                ConnectionRef target_connection;
                {
                    std::lock_guard lock{target_state->mutex};
                    assert(!target_state->connections.empty());
                    target_connection = target_state->connections.front();
                }

                Frame response{
                    .type = MessageType::Pong,
                    .request_id = 91,
                    .payload = std::vector<std::byte>{std::byte{0x33}},
                };
                const SendResult send_result = source_worker.send(target_connection, std::move(response), true);
                const bool close_result = source_worker.closeConnection(target_connection, CloseReason::Application, true);
                {
                    std::lock_guard lock{source_state->mutex};
                    source_state->send_results.push_back(send_result);
                    source_state->close_results.push_back(close_result);
                }
                source_state->changed.notify_all();
            }
        );

        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        target_worker.attachListener(std::move(listener));

        std::thread target_thread{[&target_worker]
                                  {
                                      target_worker.run();
                                  }};
        std::thread source_thread{[&source_worker]
                                  {
                                      source_worker.run();
                                  }};

        auto client = connectClient(port);
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(pingFrame(90)));
        assert(waitFor(
            *target_state,
            [](const SinkState& observed)
            {
                return observed.connections.size() == 1;
            }
        ));

        ConnectionRef target_connection;
        {
            std::lock_guard lock{target_state->mutex};
            target_connection = target_state->connections.front();
        }
        WorkerEnvelope trigger{
            .event =
                RemoteConnectionClose{
                    .connection = target_connection,
                    .reason = CloseReason::Application,
                    .graceful = false,
                },
            .charged_bytes = static_cast<std::uint32_t>(sizeof(RemoteConnectionClose)),
        };
        assert(trigger_port.tryPush(std::move(trigger)) == InboxPushResult::Accepted);
        assert(waitFor(
            *source_state,
            [](const SinkState& observed)
            {
                return observed.send_results.size() == 1 && observed.close_results.size() == 1;
            }
        ));

        const Frame expected_response{
            .type = MessageType::Pong,
            .request_id = 91,
            .payload = std::vector<std::byte>{std::byte{0x33}},
        };
        const auto encoded_response = receiveExact(client.getDescriptor(), snf::protocol::encode_frame(expected_response).size());
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(encoded_response);
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        assert(decoded.frames.front() == expected_response);
        assert(receivesEof(client.getDescriptor()));

        target_worker.requestStop();
        source_worker.requestStop();
        target_thread.join();
        source_thread.join();

        {
            std::lock_guard lock{source_state->mutex};
            assert(source_state->send_results.front() == SendResult::Accepted);
            assert(source_state->close_results.front());
        }
        assert(target_worker.metrics().network.sent_frames == 1);
        assert(target_worker.metrics().network.soft_limit_sends == 0);
        assert(target_worker.metrics().network.graceful_closes == 1);
        assert(target_worker.metrics().network.immediate_closes == 0);
    }

    void run_terminal_sink_case(const SinkMode mode, const RequestPostResult expected_result, const CloseReason expected_reason)
    {
        auto state = std::make_shared<SinkState>();
        TestRequestSink sink{state, mode};
        Worker worker(WorkerId{0}, 1, testBudgets(), WorkerInboxConfig{}, testNetworkConfig(), sink);
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));

        std::thread thread{[&worker]
                           {
                               worker.run();
                           }};
        auto client = connectClient(port);
        const auto request = pingFrame(22);
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(request));

        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.requests.size() == 1 && observed.close_reasons.size() == 1;
            }
        ));
        assert(receivesEof(client.getDescriptor()));

        worker.requestStop();
        thread.join();

        std::lock_guard lock{state->mutex};
        assert(state->requests.size() == 1);
        assert(state->requests.front() == request);
        assert(state->close_reasons.front() == expected_reason);
        assert(worker.metrics().network.rejected_requests == 1);
        assert(worker.metrics().network.immediate_closes == 1);
        if (expected_result == RequestPostResult::Invalid)
        {
            assert(worker.metrics().network.protocol_errors == 1);
        }
        else
        {
            assert(worker.metrics().network.protocol_errors == 0);
        }
    }

    void test_request_sink_terminal_results_are_not_retried()
    {
        run_terminal_sink_case(SinkMode::Invalid, RequestPostResult::Invalid, CloseReason::ProtocolViolation);
        run_terminal_sink_case(SinkMode::Rejected, RequestPostResult::Rejected, CloseReason::Overload);
    }

    void test_slow_consumer_hard_limit_closes_only_that_connection()
    {
        auto state = std::make_shared<SinkState>();
        TestRequestSink sink{state, SinkMode::RespondOverflow};
        WorkerNetworkConfig network = testNetworkConfig();
        network.table.limits.write_soft_watermark_bytes = 5;
        network.table.limits.write_hard_limit_bytes = 10;
        Worker worker(WorkerId{0}, 1, testBudgets(), WorkerInboxConfig{}, network, sink);
        sink.setWorker(worker);
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));

        std::thread thread{[&worker]
                           {
                               worker.run();
                           }};
        auto client = connectClient(port);
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(pingFrame(23)));

        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.requests.size() == 1 && observed.send_results.size() == 1 && observed.close_reasons.size() == 1;
            }
        ));
        assert(receivesEof(client.getDescriptor()));

        worker.requestStop();
        thread.join();

        std::lock_guard lock{state->mutex};
        assert(state->send_results.front() == SendResult::HardLimit);
        assert(!state->close_results.front());
        assert(state->close_reasons.front() == CloseReason::SlowConsumer);
        assert(worker.metrics().network.hard_limit_sends == 1);
        assert(worker.metrics().network.immediate_closes == 1);
        assert(worker.metrics().network.graceful_closes == 0);
    }

    void test_listener_pauses_until_both_capacity_tables_have_room()
    {
        auto state = std::make_shared<SinkState>();
        WorkerGroupConfig config;
        config.worker_count = 1;
        config.max_workers = 1;
        config.port = 0;
        config.budgets = testBudgets();
        config.network = testNetworkConfig(1);

        WorkerGroup group{
            config,
            [state](WorkerId)
            {
                return std::make_unique<TestRequestSink>(state, SinkMode::Accept);
            },
        };
        group.start();

        auto first_client = connectClient(group.port());
        sendAll(first_client.getDescriptor(), snf::protocol::encode_frame(pingFrame(31)));
        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.requests.size() == 1;
            }
        ));

        auto second_client = connectClient(group.port());
        first_client.init();
        sendAll(second_client.getDescriptor(), snf::protocol::encode_frame(pingFrame(32)));
        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.requests.size() == 2;
            }
        ));

        group.requestStop();
        group.join();

        const auto& metrics = group.worker(0).metrics().network;
        assert(metrics.accepted_connections == 2);
        assert(metrics.listener_pauses >= 1);
        assert(metrics.listener_resumes >= 1);
    }

    void test_group_bootstrap_binds_each_worker_with_port_zero()
    {
        WorkerGroupConfig config;
        config.worker_count = 2;
        config.max_workers = 2;
        config.port = 0;
        config.budgets = testBudgets();
        config.network = testNetworkConfig(2);

        WorkerGroup group{config};
        assert(group.port() != 0);
        assert(group.workerCount() == 2);
        group.start();
        std::this_thread::sleep_for(10ms);
        group.requestStop();
        group.join();

        assert(group.worker(0).metrics().loop_iterations > 0);
        assert(group.worker(1).metrics().loop_iterations > 0);
    }
}

int main()
{
    test_worker_round_trip_and_graceful_write_drain();
    test_decode_frame_budget_requeues_buffered_frames();
    test_remote_critical_send_and_graceful_close_preserve_semantics();
    test_request_sink_terminal_results_are_not_retried();
    test_slow_consumer_hard_limit_closes_only_that_connection();
    test_listener_pauses_until_both_capacity_tables_have_room();
    test_group_bootstrap_binds_each_worker_with_port_zero();
}
