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
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <stdexcept>
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
        Invalid,
        Rejected,
    };

    struct SinkState
    {
        std::mutex mutex;
        std::condition_variable changed;
        std::vector<Frame> requests;
        std::vector<ConnectionRef> connections;
        std::vector<ConnectionRef> closed_connections;
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

        void setRequestCallback(std::function<void(ConnectionRef, const Frame&)> callback)
        {
            _request_callback = std::move(callback);
        }

        [[nodiscard]] RequestPostResult tryPost(ConnectionRef connection, Frame&& frame) override
        {
            {
                std::lock_guard lock{_state->mutex};
                _state->connections.push_back(connection);
                _state->requests.push_back(frame);
            }
            _state->changed.notify_all();

            if (_request_callback)
            {
                _request_callback(connection, frame);
            }

            if (_mode == SinkMode::RespondAndGracefulClose)
            {
                assert(_worker != nullptr);
                Frame response{
                    .type = MessageType::Pong,
                    .request_id = frame.request_id,
                    .payload = std::vector<std::byte>{std::byte{0x42}},
                };
                const SendResult send_result = _worker->send(connection, std::move(response));
                const bool close_result = _worker->closeConnection(connection, CloseReason::Application, true);
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

        void onConnectionClosed(ConnectionRef connection, CloseReason reason) override
        {
            {
                std::lock_guard lock{_state->mutex};
                _state->closed_connections.push_back(connection);
                _state->close_reasons.push_back(reason);
            }
            _state->changed.notify_all();
        }

    private:
        std::shared_ptr<SinkState> _state;
        SinkMode _mode;
        Worker* _worker;
        std::function<void(ConnectionRef, const Frame&)> _request_callback;
    };

    [[nodiscard]] Frame pingFrame(const std::uint32_t request_id)
    {
        return Frame{
            .type = MessageType::Ping,
            .request_id = request_id,
            .payload = std::vector<std::byte>{std::byte{0x10}, std::byte{0x20}},
        };
    }

    [[nodiscard]] Frame pongFrame(const std::uint32_t request_id, const std::size_t payload_size = 1)
    {
        return Frame{
            .type = MessageType::Pong,
            .request_id = request_id,
            .payload = std::vector<std::byte>(payload_size, std::byte{0x42}),
        };
    }

    [[nodiscard]] std::size_t openFileDescriptorCount()
    {
        std::size_t count = 0;
        for ([[maybe_unused]] const auto& entry : std::filesystem::directory_iterator{"/proc/self/fd"})
        {
            ++count;
        }
        return count;
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

    [[nodiscard]] snf::net::UniqueFileDescriptor connectClient(const std::uint16_t port, const int receive_buffer_size = 0)
    {
        const int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
        assert(descriptor != -1);
        snf::net::UniqueFileDescriptor client{descriptor};
        setReceiveTimeout(descriptor);
        if (receive_buffer_size > 0)
        {
            assert(::setsockopt(descriptor, SOL_SOCKET, SO_RCVBUF, &receive_buffer_size, sizeof(receive_buffer_size)) == 0);
        }

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

    void test_eagain_waits_for_epollout_and_resumes_in_order()
    {
        constexpr std::size_t frame_count = 24;
        constexpr std::size_t payload_size = 32ull * 1024;

        auto state = std::make_shared<SinkState>();
        TestRequestSink sink{state, SinkMode::Accept};
        WorkerNetworkConfig network = testNetworkConfig(1);
        network.client_send_buffer_size = 4096;
        Worker worker(WorkerId{0}, 1, testBudgets(), WorkerInboxConfig{}, network, sink);
        sink.setRequestCallback(
            [&worker, state](const ConnectionRef connection, const Frame&)
            {
                std::vector<SendResult> results;
                results.reserve(frame_count);
                for (std::size_t index = 0; index < frame_count; ++index)
                {
                    results.push_back(worker.send(connection, pongFrame(1000 + static_cast<std::uint32_t>(index), payload_size), true));
                }
                const bool close_result = worker.closeConnection(connection, CloseReason::Application, true);
                {
                    std::lock_guard lock{state->mutex};
                    state->send_results.insert(state->send_results.end(), results.begin(), results.end());
                    state->close_results.push_back(close_result);
                }
                state->changed.notify_all();
            }
        );

        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));

        std::thread thread{[&worker]
                           {
                               worker.run();
                           }};
        auto client = connectClient(port, 4096);
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(pingFrame(99)));
        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.send_results.size() == frame_count && observed.close_results.size() == 1;
            }
        ));

        // Let the deliberately tiny server send buffer and the unread client
        // receive window force send() to return EAGAIN.
        std::this_thread::sleep_for(100ms);

        const std::size_t encoded_frame_size = snf::protocol::encode_frame(pongFrame(1000, payload_size)).size();
        const auto encoded_responses = receiveExact(client.getDescriptor(), encoded_frame_size * frame_count);
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(encoded_responses);
        assert(decoded.ok());
        assert(decoded.frames.size() == frame_count);
        for (std::size_t index = 0; index < frame_count; ++index)
        {
            assert(decoded.frames[index].request_id == 1000 + index);
        }
        assert(receivesEof(client.getDescriptor()));

        worker.requestStop();
        thread.join();

        {
            std::lock_guard lock{state->mutex};
            for (const SendResult result : state->send_results)
            {
                assert(result == SendResult::Accepted);
            }
            assert(state->close_results.front());
        }
        assert(worker.metrics().network.epollout_waits >= 1);
        assert(worker.metrics().network.epollout_resumes >= 1);
        assert(worker.metrics().network.sent_frames == frame_count);
        assert(worker.metrics().network.graceful_closes == 1);
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
        source_sink.setRequestCallback(
            [&source_worker, target_state, source_state](ConnectionRef, const Frame&)
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
        auto source_listener = snf::net::create_tcp_listener(0);
        const std::uint16_t source_port = portOf(source_listener.getDescriptor());
        source_worker.attachListener(std::move(source_listener));

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

        auto source_client = connectClient(source_port);
        sendAll(source_client.getDescriptor(), snf::protocol::encode_frame(pingFrame(89)));
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

    void test_misrouted_network_events_are_not_sent_to_generic_handler()
    {
        auto state = std::make_shared<SinkState>();
        TestRequestSink sink{state, SinkMode::Accept};
        Worker worker(WorkerId{0}, 2, testBudgets(), WorkerInboxConfig{}, testNetworkConfig(), sink);
        std::size_t generic_events = 0;
        worker.setEventHandler(
            [&generic_events](WorkerEvent&&)
            {
                ++generic_events;
            }
        );

        auto source_port = worker.bindInboxSource(WorkerId{1});
        const ConnectionRef remote_connection{ConnectionId{7}, ConnectionGeneration{8}, WorkerId{1}};
        const Frame remote_frame = pingFrame(61);
        WorkerEnvelope send_event{
            .event = RemoteConnectionSend{.connection = remote_connection, .frame = remote_frame},
            .charged_bytes = remoteSendCharge(remote_frame),
        };
        WorkerEnvelope close_event{
            .event = RemoteConnectionClose{.connection = remote_connection, .reason = CloseReason::Application, .graceful = false},
            .charged_bytes = static_cast<std::uint32_t>(sizeof(RemoteConnectionClose)),
        };
        assert(source_port.tryPush(std::move(send_event)) == InboxPushResult::Accepted);
        assert(source_port.tryPush(std::move(close_event)) == InboxPushResult::Accepted);

        worker.requestStop();
        worker.run();

        assert(generic_events == 0);
        assert(worker.metrics().network.misrouted_events == 2);
    }

    void test_slow_consumer_hard_limit_closes_only_that_connection()
    {
        auto state = std::make_shared<SinkState>();
        TestRequestSink sink{state, SinkMode::Accept};
        WorkerNetworkConfig network = testNetworkConfig(2);
        network.table.limits.write_soft_watermark_bytes = 128;
        network.table.limits.write_hard_limit_bytes = 256;
        Worker worker(WorkerId{0}, 1, testBudgets(), WorkerInboxConfig{}, network, sink);
        sink.setRequestCallback(
            [&worker, state](const ConnectionRef connection, const Frame& request)
            {
                const std::size_t response_size = request.request_id == 70 ? 512 : 1;
                const SendResult result = worker.send(connection, pongFrame(request.request_id, response_size), true);
                {
                    std::lock_guard lock{state->mutex};
                    state->send_results.push_back(result);
                }
                state->changed.notify_all();
            }
        );
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));

        std::thread thread{[&worker]
                           {
                               worker.run();
                           }};
        auto slow_client = connectClient(port);
        auto healthy_client = connectClient(port);
        sendAll(slow_client.getDescriptor(), snf::protocol::encode_frame(pingFrame(70)));

        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.requests.size() == 1 && observed.send_results.size() == 1 && observed.close_reasons.size() == 1;
            }
        ));
        assert(receivesEof(slow_client.getDescriptor()));

        sendAll(healthy_client.getDescriptor(), snf::protocol::encode_frame(pingFrame(71)));
        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.requests.size() == 2 && observed.send_results.size() == 2;
            }
        ));

        const auto encoded_response = receiveExact(healthy_client.getDescriptor(), snf::protocol::encode_frame(pongFrame(71)).size());
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(encoded_response);
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        assert(decoded.frames.front() == pongFrame(71));

        worker.requestStop();
        thread.join();

        std::lock_guard lock{state->mutex};
        assert(state->send_results.size() == 2);
        assert(state->send_results[0] == SendResult::HardLimit);
        assert(state->send_results[1] == SendResult::Accepted);
        assert(state->close_reasons.front() == CloseReason::SlowConsumer);
        assert(worker.metrics().network.accepted_connections == 2);
        assert(worker.metrics().network.hard_limit_sends == 1);
        assert(worker.metrics().network.sent_frames == 1);
        assert(worker.metrics().network.immediate_closes == 1);
        assert(worker.metrics().network.graceful_closes == 1);
    }

    void test_repeated_connect_disconnect_reuses_generation_without_fd_leak()
    {
        constexpr std::size_t iteration_count = 128;

        auto state = std::make_shared<SinkState>();
        TestRequestSink sink{state, SinkMode::Accept};
        Worker worker(WorkerId{0}, 1, testBudgets(), WorkerInboxConfig{}, testNetworkConfig(1), sink);
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));

        std::thread thread{[&worker]
                           {
                               worker.run();
                           }};
        const std::size_t baseline_descriptor_count = openFileDescriptorCount();

        for (std::size_t index = 0; index < iteration_count; ++index)
        {
            auto client = connectClient(port);
            sendAll(client.getDescriptor(), snf::protocol::encode_frame(pingFrame(2000 + static_cast<std::uint32_t>(index))));
            assert(waitFor(
                *state,
                [index](const SinkState& observed)
                {
                    return observed.requests.size() == index + 1;
                }
            ));
            client.init();
            assert(waitFor(
                *state,
                [index](const SinkState& observed)
                {
                    return observed.closed_connections.size() == index + 1;
                }
            ));
        }

        assert(openFileDescriptorCount() == baseline_descriptor_count);
        worker.requestStop();
        thread.join();

        std::lock_guard lock{state->mutex};
        assert(state->connections.size() == iteration_count);
        assert(state->closed_connections.size() == iteration_count);
        for (std::size_t index = 0; index < iteration_count; ++index)
        {
            assert(state->requests[index].request_id == 2000 + index);
            assert(state->connections[index] == state->closed_connections[index]);
            assert(state->connections[index].id == state->connections.front().id);
            if (index != 0)
            {
                assert(state->connections[index - 1].generation.value < state->connections[index].generation.value);
            }
        }
        assert(worker.connectionCount() == 0);
        assert(worker.metrics().network.accepted_connections == iteration_count);
        assert(worker.metrics().network.closed_connections == iteration_count);
    }

    void test_close_deadline_and_stale_timer_ignore_reused_slot()
    {
        constexpr std::size_t stalled_frame_count = 48;
        constexpr std::size_t stalled_payload_size = 32ull * 1024;

        auto state = std::make_shared<SinkState>();
        TestRequestSink sink{state, SinkMode::Accept};
        WorkerNetworkConfig network = testNetworkConfig(1);
        network.client_send_buffer_size = 4096;
        network.table.limits.write_soft_watermark_bytes = 1ull * 1024 * 1024;
        network.table.limits.write_hard_limit_bytes = 2ull * 1024 * 1024;
        network.table.limits.close_drain_deadline = 250ms;
        Worker worker(WorkerId{0}, 1, testBudgets(), WorkerInboxConfig{}, network, sink);
        sink.setRequestCallback(
            [&worker, state](const ConnectionRef connection, const Frame& request)
            {
                std::vector<SendResult> results;
                if (request.request_id == 82)
                {
                    results.reserve(stalled_frame_count);
                    for (std::size_t index = 0; index < stalled_frame_count; ++index)
                    {
                        results.push_back(worker.send(connection, pongFrame(3000 + static_cast<std::uint32_t>(index), stalled_payload_size), true));
                    }
                }
                else
                {
                    results.push_back(worker.send(connection, pongFrame(request.request_id), true));
                }

                const bool should_close = request.request_id == 80 || request.request_id == 82;
                const bool close_result = should_close && worker.closeConnection(connection, CloseReason::Application, true);
                {
                    std::lock_guard lock{state->mutex};
                    state->send_results.insert(state->send_results.end(), results.begin(), results.end());
                    if (should_close)
                    {
                        state->close_results.push_back(close_result);
                    }
                }
                state->changed.notify_all();
            }
        );

        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));
        std::thread thread{[&worker]
                           {
                               worker.run();
                           }};

        auto first_client = connectClient(port);
        sendAll(first_client.getDescriptor(), snf::protocol::encode_frame(pingFrame(80)));
        const auto first_response = receiveExact(first_client.getDescriptor(), snf::protocol::encode_frame(pongFrame(80)).size());
        snf::protocol::FrameDecoder first_decoder;
        const auto first_decoded = first_decoder.append(first_response);
        assert(first_decoded.ok());
        assert(first_decoded.frames.size() == 1);
        assert(first_decoded.frames.front() == pongFrame(80));
        assert(receivesEof(first_client.getDescriptor()));
        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.close_reasons.size() == 1;
            }
        ));

        auto reused_client = connectClient(port, 4096);
        sendAll(reused_client.getDescriptor(), snf::protocol::encode_frame(pingFrame(81)));
        const auto reused_response = receiveExact(reused_client.getDescriptor(), snf::protocol::encode_frame(pongFrame(81)).size());
        snf::protocol::FrameDecoder reused_decoder;
        const auto reused_decoded = reused_decoder.append(reused_response);
        assert(reused_decoded.ok());
        assert(reused_decoded.frames.size() == 1);
        assert(reused_decoded.frames.front() == pongFrame(81));

        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.connections.size() == 2;
            }
        ));
        {
            std::lock_guard lock{state->mutex};
            assert(state->connections[0].id == state->connections[1].id);
            assert(state->connections[0].generation != state->connections[1].generation);
        }

        std::this_thread::sleep_for(350ms);
        sendAll(reused_client.getDescriptor(), snf::protocol::encode_frame(pingFrame(83)));
        const auto post_deadline_response = receiveExact(reused_client.getDescriptor(), snf::protocol::encode_frame(pongFrame(83)).size());
        snf::protocol::FrameDecoder post_deadline_decoder;
        const auto post_deadline_decoded = post_deadline_decoder.append(post_deadline_response);
        assert(post_deadline_decoded.ok());
        assert(post_deadline_decoded.frames.size() == 1);
        assert(post_deadline_decoded.frames.front() == pongFrame(83));

        sendAll(reused_client.getDescriptor(), snf::protocol::encode_frame(pingFrame(82)));
        assert(waitFor(
            *state,
            [](const SinkState& observed)
            {
                return observed.close_reasons.size() == 2;
            },
            3s
        ));

        worker.requestStop();
        thread.join();

        std::lock_guard lock{state->mutex};
        assert(state->close_results.size() == 2);
        assert(state->close_results[0]);
        assert(state->close_results[1]);
        assert(state->close_reasons[0] == CloseReason::Application);
        assert(state->close_reasons[1] == CloseReason::Timeout);
        assert(worker.metrics().network.close_deadline_expirations == 1);
        assert(worker.metrics().network.epollout_waits >= 1);
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

    void test_group_bootstrap_rolls_back_listeners_when_sink_factory_throws()
    {
        auto port_reservation = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(port_reservation.getDescriptor());
        port_reservation.init();

        auto state = std::make_shared<SinkState>();
        WorkerGroupConfig config;
        config.worker_count = 2;
        config.max_workers = 2;
        config.port = port;
        config.budgets = testBudgets();
        config.network = testNetworkConfig(2);

        bool threw = false;
        try
        {
            WorkerGroup group{
                config,
                [state](const WorkerId id) -> std::unique_ptr<RequestSink>
                {
                    if (id.value == 1)
                    {
                        throw std::runtime_error{"injected sink factory failure"};
                    }
                    return std::make_unique<TestRequestSink>(state, SinkMode::Accept);
                },
            };
        }
        catch (const std::runtime_error&)
        {
            threw = true;
        }
        assert(threw);

        auto rebound_listener = snf::net::create_tcp_listener(port);
        assert(rebound_listener.isValid());
        assert(portOf(rebound_listener.getDescriptor()) == port);
    }

    // =========================================================================
    // Vertical Slice: TCP Request -> Actor -> Domain Result -> toEffects -> Pong -> Close
    // =========================================================================

    struct SyntheticPlayerCommand
    {
        ConnectionRef connection;
        std::uint32_t request_id;
    };

    enum class SyntheticDomainStatus
    {
        Success,
    };

    struct SyntheticPlayerResult
    {
        ConnectionRef connection;
        std::uint32_t request_id;
        SyntheticDomainStatus status;
    };

    [[nodiscard]] EffectBatch toEffects(const SyntheticPlayerResult& result)
    {
        EffectBatch batch;
        batch.push(SendFrameEffect{
            .connection = result.connection,
            .frame = pongFrame(result.request_id),
            .critical = false,
        });
        batch.push(CloseConnectionEffect{
            .connection = result.connection,
            .reason = CloseReason::Application,
            .graceful = true,
        });
        return batch;
    }

    class SyntheticPlayerActor final : public ActorInstance
    {
    public:
        TurnResult dispatch(ActorEnvelope&& envelope, const ActorTurnContext&) override
        {
            assert(envelope.connection.has_value());
            const SyntheticPlayerCommand command{
                .connection = *envelope.connection,
                .request_id = envelope.frame.request_id,
            };

            const SyntheticPlayerResult domain_result{
                .connection = command.connection,
                .request_id = command.request_id,
                .status = SyntheticDomainStatus::Success,
            };

            EffectBatch effects = toEffects(domain_result);
            return CompletedTurn{.effects = std::move(effects)};
        }
    };

    class ActorForwardingSink final : public RequestSink
    {
    public:
        void setWorker(Worker& worker) noexcept
        {
            _worker = &worker;
        }

        [[nodiscard]] RequestPostResult tryPost(ConnectionRef connection, Frame&& frame) override
        {
            assert(_worker != nullptr);
            const ActorKey key{.kind = ActorKind::Player, .entity = 1};
            const DeliveryResult delivery =
                _worker->tryDeliverLocal(key, ActorEnvelope::fromFrame(connection, std::move(frame)));
            return delivery == DeliveryResult::Accepted ? RequestPostResult::Accepted : RequestPostResult::Rejected;
        }

    private:
        Worker* _worker{nullptr};
    };

    class SyntheticActorFactory final : public ActorFactory
    {
    public:
        ActorConstructionResult construct(const ActorKey) override
        {
            return ActorConstructionResult::ready(std::make_unique<SyntheticPlayerActor>());
        }
    };

    void test_tcp_request_to_actor_vertical_slice_pong_graceful_close()
    {
        for (int iteration = 0; iteration < 20; ++iteration)
        {
            auto listener = snf::net::create_tcp_listener(0);
            const std::uint16_t port = portOf(listener.getDescriptor());

            ActorForwardingSink sink;
            SyntheticActorFactory factory;

            WorkerActorConfig actor_config{
                .actor_table_capacity = 10,
                .max_mailbox_messages_per_actor = 100,
                .max_mailbox_bytes_per_actor = 1024 * 1024,
                .max_mailbox_messages_total = 100,
                .max_mailbox_bytes_total = 1024 * 1024,
                .max_turns_per_actor_slice = 30,
                .placement_seed = 0,
                .worker_shutdown_timeout = 2000ms,
            };

            Worker worker(
                WorkerId{0},
                1,
                testBudgets(),
                WorkerInboxConfig{},
                testNetworkConfig(1),
                sink,
                actor_config,
                factory
            );
            sink.setWorker(worker);
            worker.attachListener(std::move(listener));

            std::thread worker_thread([&]() { worker.run(); });

            auto client = connectClient(port);
            sendAll(client.getDescriptor(), snf::protocol::encode_frame(pingFrame(42)));

            const Frame expected_pong = pongFrame(42);
            const auto encoded_pong = snf::protocol::encode_frame(expected_pong);
            const auto received_bytes = receiveExact(client.getDescriptor(), encoded_pong.size());

            snf::protocol::FrameDecoder decoder;
            const auto decoded = decoder.append(received_bytes);
            assert(decoded.ok());
            assert(decoded.frames.size() == 1);
            assert(decoded.frames.front() == expected_pong);

            // Expect EOF due to graceful close from Actor effect
            assert(receivesEof(client.getDescriptor()));

            worker.requestStop();
            worker_thread.join();

            assert(worker.metrics().actor.actor_turns >= 1);
            assert(worker.metrics().network.sent_frames >= 1);
            assert(worker.metrics().network.closed_connections >= 1);
        }
    }
}

int main()
{
    test_worker_round_trip_and_graceful_write_drain();
    test_eagain_waits_for_epollout_and_resumes_in_order();
    test_decode_frame_budget_requeues_buffered_frames();
    test_remote_critical_send_and_graceful_close_preserve_semantics();
    test_request_sink_terminal_results_are_not_retried();
    test_misrouted_network_events_are_not_sent_to_generic_handler();
    test_slow_consumer_hard_limit_closes_only_that_connection();
    test_repeated_connect_disconnect_reuses_generation_without_fd_leak();
    test_close_deadline_and_stale_timer_ignore_reused_slot();
    test_listener_pauses_until_both_capacity_tables_have_room();
    test_group_bootstrap_binds_each_worker_with_port_zero();
    test_group_bootstrap_rolls_back_listeners_when_sink_factory_throws();
    test_tcp_request_to_actor_vertical_slice_pong_graceful_close();
}
