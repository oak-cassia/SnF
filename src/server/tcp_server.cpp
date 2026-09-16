#include "snf/server/tcp_server.hpp"

#include "snf/net/socket_options.hpp"
#include "snf/net/system_error.hpp"
#include "snf/net/tcp_listener.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <iostream>
#include <limits>
#include <netinet/in.h>
#include <span>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <system_error>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    constexpr std::size_t MAX_READY_EVENTS = 64;
    constexpr std::size_t RECEIVE_BUFFER_SIZE = 4096;
    constexpr std::uint64_t LISTENER_EVENT_TOKEN = std::numeric_limits<std::uint64_t>::max();
    constexpr std::uint64_t STOP_EVENT_TOKEN = LISTENER_EVENT_TOKEN - 1;
    constexpr std::uint64_t OUTBOUND_EVENT_TOKEN = STOP_EVENT_TOKEN - 1;
    constexpr std::uint64_t TERMINATION_SIGNAL_EVENT_TOKEN = OUTBOUND_EVENT_TOKEN - 1;
    constexpr std::uint64_t MAX_CONNECTION_GENERATION = TERMINATION_SIGNAL_EVENT_TOKEN - 1;
    constexpr std::size_t CONNECTION_CLOSE_RETRY_BUDGET = 64;
    constexpr std::size_t OUTBOUND_DRAIN_BATCH_SIZE = 64;
    constexpr std::chrono::milliseconds CONNECTION_CLOSE_RETRY_INTERVAL{1};

    snf::net::UniqueFileDescriptor create_epoll_instance()
    {
        const int epoll_file_descriptor = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_file_descriptor == -1)
        {
            snf::net::throw_system_error("epoll_create1");
        }

        return snf::net::UniqueFileDescriptor{epoll_file_descriptor};
    }

    snf::net::UniqueFileDescriptor create_stop_event()
    {
        const int stop_event_descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (stop_event_descriptor == -1)
        {
            snf::net::throw_system_error("eventfd");
        }

        return snf::net::UniqueFileDescriptor{stop_event_descriptor};
    }

    std::uint16_t get_listener_port(const int listener_descriptor)
    {
        sockaddr_in address{};
        auto address_size = static_cast<socklen_t>(sizeof(address));

        if (::getsockname(listener_descriptor, reinterpret_cast<sockaddr*>(&address), &address_size) == -1)
        {
            snf::net::throw_system_error("getsockname");
        }

        return ntohs(address.sin_port);
    }
}

namespace snf::server
{
    TcpServer::TcpServer(
        const TcpServerConfig& config,
        FrameIngress& frame_ingress,
        OutboundChannel& outbound,
        snf::runtime::RuntimeCompletionSource& runtime_completion,
        const int outbound_event_descriptor
    )
        : _listener(snf::net::create_tcp_listener(config.port))
        , _epoll(create_epoll_instance())
        , _stop_event(create_stop_event())
        , _port(get_listener_port(_listener.getDescriptor()))
        , _shutdown_grace_period(config.shutdown_grace_period)
        , _max_pending_send_bytes(config.max_pending_send_bytes)
        , _client_send_buffer_size(config.client_send_buffer_size)
        , _connection_lifecycle_capacity(config.connection_lifecycle_capacity)
        , _metrics_report_interval(config.metrics_report_interval)
        , _on_metrics_interval(config.on_metrics_interval)
        , _on_control_wake(config.on_control_wake)
        , _is_control_drained(config.is_control_drained)
        , _frame_ingress(frame_ingress)
        , _outbound(outbound)
        , _runtime_completion(runtime_completion)
        , _outbound_event_descriptor(outbound_event_descriptor)
    {
        if (_shutdown_grace_period < std::chrono::milliseconds::zero() || _metrics_report_interval < std::chrono::milliseconds::zero() ||
            _max_pending_send_bytes == 0 || (_client_send_buffer_size && *_client_send_buffer_size <= 0) || _connection_lifecycle_capacity == 0 ||
            _outbound_event_descriptor == snf::net::UniqueFileDescriptor::INVALID_FD)
        {
            throw std::invalid_argument{"Invalid TCP server configuration"};
        }

        registerListener();
        registerControlDescriptor(_stop_event.getDescriptor(), STOP_EVENT_TOKEN);
        registerControlDescriptor(_outbound_event_descriptor, OUTBOUND_EVENT_TOKEN);
    }

    std::uint16_t TcpServer::getPort() const noexcept
    {
        return _port;
    }

    const TcpServerStats& TcpServer::getStats() const noexcept
    {
        return _stats;
    }

    TcpServerMetrics TcpServer::getMetrics() const
    {
        TcpServerMetrics metrics{
            .reactor_turn_nanoseconds = _reactor_turn_nanoseconds.snapshot(),
            .session_pending_send_bytes = _session_pending_send_bytes.snapshot(),
            .outbound_queue_depth = _outbound_queue_depth.snapshot(),
            .outbound_queue_wait_nanoseconds = _outbound_queue_wait_nanoseconds.snapshot(),
            .session_count = _sessions.size(),
            .sessions_with_pending_send = 0,
            .total_pending_send_bytes = 0,
            .current_outbound_queue_depth = _outbound.size(),
            .outbound_queue_high_water_mark = _outbound.highWaterMark(),
            .reserved_outbound_slots = _outbound.reservedSlotCount(),
            .pending_outbound_reservations = _outbound.pendingWaiterCount(),
            .tracked_outbound_connections = _outbound.trackedConnectionCount(),
        };

        for (const auto& [client_descriptor, session] : _sessions)
        {
            static_cast<void>(client_descriptor);
            const std::size_t pending_send_byte_count = session.getPendingSendByteCount();
            metrics.total_pending_send_bytes += pending_send_byte_count;
            if (pending_send_byte_count != 0)
            {
                ++metrics.sessions_with_pending_send;
            }
        }

        return metrics;
    }

    void TcpServer::run(const int termination_signal_descriptor)
    {
        // 종료 시그널용 FD가 주어지면 소켓들과 같은 epoll에서 알림을 받도록 등록한다.
        if (termination_signal_descriptor != snf::net::UniqueFileDescriptor::INVALID_FD)
        {
            registerControlDescriptor(termination_signal_descriptor, TERMINATION_SIGNAL_EVENT_TOKEN);
        }

        // epoll_wait가 결과를 써 넣을 배열. 이번에 반환된 이벤트들을 담는다.
        std::array<epoll_event, MAX_READY_EVENTS> events{};
        if (hasMetricsReporting())
        {
            _next_metrics_report = std::chrono::steady_clock::now() + _metrics_report_interval;
        }

        // 한 차례: 종료 조건 확인 -> 이벤트 대기 -> 제어 이벤트 -> 연결 이벤트 -> 통계 보고.
        while (true)
        {
            // 큐 포화로 로직 측에 전달하지 못했던 연결 종료 통지를 제한된 개수만 재시도한다.
            retryPendingConnectionCloses();

            // 종료 요청이 있을 때 남은 로직 및 제어 작업과 모든 Session이 정리되면 종료
            if (_is_stopping && _logic_runtime_drained && isControlDrained() && _sessions.empty())
            {
                break;
            }

            // 종료 유예 시간이 지나면 큐를 취소하고 루프를 끝낸다.
            if (_is_stopping && hasShutdownDeadlineExpired())
            {
                cancelQueues();
                break;
            }

            // 반환값: 양수=채워진 개수, 0=시간 만료, -1=오류. 유효한 구간만 아래에서 순회한다.
            // 소켓 I/O는 논블로킹이지만 여기서는 대기할 수 있다. timeout은 종료·재시도·보고 시점을 반영한다.
            const int ready_event_count = ::epoll_wait(_epoll.getDescriptor(), events.data(), static_cast<int>(events.size()), getEpollWaitTimeout());

            if (ready_event_count == -1)
            {
                // 시그널로 대기가 중단되면 바깥 while로 돌아가 종료 조건부터 다시 확인한다.
                if (errno == EINTR)
                {
                    continue;
                }

                snf::net::throw_system_error("epoll_wait");
            }

            // epoll 대기 시간을 제외하고, 반환된 이벤트 묶음의 처리 시간을 측정한다.
            const auto turn_started_at = std::chrono::steady_clock::now();

            // 1차 순회: 종료 요청·시그널·송신 작업 알림을 연결 이벤트보다 먼저 처리한다.
            // 같은 묶음에 종료 요청과 새 연결이 있으면 종료 상태를 먼저 반영할 수 있다.
            for (int event_index = 0; event_index < ready_event_count; ++event_index)
            {
                // epoll_ctl 등록 시 넣어둔 식별 값이 그대로 돌아온다. 제어 FD에는 전용 토큰을 사용한다.
                const std::uint64_t event_token = events[event_index].data.u64;

                if (event_token == STOP_EVENT_TOKEN)
                {
                    handleStopRequest();
                }
                else if (event_token == TERMINATION_SIGNAL_EVENT_TOKEN)
                {
                    handleTerminationSignal(termination_signal_descriptor);
                }
                else if (event_token == OUTBOUND_EVENT_TOKEN)
                {
                    // 로직으로 만든 송신 작업 등을 가져온다. 클라이언트의 EPOLLOUT(쓰기 가능 상턔) 이벤트와는 별개다.
                    handleOutboundActions();
                }
            }

            // 2차 순회: 새 연결과 기존 클라이언트의 I/O를 처리한다.
            for (int event_index = 0; event_index < ready_event_count; ++event_index)
            {
                const epoll_event& event = events[event_index];
                const std::uint64_t event_token = event.data.u64;

                // 1차 순회에서 처리한 제어 이벤트는 중복 처리하지 않는다.
                if (event_token == STOP_EVENT_TOKEN || event_token == TERMINATION_SIGNAL_EVENT_TOKEN || event_token == OUTBOUND_EVENT_TOKEN)
                {
                    continue;
                }

                // 리스너의 읽기 가능 알림은 새 연결을 accept
                if (event_token == LISTENER_EVENT_TOKEN)
                {
                    if (!_is_stopping)
                    {
                        acceptPendingClients();
                    }

                    continue;
                }

                // 클라이언트 토큰은 연결의 generation. FD가 재사용돼도 이전 연결 이벤트와 구분
                const auto descriptor_iterator = _client_descriptors_by_event_token.find(event_token);
                // 앞선 이벤트 처리에서 이미 제거된 연결의 이벤트라면 건너뛴다.
                if (descriptor_iterator == _client_descriptors_by_event_token.end())
                {
                    continue;
                }

                const int client_descriptor = descriptor_iterator->second;
                // token으로 "어느 연결인지" 찾았고, events 비트로 "수신·송신·종료 중 무엇인지" 전달한다.
                handleClientEvent(client_descriptor, event.events);
            }

            if (ready_event_count > 0)
            {
                _reactor_turn_nanoseconds.record(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - turn_started_at)
                );
            }

            // timeout으로 이벤트가 0개여도 보고 시점은 확인한다.
            reportMetricsIfDue();
        }

        // 정상 루프 종료 후 남은 연결을 정리한다. 종료 시간 초과로 빠져나온 경우도 포함한다.
        closeRemainingSessions();
    }

    void TcpServer::requestStop() const noexcept
    {
        constexpr std::uint64_t stop_value = 1;

        while (::write(_stop_event.getDescriptor(), &stop_value, sizeof(stop_value)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return;
        }
    }

    void TcpServer::registerListener() const
    {
        epoll_event listener_event{};
        listener_event.events = EPOLLIN;
        listener_event.data.u64 = LISTENER_EVENT_TOKEN;

        if (::epoll_ctl(_epoll.getDescriptor(), EPOLL_CTL_ADD, _listener.getDescriptor(), &listener_event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_ADD listener)");
        }
    }

    void TcpServer::registerControlDescriptor(const int descriptor, const std::uint64_t event_token) const
    {
        epoll_event control_event{};
        control_event.events = EPOLLIN;
        control_event.data.u64 = event_token;

        if (::epoll_ctl(_epoll.getDescriptor(), EPOLL_CTL_ADD, descriptor, &control_event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_ADD control)");
        }
    }

    void TcpServer::acceptPendingClients()
    {
        while (true)
        {
            const int client_descriptor = ::accept4(_listener.getDescriptor(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);

            if (client_descriptor == -1)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    return;
                }

                if (errno == EINTR)
                {
                    continue;
                }

                snf::net::throw_system_error("accept4");
            }

            snf::net::UniqueFileDescriptor client_socket{client_descriptor};

            if (!hasAvailableConnectionLifecycleSlot())
            {
                ++_stats.connection_lifecycle_rejections;
                return;
            }

            snf::net::enable_tcp_no_delay(client_socket.getDescriptor());

            if (_client_send_buffer_size)
            {
                snf::net::set_socket_send_buffer_size(client_socket.getDescriptor(), *_client_send_buffer_size);
            }

            if (_next_connection_generation == MAX_CONNECTION_GENERATION)
            {
                throw std::overflow_error{"Connection generation exhausted"};
            }

            const snf::net::ConnectionId connection{
                .descriptor = client_descriptor,
                .generation = ++_next_connection_generation,
            };
            const bool inserted =
                _sessions.emplace(client_descriptor, snf::net::Session{std::move(client_socket), connection, _max_pending_send_bytes}).second;

            if (!inserted)
            {
                throw std::logic_error{"A session already owns the client descriptor"};
            }

            const bool event_token_inserted = _client_descriptors_by_event_token.emplace(connection.generation, client_descriptor).second;
            if (!event_token_inserted)
            {
                throw std::logic_error{"A client event token is already registered"};
            }

            epoll_event client_event{};
            client_event.events = EPOLLIN | EPOLLRDHUP;
            client_event.data.u64 = connection.generation;

            if (::epoll_ctl(_epoll.getDescriptor(), EPOLL_CTL_ADD, client_descriptor, &client_event) == -1)
            {
                snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_ADD client)");
            }

            _outbound.trackConnection(connection);

            ++_stats.accepted_connections;
            std::cout << "Accepted client FD: " << client_descriptor << '\n';
        }
    }

    void TcpServer::handleClientEvent(const int client_descriptor, const std::uint32_t event_flags)
    {
        // 종료 사유를 기록하고 함수 끝에서 Session을 제거한다. 수신 처리 중에는 Session 참조를 유지한다.
        std::optional<ConnectionCloseCause> close_cause;
        if ((event_flags & EPOLLERR) != 0)
        {
            close_cause = ConnectionCloseCause::PeerClosed;
        }
        bool should_update_events = false;

        // 종료 알림과 함께 마지막 데이터가 올 수 있어 HUP 계열도 수신 시도
        // 서버 종료 중에는 새로운 입력을 받지 않는다.
        const bool has_read_event = !_is_stopping && (event_flags & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0;

        // optional close_cause에 값 들어있는지 체크
        if (!close_cause && has_read_event)
        {
            // recv 한 번에 사용할 임시 버퍼. 미완성 프레임의 보존은 Session 내부 디코더가 담당
            std::array<std::byte, RECEIVE_BUFFER_SIZE> receive_buffer{};

            const std::unordered_map<int, net::Session>::iterator session_iterator = _sessions.find(client_descriptor);
            if (session_iterator == _sessions.end())
            {
                return;
            }

            // 소켓은 논블로킹이다. 한 번의 이벤트에서 당장 읽을 수 있는 데이터를 반복해서 꺼낸다.
            while (true)
            {
                const auto received_byte_count = ::recv(client_descriptor, receive_buffer.data(), receive_buffer.size(), 0);

                if (received_byte_count > 0)
                {
                    // 임시 버퍼 중 유효한 구간
                    const std::span<const std::byte> received_bytes{receive_buffer.data(), static_cast<std::size_t>(received_byte_count)};

                    // 세션 내의 디코더가 바이트를 내부 버퍼에 복사하고 완성된 프레임들을 반환
                    // 프레임이 덜 도착했다면 오류 없이 frames가 비어 있을 수 있다.
                    protocol::DecodeResult decode_result = session_iterator->second.appendReceivedBytes(received_bytes);

                    // 잘못된 길이·메시지 종류 등 디코딩 오류는 연결 종료로 처리한다.
                    if (!decode_result.ok())
                    {
                        ++_stats.protocol_errors;
                        std::cerr << "Protocol error from client FD: " << client_descriptor << '\n';
                        close_cause = ConnectionCloseCause::ProtocolError;
                        break;
                    }

                    // 한 번에 여러 프레임이 완성될 수 있다. 연결 식별자와 묶어 다음 처리 단계에 이동 전달한다.
                    for (auto& frame : decode_result.frames)
                    {
                        ++_stats.received_frames;
                        const FramePostResult post_result = _frame_ingress.tryPost(
                            FrameEnvelope{
                                .connection = session_iterator->second.getConnectionId(),
                                .frame = std::move(frame),
                            }
                        );
                        // 전달 거절 시 원인을 구분한다. 큐 포화도 조용히 버리지 않고 이 연결을 종료한다.
                        if (post_result != FramePostResult::Accepted)
                        {
                            if (post_result == FramePostResult::UnsupportedMessage || post_result == FramePostResult::InvalidPayload)
                            {
                                ++_stats.protocol_errors;
                                std::cerr << "Rejected message from client FD: " << client_descriptor << '\n';
                                close_cause = ConnectionCloseCause::ProtocolError;
                            }
                            else if (post_result == FramePostResult::Full)
                            {
                                ++_stats.actor_queue_overflows;
                                std::cerr << "Actor queue limit exceeded for client FD: " << client_descriptor << '\n';
                                close_cause = ConnectionCloseCause::Overflow;
                            }
                            else
                            {
                                close_cause = ConnectionCloseCause::ServerShutdown;
                            }
                            break;
                        }
                    }

                    // 내부 for만 빠져나온 경우 recv 반복문도 종료해야 한다.
                    if (close_cause)
                    {
                        break;
                    }

                    continue;
                }

                // 0은 상대가 송신 방향을 정상 종료했고 더 읽을 바이트가 없다는 뜻이다.
                // 이 서버는 이를 연결 종료로 처리한다. "지금 데이터가 없음"과는 다르다.
                if (received_byte_count == 0)
                {
                    close_cause = ConnectionCloseCause::PeerClosed;
                    break;
                }

                // 여기부터는 recv가 -1을 반환한 경우다. EINTR은 시그널에 의한 중단이므로 재시도한다.
                if (errno == EINTR)
                {
                    continue;
                }

                // 지금 읽을 데이터가 없다는 뜻이다. 연결은 유지하고 이벤트 루프로 돌아간다.
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    break;
                }

                // 그 밖의 수신 오류는 이 구현에서 PeerClosed 사유로 연결을 종료한다.
                close_cause = ConnectionCloseCause::PeerClosed;
                break;
            }
        }

        // 종료 사유가 없고 소켓이 쓰기 가능하면 송신한다. EPOLLIN과 EPOLLOUT은 함께 올 수 있다.
        if (!close_cause && (event_flags & EPOLLOUT) != 0)
        {
            const auto session_iterator = _sessions.find(client_descriptor);
            if (session_iterator == _sessions.end())
            {
                return;
            }

            // 큐가 빌 때까지 보내거나 EAGAIN(송신 버퍼 full)에서 멈춘다. true여도 미전송 데이터가 남을 수 있다.
            if (!flushPendingSend(session_iterator->second))
            {
                // false는 송신 실패
                close_cause = ConnectionCloseCause::PeerClosed;
            }
            else
            {
                // 송신 후 남은 데이터 유무에 맞춰 함수 끝에서 EPOLLOUT 등록을 갱신한다.
                should_update_events = true;
            }

            // 서버 종료 중이고 로직 작업이 정리됐으며 이 연결의 송신 큐도 비었다면 종료한다.
            if (_is_stopping && _logic_runtime_drained && !session_iterator->second.hasPendingSend())
            {
                close_cause = ConnectionCloseCause::ServerShutdown;
            }
        }

        // EPOLLRDHUP은 상대의 송신 방향 종료, EPOLLHUP은 hang-up 알림이다.
        if (!close_cause && (event_flags & (EPOLLRDHUP | EPOLLHUP)) != 0)
        {
            close_cause = _is_stopping ? ConnectionCloseCause::ServerShutdown : ConnectionCloseCause::PeerClosed;
        }

        if (close_cause)
        {
            // 기록한 사유로 실제 Session을 제거. *는 optional 안의 종료 사유를 참조
            removeSession(client_descriptor, *close_cause);
        }
        else if (should_update_events)
        {
            // 송신 데이터가 남으면 EPOLLOUT을 유지하고, 비었으면 제외해 불필요한 알림을 피한다.
            updateClientEvents(_sessions.at(client_descriptor));
        }
    }

    void TcpServer::handleOutboundActions()
    {
        _outbound_queue_depth.record(static_cast<std::uint64_t>(_outbound.size()));

        std::uint64_t wakeup_count = 0;
        while (::read(_outbound_event_descriptor, &wakeup_count, sizeof(wakeup_count)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }

            snf::net::throw_system_error("read(outbound eventfd)");
        }

        if (_on_control_wake)
        {
            _on_control_wake();
        }

        while (true)
        {
            _outbound.drainInto(_drained_outbound_actions, OUTBOUND_DRAIN_BATCH_SIZE);
            if (_drained_outbound_actions.empty())
            {
                break;
            }

            for (PostedOutboundAction& posted : _drained_outbound_actions)
            {
                _outbound_queue_wait_nanoseconds.record(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - posted.posted_at)
                );
                handleOutboundAction(std::move(posted.action));
            }

            _drained_outbound_actions.clear();
        }

        closeConnectionsWithFailedOutboundAdmission();

        static_cast<void>(_outbound.grantPending());

        handleRuntimeCompletion();
        if (_is_stopping && _logic_runtime_drained && isControlDrained())
        {
            completeShutdownAfterLogicRuntimeDrained();
        }
        retryPendingConnectionCloses();
    }

    void TcpServer::closeConnectionsWithFailedOutboundAdmission()
    {
        const bool close_all_sessions = _outbound.takePendingAdmissionFailures(_failed_outbound_admissions);

        for (const snf::net::ConnectionId connection : _failed_outbound_admissions)
        {
            if (findCurrentSession(connection) == nullptr)
            {
                continue;
            }

            ++_stats.outbound_admission_failures;
            std::cerr << "Closing client FD " << connection.descriptor << " because its response could not be admitted to the outbound channel\n";
            removeSession(connection.descriptor, ConnectionCloseCause::Overflow);
        }

        _failed_outbound_admissions.clear();

        if (!close_all_sessions)
        {
            return;
        }

        ++_stats.outbound_admission_failure_fallbacks;
        while (!_sessions.empty())
        {
            ++_stats.outbound_admission_failures;
            removeSession(_sessions.begin()->first, ConnectionCloseCause::Overflow);
        }
    }

    void TcpServer::handleOutboundAction(OutboundAction action)
    {
        std::visit(
            [this](auto&& network_action)
            {
                using Action = std::decay_t<decltype(network_action)>;

                if constexpr (std::is_same_v<Action, SendFrame>)
                {
                    auto* session = findCurrentSession(network_action.connection);
                    if (session == nullptr)
                    {
                        return;
                    }

                    if (!session->enqueueFrame(network_action.frame))
                    {
                        std::cerr << "Send queue limit exceeded for client FD: " << network_action.connection.descriptor << '\n';
                        removeSession(network_action.connection.descriptor, ConnectionCloseCause::Overflow);
                        return;
                    }

                    _session_pending_send_bytes.record(static_cast<std::uint64_t>(session->getPendingSendByteCount()));
                    updateClientEvents(*session);
                }
                else
                {
                    if (findCurrentSession(network_action.connection) == nullptr)
                    {
                        return;
                    }

                    ++_stats.protocol_errors;
                    std::cerr << "Closing client FD " << network_action.connection.descriptor << " because Logic runtime requested "
                              << to_string(network_action.reason) << '\n';
                    removeSession(network_action.connection.descriptor, ConnectionCloseCause::ProtocolError);
                }
            },
            std::move(action)
        );
    }

    void TcpServer::handleRuntimeCompletion()
    {
        if (_runtime_completion.anyRuntimeFailed())
        {
            abortShutdownAfterLogicRuntimeFailure();
            return;
        }

        if (!_logic_runtime_drained && _runtime_completion.allRequiredRuntimesDrained())
        {
            _logic_runtime_drained = true;
            if (_is_stopping && isControlDrained())
            {
                completeShutdownAfterLogicRuntimeDrained();
            }
        }
    }

    void TcpServer::handleStopRequest()
    {
        std::uint64_t stop_value = 0;

        while (::read(_stop_event.getDescriptor(), &stop_value, sizeof(stop_value)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno != EAGAIN && errno != EWOULDBLOCK)
            {
                snf::net::throw_system_error("read(eventfd)");
            }

            break;
        }

        beginShutdown();
    }

    void TcpServer::handleTerminationSignal(const int signal_descriptor)
    {
        signalfd_siginfo signal_information{};

        while (::read(signal_descriptor, &signal_information, sizeof(signal_information)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno != EAGAIN && errno != EWOULDBLOCK)
            {
                snf::net::throw_system_error("read(signalfd)");
            }

            break;
        }

        beginShutdown();
    }

    void TcpServer::beginShutdown()
    {
        if (_is_stopping)
        {
            return;
        }

        _is_stopping = true;
        _shutdown_deadline = std::chrono::steady_clock::now() + _shutdown_grace_period;

        if (_listener.isValid())
        {
            if (::epoll_ctl(_epoll.getDescriptor(), EPOLL_CTL_DEL, _listener.getDescriptor(), nullptr) == -1)
            {
                snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_DEL listener)");
            }

            _listener.init();
        }

        for (const auto& [client_descriptor, session] : _sessions)
        {
            static_cast<void>(client_descriptor);
            updateClientEvents(session);

            ConnectionClosed closed{
                .connection = session.getConnectionId(),
                .cause = ConnectionCloseCause::ServerShutdown,
                .has_location_snapshot = false,
                .last_location = std::nullopt,
            };
            if (_frame_ingress.tryPostConnectionClosed(closed) == PostResult::Full)
            {
                _pending_connection_closes.push_back(std::move(closed));
            }
        }

        _stats.pending_connection_closes_high_water_mark =
            std::max(_stats.pending_connection_closes_high_water_mark, _pending_connection_closes.size());
        retryPendingConnectionCloses();

        if (_logic_runtime_drained && isControlDrained())
        {
            completeShutdownAfterLogicRuntimeDrained();
        }
    }

    void TcpServer::completeShutdownAfterLogicRuntimeDrained()
    {
        std::vector<int> sessions_without_pending_send;
        for (const auto& [client_descriptor, session] : _sessions)
        {
            if (session.hasPendingSend())
            {
                updateClientEvents(session);
            }
            else
            {
                sessions_without_pending_send.push_back(client_descriptor);
            }
        }

        for (const int client_descriptor : sessions_without_pending_send)
        {
            removeSession(client_descriptor, ConnectionCloseCause::ServerShutdown);
        }
    }

    void TcpServer::abortShutdownAfterLogicRuntimeFailure()
    {
        _logic_runtime_drained = true;
        beginShutdown();
        cancelQueues();
        closeRemainingSessions();
    }

    void TcpServer::cancelQueues()
    {
        _frame_ingress.cancel();
        static_cast<void>(_outbound.cancel());
    }

    bool TcpServer::flushPendingSend(snf::net::Session& session)
    {
        // send 한 번은 맨 앞 PendingSend의 남은 구간만 요청한다.
        // 이 반복문은 부분 전송의 나머지나 다음 프레임을 연속해서 요청할 수 있다.
        while (session.hasPendingSend())
        {
            const std::span<const std::byte> pending_bytes = session.getPendingSendBytes();
            // data()는 span 구간의 첫 바이트 주소, size()는 그 구간의 바이트 수다.
            // offset=30인 100바이트 프레임이면 원본의 30번 위치부터 70바이트를 요청한다.
            // data() 자체는 복사나 전송을 하지 않는다. MSG_NOSIGNAL은 송신 시 SIGPIPE 발생을 막는다.
            const auto sent_byte_count = ::send(session.getDescriptor(), pending_bytes.data(), pending_bytes.size(), MSG_NOSIGNAL);

            // 양수는 커널이 받아들인 바이트 수로, 요청량보다 작을 수 있고 상대방의 수신 완료를 뜻하지 않는다.

            if (sent_byte_count > 0)
            {
                if (session.consumeSentBytes(static_cast<std::size_t>(sent_byte_count)))
                {
                    ++_stats.sent_frames;
                }
                continue;
            }

            // -1이면 errno로 원인을 구분한다. 인터럽트는 재시도하고, 지금 보낼 수 없으면 위치를 보존한다.
            if (sent_byte_count == -1 && errno == EINTR)
            {
                continue;
            }

            if (sent_byte_count == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                return true;
            }

            return false;
        }

        return true;
    }

    void TcpServer::updateClientEvents(const snf::net::Session& session) const
    {
        epoll_event client_event{};
        client_event.events = EPOLLRDHUP;
        client_event.data.u64 = session.getConnectionId().generation;

        if (!_is_stopping)
        {
            client_event.events |= EPOLLIN;
        }

        if (session.hasPendingSend())
        {
            client_event.events |= EPOLLOUT;
        }

        if (::epoll_ctl(_epoll.getDescriptor(), EPOLL_CTL_MOD, session.getDescriptor(), &client_event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_MOD client)");
        }
    }

    void TcpServer::removeSession(const int client_descriptor, const ConnectionCloseCause cause)
    {
        const auto session_iterator = _sessions.find(client_descriptor);
        if (session_iterator == _sessions.end())
        {
            return;
        }

        const snf::net::ConnectionId connection = session_iterator->second.getConnectionId();

        if (::epoll_ctl(_epoll.getDescriptor(), EPOLL_CTL_DEL, client_descriptor, nullptr) == -1)
        {
            const int error_number = errno;
            std::cerr << "Failed to remove client FD " << client_descriptor << " from epoll: " << std::generic_category().message(error_number)
                      << '\n';
        }

        _client_descriptors_by_event_token.erase(connection.generation);
        _sessions.erase(session_iterator);
        ++_stats.closed_connections;
        _outbound.forgetConnection(connection);
        std::cout << "Closed client FD: " << client_descriptor << '\n';

        if (!_is_stopping)
        {
            notifyConnectionClosed(
                ConnectionClosed{
                    .connection = connection,
                    .cause = cause,
                    .has_location_snapshot = false,
                    .last_location = std::nullopt,
                }
            );
        }
    }

    void TcpServer::closeRemainingSessions()
    {
        while (!_sessions.empty())
        {
            removeSession(_sessions.begin()->first, ConnectionCloseCause::ServerShutdown);
        }
    }

    void TcpServer::notifyConnectionClosed(ConnectionClosed closed)
    {
        if (_is_stopping)
        {
            return;
        }

        switch (_frame_ingress.tryPostConnectionClosed(closed))
        {
        case PostResult::Accepted:
        case PostResult::Closed:
            return;
        case PostResult::Full:
            if (!hasAvailableConnectionLifecycleSlot())
            {
                throw std::logic_error{"Connection lifecycle capacity invariant violated"};
            }
            _pending_connection_closes.push_back(std::move(closed));
            _stats.pending_connection_closes_high_water_mark =
                std::max(_stats.pending_connection_closes_high_water_mark, _pending_connection_closes.size());
            return;
        }
    }

    void TcpServer::retryPendingConnectionCloses()
    {
        const std::size_t attempt_count = std::min(_pending_connection_closes.size(), CONNECTION_CLOSE_RETRY_BUDGET);

        for (std::size_t attempt = 0; attempt < attempt_count; ++attempt)
        {
            ConnectionClosed closed = std::move(_pending_connection_closes.front());
            _pending_connection_closes.pop_front();

            if (_frame_ingress.tryPostConnectionClosed(closed) == PostResult::Full)
            {
                _pending_connection_closes.push_back(std::move(closed));
            }
        }

        closeFrameIngressAfterConnectionLifecyclesDrain();
    }

    void TcpServer::closeFrameIngressAfterConnectionLifecyclesDrain()
    {
        if (!_is_stopping || _frame_ingress_closed || !_pending_connection_closes.empty())
        {
            return;
        }

        _frame_ingress_closed = true;
        _frame_ingress.close();
    }

    void TcpServer::reportMetricsIfDue()
    {
        if (!hasMetricsReporting())
        {
            return;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now < _next_metrics_report)
        {
            return;
        }

        _next_metrics_report = now + _metrics_report_interval;
        _on_metrics_interval();
    }

    bool TcpServer::hasAvailableConnectionLifecycleSlot() const noexcept
    {
        return _sessions.size() < _connection_lifecycle_capacity &&
               _pending_connection_closes.size() < _connection_lifecycle_capacity - _sessions.size();
    }

    bool TcpServer::isControlDrained() const noexcept
    {
        return !_is_control_drained || _is_control_drained();
    }

    bool TcpServer::hasMetricsReporting() const noexcept
    {
        return _on_metrics_interval != nullptr && _metrics_report_interval > std::chrono::milliseconds::zero();
    }

    bool TcpServer::hasShutdownDeadlineExpired() const noexcept
    {
        return std::chrono::steady_clock::now() >= _shutdown_deadline;
    }

    int TcpServer::getEpollWaitTimeout() const
    {
        int timeout = -1;
        if (_is_stopping)
        {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(_shutdown_deadline - std::chrono::steady_clock::now());
            timeout = static_cast<int>(std::clamp<std::int64_t>(remaining.count(), 0, std::numeric_limits<int>::max()));
        }

        if (!_pending_connection_closes.empty())
        {
            const int retry_timeout = static_cast<int>(CONNECTION_CLOSE_RETRY_INTERVAL.count());
            timeout = timeout == -1 ? retry_timeout : std::min(timeout, retry_timeout);
        }

        if (hasMetricsReporting())
        {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(_next_metrics_report - std::chrono::steady_clock::now());
            const int report_timeout = static_cast<int>(std::clamp<std::int64_t>(remaining.count(), 0, std::numeric_limits<int>::max()));
            timeout = timeout == -1 ? report_timeout : std::min(timeout, report_timeout);
        }

        return timeout;
    }

    snf::net::Session* TcpServer::findCurrentSession(const snf::net::ConnectionId connection)
    {
        const auto iterator = _sessions.find(connection.descriptor);
        if (iterator == _sessions.end() || iterator->second.getConnectionId() != connection)
        {
            ++_stats.stale_outbound_actions;
            return nullptr;
        }

        return &iterator->second;
    }
}
