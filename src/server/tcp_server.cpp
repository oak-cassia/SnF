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
        const TcpServerConfig& config, FrameIngress& frame_ingress, OutboundChannel& outbound, snf::runtime::RuntimeCompletionSource& runtime_completion, const int outbound_event_descriptor)
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
        if (_shutdown_grace_period < std::chrono::milliseconds::zero() || _metrics_report_interval < std::chrono::milliseconds::zero() || _max_pending_send_bytes == 0 ||
            (_client_send_buffer_size && *_client_send_buffer_size <= 0) || _connection_lifecycle_capacity == 0 || _outbound_event_descriptor == snf::net::UniqueFileDescriptor::INVALID_FD)
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
        if (termination_signal_descriptor != snf::net::UniqueFileDescriptor::INVALID_FD)
        {
            registerControlDescriptor(termination_signal_descriptor, TERMINATION_SIGNAL_EVENT_TOKEN);
        }

        std::array<epoll_event, MAX_READY_EVENTS> events{};
        if (hasMetricsReporting())
        {
            _next_metrics_report = std::chrono::steady_clock::now() + _metrics_report_interval;
        }

        while (true)
        {
            retryPendingConnectionCloses();

            if (_is_stopping && _logic_runtime_drained && isControlDrained() && _sessions.empty())
            {
                break;
            }

            if (_is_stopping && hasShutdownDeadlineExpired())
            {
                cancelQueues();
                break;
            }

            const int ready_event_count = ::epoll_wait(_epoll.getDescriptor(), events.data(), static_cast<int>(events.size()), getEpollWaitTimeout());

            if (ready_event_count == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                snf::net::throw_system_error("epoll_wait");
            }

            const auto turn_started_at = std::chrono::steady_clock::now();

            for (int event_index = 0; event_index < ready_event_count; ++event_index)
            {
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
                    handleOutboundActions();
                }
            }

            for (int event_index = 0; event_index < ready_event_count; ++event_index)
            {
                const epoll_event& event = events[event_index];
                const std::uint64_t event_token = event.data.u64;

                if (event_token == STOP_EVENT_TOKEN || event_token == TERMINATION_SIGNAL_EVENT_TOKEN || event_token == OUTBOUND_EVENT_TOKEN)
                {
                    continue;
                }

                if (event_token == LISTENER_EVENT_TOKEN)
                {
                    if (!_is_stopping)
                    {
                        acceptPendingClients();
                    }

                    continue;
                }

                const auto descriptor_iterator = _client_descriptors_by_event_token.find(event_token);
                if (descriptor_iterator == _client_descriptors_by_event_token.end())
                {
                    continue;
                }

                const int client_descriptor = descriptor_iterator->second;
                handleClientEvent(client_descriptor, event.events);
            }

            if (ready_event_count > 0)
            {
                _reactor_turn_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - turn_started_at));
            }

            // Reported after the turn is measured so that the report itself does
            // not inflate the reactor turn distribution.
            reportMetricsIfDue();
        }

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
                // Leave any remaining listener backlog for a later reactor turn
                // so a connection flood cannot monopolize this turn.
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
            const bool inserted = _sessions.emplace(client_descriptor, snf::net::Session{std::move(client_socket), connection, _max_pending_send_bytes}).second;

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

            // Creates this connection's outbound accounting once, so its commands do
            // not each allocate one inside the channel lock.
            _outbound.trackConnection(connection);

            ++_stats.accepted_connections;
            std::cout << "Accepted client FD: " << client_descriptor << '\n';
        }
    }

    void TcpServer::handleClientEvent(const int client_descriptor, const std::uint32_t event_flags)
    {
        std::optional<ConnectionCloseCause> close_cause;
        if ((event_flags & EPOLLERR) != 0)
        {
            close_cause = ConnectionCloseCause::PeerClosed;
        }
        bool should_update_events = false;

        const bool has_read_event = !_is_stopping && (event_flags & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0;

        if (!close_cause && has_read_event)
        {
            std::array<std::byte, RECEIVE_BUFFER_SIZE> receive_buffer{};

            const auto session_iterator = _sessions.find(client_descriptor);
            if (session_iterator == _sessions.end())
            {
                return;
            }

            while (true)
            {
                const auto received_byte_count = ::recv(client_descriptor, receive_buffer.data(), receive_buffer.size(), 0);

                if (received_byte_count > 0)
                {
                    const std::span<const std::byte> received_bytes{receive_buffer.data(), static_cast<std::size_t>(received_byte_count)};

                    auto decode_result = session_iterator->second.appendReceivedBytes(received_bytes);

                    if (!decode_result.ok())
                    {
                        ++_stats.protocol_errors;
                        std::cerr << "Protocol error from client FD: " << client_descriptor << '\n';
                        close_cause = ConnectionCloseCause::ProtocolError;
                        break;
                    }

                    for (auto& frame : decode_result.frames)
                    {
                        ++_stats.received_frames;
                        const snf::net::ConnectionId connection = session_iterator->second.getConnectionId();
                        const FramePostResult post_result = _frame_ingress.tryPost(FrameEnvelope{
                            .connection = connection,
                            .frame = std::move(frame),
                        });
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

                    if (close_cause)
                    {
                        break;
                    }

                    continue;
                }

                if (received_byte_count == 0)
                {
                    close_cause = ConnectionCloseCause::PeerClosed;
                    break;
                }

                if (errno == EINTR)
                {
                    continue;
                }

                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    break;
                }

                close_cause = ConnectionCloseCause::PeerClosed;
                break;
            }
        }

        if (!close_cause && (event_flags & EPOLLOUT) != 0)
        {
            const auto session_iterator = _sessions.find(client_descriptor);
            if (session_iterator == _sessions.end())
            {
                return;
            }

            if (!flushPendingSend(session_iterator->second))
            {
                close_cause = ConnectionCloseCause::PeerClosed;
            }
            else
            {
                should_update_events = true;
            }

            if (_is_stopping && _logic_runtime_drained && !session_iterator->second.hasPendingSend())
            {
                close_cause = ConnectionCloseCause::ServerShutdown;
            }
        }

        if (!close_cause && (event_flags & (EPOLLRDHUP | EPOLLHUP)) != 0)
        {
            close_cause = _is_stopping ? ConnectionCloseCause::ServerShutdown : ConnectionCloseCause::PeerClosed;
        }

        if (close_cause)
        {
            removeSession(client_descriptor, *close_cause);
        }
        else if (should_update_events)
        {
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
                _outbound_queue_wait_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - posted.posted_at));
                handleOutboundAction(std::move(posted.action));
            }

            _drained_outbound_actions.clear();
        }

        closeConnectionsWithFailedOutboundAdmission();

        // Granting after the drain, so capacity freed in this turn reaches a waiting
        // actor without costing it another reactor turn. The reactor is the only
        // granter, which is what bounds this work per turn.
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

        // The channel could not retain one exact connection ID without violating its
        // fixed memory bound. Closing every current session is deliberately conservative:
        // it guarantees the affected live connection is not left with a silently missing
        // response, while the Worker that detected the condition remains healthy.
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
                    std::cerr << "Closing client FD " << network_action.connection.descriptor << " because Logic runtime requested " << to_string(network_action.reason) << '\n';
                    removeSession(network_action.connection.descriptor, ConnectionCloseCause::ProtocolError);
                }
            },
            std::move(action));
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

        _stats.pending_connection_closes_high_water_mark = std::max(_stats.pending_connection_closes_high_water_mark, _pending_connection_closes.size());
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
        // Cancelling releases every actor waiting for capacity. Without it a Logic
        // Worker would wait for a grant this reactor can no longer make, and the
        // Logic Runtime would never reach its drained state.
        static_cast<void>(_outbound.cancel());
    }

    bool TcpServer::flushPendingSend(snf::net::Session& session)
    {
        while (session.hasPendingSend())
        {
            const std::span<const std::byte> pending_bytes = session.getPendingSendBytes();
            const auto sent_byte_count = ::send(session.getDescriptor(), pending_bytes.data(), pending_bytes.size(), MSG_NOSIGNAL);

            if (sent_byte_count > 0)
            {
                if (session.consumeSentBytes(static_cast<std::size_t>(sent_byte_count)))
                {
                    ++_stats.sent_frames;
                }
                continue;
            }

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
            std::cerr << "Failed to remove client FD " << client_descriptor << " from epoll: " << std::generic_category().message(error_number) << '\n';
        }

        _client_descriptors_by_event_token.erase(connection.generation);
        _sessions.erase(session_iterator);
        ++_stats.closed_connections;
        // Releases this connection's outbound accounting once whatever it still holds
        // has drained. Until this point the channel keeps the entry alive so a live
        // connection's commands do not allocate one each.
        _outbound.forgetConnection(connection);
        std::cout << "Closed client FD: " << client_descriptor << '\n';

        if (!_is_stopping)
        {
            notifyConnectionClosed(ConnectionClosed{
                .connection = connection,
                .cause = cause,
                .has_location_snapshot = false,
                .last_location = std::nullopt,
            });
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
            _stats.pending_connection_closes_high_water_mark = std::max(_stats.pending_connection_closes_high_water_mark, _pending_connection_closes.size());
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

        // ConnectionClosed drives Player snapshot persistence and actor eviction.
        // Close Logic ingress only after every retained lifecycle fact was admitted.
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
        return _sessions.size() < _connection_lifecycle_capacity && _pending_connection_closes.size() < _connection_lifecycle_capacity - _sessions.size();
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
