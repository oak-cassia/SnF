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

        if (::getsockname(
                listener_descriptor, reinterpret_cast<sockaddr*>(&address), &address_size) == -1)
        {
            snf::net::throw_system_error("getsockname");
        }

        return ntohs(address.sin_port);
    }
}

namespace snf::server
{
    TcpServer::TcpServer(const TcpServerConfig& config,
                         snf::runtime::BoundedQueue<InboundCommand>& inbound_commands,
                         snf::runtime::BoundedQueue<NetworkAction>& network_actions,
                         const int outbound_event_descriptor)
        : _listener(snf::net::create_tcp_listener(config.port))
        , _epoll(create_epoll_instance())
        , _stop_event(create_stop_event())
        , _port(get_listener_port(_listener.getDescriptor()))
        , _shutdown_grace_period(config.shutdown_grace_period)
        , _max_pending_send_bytes(config.max_pending_send_bytes)
        , _client_send_buffer_size(config.client_send_buffer_size)
        , _inbound_commands(inbound_commands)
        , _network_actions(network_actions)
        , _outbound_event_descriptor(outbound_event_descriptor)
    {
        if (_shutdown_grace_period < std::chrono::milliseconds::zero() ||
            _max_pending_send_bytes == 0 ||
            (_client_send_buffer_size && *_client_send_buffer_size <= 0) ||
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

    void TcpServer::run(const int termination_signal_descriptor)
    {
        if (termination_signal_descriptor != snf::net::UniqueFileDescriptor::INVALID_FD)
        {
            registerControlDescriptor(termination_signal_descriptor,
                                      TERMINATION_SIGNAL_EVENT_TOKEN);
        }

        std::array<epoll_event, MAX_READY_EVENTS> events{};

        while (true)
        {
            if (_is_stopping && _game_runtime_drained && _sessions.empty())
            {
                break;
            }

            const int wait_timeout = getEpollWaitTimeout();
            if (_is_stopping && wait_timeout == 0)
            {
                cancelQueues();
                break;
            }

            const int ready_event_count = ::epoll_wait(_epoll.getDescriptor(),
                                                       events.data(),
                                                       static_cast<int>(events.size()),
                                                       wait_timeout);

            if (ready_event_count == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                snf::net::throw_system_error("epoll_wait");
            }

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

                if (event_token == STOP_EVENT_TOKEN ||
                    event_token == TERMINATION_SIGNAL_EVENT_TOKEN ||
                    event_token == OUTBOUND_EVENT_TOKEN)
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

                const auto descriptor_iterator =
                    _client_descriptors_by_event_token.find(event_token);
                if (descriptor_iterator == _client_descriptors_by_event_token.end())
                {
                    continue;
                }

                const int client_descriptor = descriptor_iterator->second;
                handleClientEvent(client_descriptor, event.events);
            }
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

        if (::epoll_ctl(_epoll.getDescriptor(),
                        EPOLL_CTL_ADD,
                        _listener.getDescriptor(),
                        &listener_event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_ADD listener)");
        }
    }

    void TcpServer::registerControlDescriptor(const int descriptor,
                                              const std::uint64_t event_token) const
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
            const int client_descriptor = ::accept4(
                _listener.getDescriptor(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);

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
            snf::net::enable_tcp_no_delay(client_socket.getDescriptor());

            if (_client_send_buffer_size)
            {
                snf::net::set_socket_send_buffer_size(client_socket.getDescriptor(),
                                                      *_client_send_buffer_size);
            }

            if (_next_connection_generation == MAX_CONNECTION_GENERATION)
            {
                throw std::overflow_error{"Connection generation exhausted"};
            }

            const ConnectionId connection{
                .descriptor = client_descriptor,
                .generation = ++_next_connection_generation,
            };
            const bool inserted =
                _sessions
                    .emplace(client_descriptor,
                             snf::net::Session{
                                 std::move(client_socket), connection, _max_pending_send_bytes})
                    .second;

            if (!inserted)
            {
                throw std::logic_error{"A session already owns the client descriptor"};
            }

            const bool event_token_inserted =
                _client_descriptors_by_event_token.emplace(connection.generation, client_descriptor)
                    .second;
            if (!event_token_inserted)
            {
                throw std::logic_error{"A client event token is already registered"};
            }

            epoll_event client_event{};
            client_event.events = EPOLLIN | EPOLLRDHUP;
            client_event.data.u64 = connection.generation;

            if (::epoll_ctl(
                    _epoll.getDescriptor(), EPOLL_CTL_ADD, client_descriptor, &client_event) == -1)
            {
                snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_ADD client)");
            }

            ++_stats.accepted_connections;
            std::cout << "Accepted client FD: " << client_descriptor << '\n';
        }
    }

    void TcpServer::handleClientEvent(const int client_descriptor, const std::uint32_t event_flags)
    {
        bool should_remove_session = (event_flags & EPOLLERR) != 0;
        bool should_update_events = false;

        const bool has_read_event =
            !_is_stopping && (event_flags & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0;

        if (!should_remove_session && has_read_event)
        {
            std::array<std::byte, RECEIVE_BUFFER_SIZE> receive_buffer{};

            const auto session_iterator = _sessions.find(client_descriptor);
            if (session_iterator == _sessions.end())
            {
                return;
            }

            while (true)
            {
                const auto received_byte_count =
                    ::recv(client_descriptor, receive_buffer.data(), receive_buffer.size(), 0);

                if (received_byte_count > 0)
                {
                    const std::span<const std::byte> received_bytes{
                        receive_buffer.data(), static_cast<std::size_t>(received_byte_count)
                    };

                    const auto decode_result =
                        session_iterator->second.appendReceivedBytes(received_bytes);

                    if (!decode_result.ok())
                    {
                        ++_stats.protocol_errors;
                        std::cerr << "Protocol error from client FD: " << client_descriptor << '\n';
                        should_remove_session = true;
                        break;
                    }

                    for (const auto& frame : decode_result.frames)
                    {
                        ++_stats.received_frames;
                        if (!_inbound_commands.tryPush(InboundCommand{
                                .connection = session_iterator->second.getConnectionId(),
                                .frame = frame,
                            }))
                        {
                            ++_stats.game_queue_overflows;
                            std::cerr << "Game queue limit exceeded for client FD: "
                                      << client_descriptor << '\n';
                            should_remove_session = true;
                            break;
                        }
                    }

                    if (should_remove_session)
                    {
                        break;
                    }

                    continue;
                }

                if (received_byte_count == 0)
                {
                    should_remove_session = true;
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

                should_remove_session = true;
                break;
            }
        }

        if (!should_remove_session && (event_flags & EPOLLOUT) != 0)
        {
            const auto session_iterator = _sessions.find(client_descriptor);
            if (session_iterator == _sessions.end())
            {
                return;
            }

            should_remove_session = !flushPendingSend(session_iterator->second);
            should_update_events = !should_remove_session;

            if (_is_stopping && _game_runtime_drained &&
                !session_iterator->second.hasPendingSend())
            {
                should_remove_session = true;
            }
        }

        if ((event_flags & (EPOLLRDHUP | EPOLLHUP)) != 0)
        {
            should_remove_session = true;
        }

        if (should_remove_session)
        {
            removeSession(client_descriptor);
        }
        else if (should_update_events)
        {
            updateClientEvents(_sessions.at(client_descriptor));
        }
    }

    void TcpServer::handleOutboundActions()
    {
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

        while (auto action = _network_actions.tryPop())
        {
            handleNetworkAction(std::move(*action));
        }
    }

    void TcpServer::handleNetworkAction(NetworkAction action)
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
                        std::cerr << "Send queue limit exceeded for client FD: "
                                  << network_action.connection.descriptor << '\n';
                        removeSession(network_action.connection.descriptor);
                        return;
                    }

                    updateClientEvents(*session);
                }
                else if constexpr (std::is_same_v<Action, CloseConnection>)
                {
                    if (findCurrentSession(network_action.connection) == nullptr)
                    {
                        return;
                    }

                    ++_stats.protocol_errors;
                    std::cerr << "Closing client FD " << network_action.connection.descriptor
                              << " because GameRuntime requested "
                              << to_string(network_action.reason) << '\n';
                    removeSession(network_action.connection.descriptor);
                }
                else
                {
                    _game_runtime_drained = true;
                    if (_is_stopping)
                    {
                        completeShutdownAfterGameRuntimeDrained();
                    }
                }
            },
            std::move(action));
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
        _inbound_commands.close();

        if (_listener.isValid())
        {
            if (::epoll_ctl(
                    _epoll.getDescriptor(), EPOLL_CTL_DEL, _listener.getDescriptor(), nullptr) == -1)
            {
                snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_DEL listener)");
            }

            _listener.init();
        }

        for (const auto& [client_descriptor, session] : _sessions)
        {
            static_cast<void>(client_descriptor);
            updateClientEvents(session);
        }

        if (_game_runtime_drained)
        {
            completeShutdownAfterGameRuntimeDrained();
        }
    }

    void TcpServer::completeShutdownAfterGameRuntimeDrained()
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
            removeSession(client_descriptor);
        }
    }

    void TcpServer::cancelQueues()
    {
        _inbound_commands.cancel();
        _network_actions.cancel();
    }

    bool TcpServer::flushPendingSend(snf::net::Session& session)
    {
        while (session.hasPendingSend())
        {
            const std::span<const std::byte> pending_bytes = session.getPendingSendBytes();
            const auto sent_byte_count = ::send(
                session.getDescriptor(), pending_bytes.data(), pending_bytes.size(), MSG_NOSIGNAL);

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

        if (::epoll_ctl(
                _epoll.getDescriptor(), EPOLL_CTL_MOD, session.getDescriptor(), &client_event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_MOD client)");
        }
    }

    void TcpServer::removeSession(const int client_descriptor)
    {
        const auto session_iterator = _sessions.find(client_descriptor);
        if (session_iterator == _sessions.end())
        {
            return;
        }

        if (::epoll_ctl(_epoll.getDescriptor(), EPOLL_CTL_DEL, client_descriptor, nullptr) == -1)
        {
            const int error_number = errno;
            std::cerr << "Failed to remove client FD " << client_descriptor
                      << " from epoll: "
                      << std::generic_category().message(error_number) << '\n';
        }

        _client_descriptors_by_event_token.erase(
            session_iterator->second.getConnectionId().generation);
        _sessions.erase(session_iterator);
        ++_stats.closed_connections;
        std::cout << "Closed client FD: " << client_descriptor << '\n';
    }

    void TcpServer::closeRemainingSessions()
    {
        while (!_sessions.empty())
        {
            removeSession(_sessions.begin()->first);
        }
    }

    int TcpServer::getEpollWaitTimeout() const
    {
        if (!_is_stopping)
        {
            return -1;
        }

        const auto now = std::chrono::steady_clock::now();
        if (now >= _shutdown_deadline)
        {
            return 0;
        }

        const auto remaining =
            std::chrono::ceil<std::chrono::milliseconds>(_shutdown_deadline - now);
        return static_cast<int>(
            std::min<std::int64_t>(remaining.count(), std::numeric_limits<int>::max()));
    }

    snf::net::Session* TcpServer::findCurrentSession(const ConnectionId connection)
    {
        const auto iterator = _sessions.find(connection.descriptor);
        if (iterator == _sessions.end() || iterator->second.getConnectionId() != connection)
        {
            ++_stats.stale_network_actions;
            return nullptr;
        }

        return &iterator->second;
    }
}
