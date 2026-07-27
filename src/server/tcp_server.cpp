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
#include <unistd.h>
#include <utility>
#include <vector>

namespace
{
    constexpr std::size_t MAX_READY_EVENTS = 64;
    constexpr std::size_t RECEIVE_BUFFER_SIZE = 4096;

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
    TcpServer::TcpServer(const TcpServerConfig& config)
        : _listener(snf::net::create_tcp_listener(config.port))
          , _epoll(create_epoll_instance())
          , _stop_event(create_stop_event())
          , _port(get_listener_port(_listener.getDescriptor()))
          , _shutdown_grace_period(config.shutdown_grace_period)
          , _max_pending_send_bytes(config.max_pending_send_bytes)
          , _client_send_buffer_size(config.client_send_buffer_size)
    {
        if (_shutdown_grace_period < std::chrono::milliseconds::zero() ||
            _max_pending_send_bytes == 0 ||
            (_client_send_buffer_size && *_client_send_buffer_size <= 0))
        {
            throw std::invalid_argument{"Invalid TCP server configuration"};
        }

        registerListener();
        registerControlDescriptor(_stop_event.getDescriptor());
    }

    TcpServer::TcpServer(const std::uint16_t port,
                         const std::chrono::milliseconds shutdown_grace_period)
        : TcpServer(TcpServerConfig{
            .port = port,
            .shutdown_grace_period = shutdown_grace_period,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
        })
    {
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
            registerControlDescriptor(termination_signal_descriptor);
        }

        std::array<epoll_event, MAX_READY_EVENTS> events{};

        // 종료 요청 후 모든 송신 queue가 비거나 종료 유예 시간이 끝날 때까지 실행한다.
        while (!_is_stopping || !_sessions.empty())
        {
            const int wait_timeout = getEpollWaitTimeout();
            if (_is_stopping && wait_timeout == 0)
            {
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
                const int ready_descriptor = events[event_index].data.fd;

                if (ready_descriptor == _stop_event.getDescriptor())
                {
                    handleStopRequest();
                }
                else if (ready_descriptor == termination_signal_descriptor)
                {
                    handleTerminationSignal(termination_signal_descriptor);
                }
            }

            for (int event_index = 0; event_index < ready_event_count; ++event_index)
            {
                const epoll_event& event = events[event_index];

                if (event.data.fd == _stop_event.getDescriptor() ||
                    event.data.fd == termination_signal_descriptor)
                {
                    continue;
                }

                if (event.data.fd == _listener.getDescriptor())
                {
                    if (!_is_stopping)
                    {
                        acceptPendingClients();
                    }

                    continue;
                }

                handleClientEvent(event.data.fd, event.events);
            }
        }

        closeRemainingSessions();
    }

    void TcpServer::requestStop() const noexcept
    {
        // 값을 0에서 1로 주어, run 함수에서 중단을 알 수 있게 함
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
        listener_event.data.fd = _listener.getDescriptor();

        if (::epoll_ctl(_epoll.getDescriptor(),
                        EPOLL_CTL_ADD,
                        _listener.getDescriptor(),
                        &listener_event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_ADD listener)");
        }
    }

    void TcpServer::registerControlDescriptor(const int descriptor) const
    {
        epoll_event control_event{};
        control_event.events = EPOLLIN;
        control_event.data.fd = descriptor;

        if (::epoll_ctl(_epoll.getDescriptor(), EPOLL_CTL_ADD, descriptor, &control_event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_ADD control)");
        }
    }

    void TcpServer::acceptPendingClients()
    {
        // accept 대기열에 남은 연결이 없을 때까지 모두 수락한다.
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

            const bool inserted =
                _sessions
                .emplace(client_descriptor,
                         snf::net::Session{std::move(client_socket), _max_pending_send_bytes})
                .second;

            if (!inserted)
            {
                throw std::logic_error{"A session already owns the client descriptor"};
            }

            epoll_event client_event{};
            client_event.events = EPOLLIN | EPOLLRDHUP;
            client_event.data.fd = client_descriptor;

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

            // EAGAIN, EOF 또는 복구할 수 없는 소켓 오류가 발생할 때까지 수신한다.
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

                        const auto dispatch_result = _message_dispatcher.dispatch(frame);
                        if (!dispatch_result.handled())
                        {
                            ++_stats.protocol_errors;
                            std::cerr << "No handler for message type from client FD: "
                                << client_descriptor << '\n';
                            should_remove_session = true;
                            break;
                        }

                        for (const auto& response : dispatch_result.responses)
                        {
                            if (!session_iterator->second.enqueueFrame(response))
                            {
                                std::cerr << "Send queue limit exceeded for client FD: "
                                    << client_descriptor << '\n';
                                should_remove_session = true;
                                break;
                            }

                            should_update_events = true;
                        }

                        if (should_remove_session)
                        {
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

            if (_is_stopping && !session_iterator->second.hasPendingSend())
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
            if (::epoll_ctl(
                _epoll.getDescriptor(), EPOLL_CTL_DEL, _listener.getDescriptor(), nullptr) == -1)
            {
                snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_DEL listener)");
            }

            _listener.init();
        }

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

    bool TcpServer::flushPendingSend(snf::net::Session& session)
    {
        // 송신 queue가 비거나 소켓이 EAGAIN을 반환할 때까지 전송한다.
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
        client_event.data.fd = session.getDescriptor();

        if (!_is_stopping)
        {
            client_event.events |= EPOLLIN;
        }

        if (session.hasPendingSend())
        {
            client_event.events |= EPOLLOUT;
        }

        if (::epoll_ctl(
                _epoll.getDescriptor(), EPOLL_CTL_MOD, session.getDescriptor(), &client_event) ==
            -1)
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
                << " from epoll: " << std::generic_category().message(error_number) << '\n';
        }

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
}
