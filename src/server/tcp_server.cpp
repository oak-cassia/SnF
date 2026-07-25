#include "snf/server/tcp_server.hpp"

#include "snf/net/system_error.hpp"
#include "snf/net/tcp_listener.hpp"

#include <array>
#include <cerrno>
#include <cstddef>
#include <iostream>
#include <span>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <system_error>

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
}

namespace snf::server
{
    TcpServer::TcpServer(const std::uint16_t port)
        : _listener(snf::net::create_tcp_listener(port))
        , _epoll(create_epoll_instance())
    {
        registerListener();
    }

    void TcpServer::run()
    {
        std::array<epoll_event, MAX_READY_EVENTS> events{};

        // 종료 요청이 이벤트 루프를 멈추거나 복구할 수 없는 오류가 발생할 때까지 실행한다.
        while (true)
        {
            const int ready_event_count = ::epoll_wait(
                _epoll.getDescriptor(), events.data(), static_cast<int>(events.size()), -1);

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
                const epoll_event& event = events[event_index];

                if (event.data.fd == _listener.getDescriptor())
                {
                    acceptPendingClients();
                    continue;
                }

                handleClientEvent(event.data.fd, event.events);
            }
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

            const bool inserted =
                _sessions
                    .emplace(client_descriptor,
                             snf::net::Session{snf::net::UniqueFileDescriptor{client_descriptor}})
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

            std::cout << "Accepted client FD: " << client_descriptor << '\n';
        }
    }

    void TcpServer::handleClientEvent(const int client_descriptor, const std::uint32_t event_flags)
    {
        bool should_remove_session = (event_flags & EPOLLERR) != 0;

        const bool has_read_event = (event_flags & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0;

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
                        receive_buffer.data(), static_cast<std::size_t>(received_byte_count)};

                    const auto decode_result =
                        session_iterator->second.appendReceivedBytes(received_bytes);

                    if (!decode_result.ok())
                    {
                        std::cerr << "Protocol error from client FD: " << client_descriptor << '\n';

                        should_remove_session = true;
                        break;
                    }

                    for (const auto& frame : decode_result.frames)
                    {
                        std::cout << "Received frame from FD " << client_descriptor
                                  << ", request ID: " << frame.request_id << '\n';
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

        if ((event_flags & (EPOLLRDHUP | EPOLLHUP)) != 0)
        {
            should_remove_session = true;
        }

        if (should_remove_session)
        {
            removeSession(client_descriptor);
        }
    }

    void TcpServer::removeSession(const int client_descriptor)
    {
        if (::epoll_ctl(_epoll.getDescriptor(), EPOLL_CTL_DEL, client_descriptor, nullptr) == -1)
        {
            const int error_number = errno;
            std::cerr << "Failed to remove client FD " << client_descriptor
                      << " from epoll: " << std::generic_category().message(error_number) << '\n';
        }

        _sessions.erase(client_descriptor);

        std::cout << "Closed client FD: " << client_descriptor << '\n';
    }
}
