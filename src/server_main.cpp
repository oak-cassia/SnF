#include <array>

#include "snf/net/tcp_listener.hpp"
#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/net/system_error.hpp"

#include <cerrno>
#include <iostream>
#include <stdexcept>
#include <system_error>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unordered_map>
#include <cstddef>
#include <span>

int main()
{
    try
    {
        auto listener = snf::net::create_tcp_listener(7777);

        const int epoll_file_descriptor = ::epoll_create1(EPOLL_CLOEXEC);

        if (epoll_file_descriptor == -1)
        {
            snf::net::throw_system_error("epoll_create1");
        }

        snf::net::UniqueFileDescriptor epoll{
            epoll_file_descriptor
        };

        epoll_event listener_event{};
        listener_event.events = EPOLLIN;
        listener_event.data.fd = listener.getDescriptor();

        if (::epoll_ctl(epoll.getDescriptor(), EPOLL_CTL_ADD, listener.getDescriptor(), &listener_event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_ADD)");
        }

        std::cout << "Listening on port 7777\n";

        std::unordered_map<int, snf::net::Session> sessions;
        std::array<epoll_event, 64> events{};
        std::array<std::byte, 4096> receive_buffer{};
        while (true)
        {
            const int ready_event_count = ::epoll_wait(epoll.getDescriptor(), events.data(), static_cast<int>(events.size()), -1);

            if (ready_event_count == -1)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                snf::net::throw_system_error("epoll_wait");
            }

            for (int i = 0; i < ready_event_count; ++i)
            {
                const epoll_event& event = events[i];

                if (event.data.fd == listener.getDescriptor())
                {
                    while (true)
                    {
                        const int client_file_descriptor = ::accept4(
                            listener.getDescriptor(),
                            nullptr,
                            nullptr,
                            SOCK_NONBLOCK | SOCK_CLOEXEC
                        );

                        if (client_file_descriptor == -1)
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                            {
                                break;
                            }

                            if (errno == EINTR)
                            {
                                continue;
                            }

                            snf::net::throw_system_error("accept4");
                        }

                        const int client_descriptor = client_file_descriptor;
                        const auto insertion_result = sessions.emplace(
                            client_descriptor,
                            snf::net::Session{
                                snf::net::UniqueFileDescriptor{
                                    client_file_descriptor
                                }
                            }
                        );

                        if (!insertion_result.second)
                        {
                            throw std::logic_error{
                                "A session already owns the client descriptor"
                            };
                        }

                        epoll_event client_event{};
                        client_event.events = EPOLLIN | EPOLLRDHUP;
                        client_event.data.fd = client_descriptor;

                        if (::epoll_ctl(
                            epoll.getDescriptor(),
                            EPOLL_CTL_ADD,
                            client_descriptor,
                            &client_event) == -1)
                        {
                            snf::net::throw_system_error(
                                "epoll_ctl(EPOLL_CTL_ADD client)"
                            );
                        }

                        std::cout << "Accepted client FD: "
                            << client_descriptor << '\n';
                    }

                    continue;
                }

                // client 수신
                const int client_descriptor = event.data.fd;

                const auto session_iterator = sessions.find(client_descriptor);
                if (session_iterator == sessions.end())
                {
                    continue;
                }

                bool should_remove_session = false;
                if ((event.events & EPOLLERR) != 0)
                {
                    should_remove_session = true;
                }

                const bool has_read_event = (event.events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0;
                if (!should_remove_session && has_read_event)
                {
                    while (true)
                    {
                        const auto received_byte_count = ::recv(
                            client_descriptor,
                            receive_buffer.data(),
                            receive_buffer.size(),
                            0
                        );
                        if (received_byte_count > 0)
                        {
                            const std::span<const std::byte> received_bytes{
                                receive_buffer.data(),
                                static_cast<std::size_t>(received_byte_count)
                            };

                            const auto decode_result = session_iterator->second.appendReceivedBytes(received_bytes);

                            if (!decode_result.ok())
                            {
                                std::cerr
                                    << "Protocol error from client FD: "
                                    << client_descriptor
                                    << '\n';

                                should_remove_session = true;
                                break;
                            }

                            for (const auto& frame : decode_result.frames)
                            {
                                std::cout
                                    << "Received frame from FD "
                                    << client_descriptor
                                    << ", request ID: "
                                    << frame.request_id
                                    << '\n';
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

                if ((event.events & (EPOLLRDHUP | EPOLLHUP)) != 0)
                {
                    should_remove_session = true;
                }

                if (!should_remove_session)
                {
                    continue;
                }

                if (::epoll_ctl(
                    epoll.getDescriptor(),
                    EPOLL_CTL_DEL,
                    client_descriptor,
                    nullptr
                ) == -1)
                {
                    std::cerr
                        << "Failed to remove client FD from epoll: "
                        << client_descriptor
                        << '\n';
                }

                sessions.erase(client_descriptor);

                std::cout
                    << "Closed client FD: "
                    << client_descriptor
                    << '\n';
            }
        }
    }
    catch (const std::system_error& e)
    {
        std::cerr << "Server error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}
