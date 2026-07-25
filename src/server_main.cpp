#include "snf/net/tcp_listener.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/net/system_error.hpp"

#include <iostream>
#include <system_error>
#include <sys/epoll.h>

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
    }
    catch (const std::system_error& e)
    {
        std::cerr << "Server error: " << e.what() << '\n';
        return 1;
    }

    return 0;
}
