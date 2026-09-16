#include "snf/net/tcp_listener.hpp"
#include "snf/net/system_error.hpp"

#include <fcntl.h>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>

namespace snf::net
{
    UniqueFileDescriptor create_tcp_listener(const std::uint16_t port)
    {
        return create_tcp_listener(port, false);
    }

    UniqueFileDescriptor create_tcp_listener(const std::uint16_t port, const bool reuse_port)
    {
        const int listener_fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listener_fd == -1)
        {
            throw_system_error("socket");
        }

        UniqueFileDescriptor listener{listener_fd};

        constexpr int reuse_address = 1;

        if (::setsockopt(listener.getDescriptor(), SOL_SOCKET, SO_REUSEADDR, &reuse_address, sizeof(reuse_address)) == -1)
        {
            throw_system_error("setsockopt(SO_REUSEADDR)");
        }

        if (reuse_port)
        {
#ifdef SO_REUSEPORT
            constexpr int reuse_port_value = 1;
            if (::setsockopt(listener.getDescriptor(), SOL_SOCKET, SO_REUSEPORT, &reuse_port_value, sizeof(reuse_port_value)) == -1)
            {
                throw_system_error("setsockopt(SO_REUSEPORT)");
            }
#else
            throw std::invalid_argument{"SO_REUSEPORT is not available on this platform"};
#endif
        }

        const int file_status_flags = ::fcntl(listener.getDescriptor(), F_GETFL);

        if (file_status_flags == -1)
        {
            throw_system_error("fcntl(F_GETFL)");
        }

        if (::fcntl(listener.getDescriptor(), F_SETFL, file_status_flags | O_NONBLOCK) == -1)
        {
            throw_system_error("fcntl(F_SETFL)");
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_ANY);
        address.sin_port = htons(port);

        constexpr auto address_size = static_cast<socklen_t>(sizeof(address));

        if (::bind(listener.getDescriptor(), reinterpret_cast<const sockaddr*>(&address), address_size) == -1)
        {
            throw_system_error("bind");
        }

        if (::listen(listener.getDescriptor(), SOMAXCONN) == -1)
        {
            throw_system_error("listen");
        }

        return listener;
    }
}
