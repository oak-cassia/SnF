#include "snf/net/socket_options.hpp"

#include "snf/net/system_error.hpp"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace snf::net
{
    void enable_tcp_no_delay(const int socket_descriptor)
    {
        constexpr int enabled = 1;

        if (::setsockopt(socket_descriptor, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled)) ==
            -1)
        {
            throw_system_error("setsockopt(TCP_NODELAY)");
        }
    }

    void set_socket_send_buffer_size(const int socket_descriptor, const int byte_count)
    {
        if (::setsockopt(
                socket_descriptor, SOL_SOCKET, SO_SNDBUF, &byte_count, sizeof(byte_count)) == -1)
        {
            throw_system_error("setsockopt(SO_SNDBUF)");
        }
    }
}
