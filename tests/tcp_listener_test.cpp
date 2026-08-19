#include "snf/net/tcp_listener.hpp"

#include <cassert>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace
{
    void test_creates_a_non_blocking_listener()
    {
        const auto listener = snf::net::create_tcp_listener(0);
        const int listener_fd = listener.getDescriptor();

        assert(listener.isValid());

        const int file_status_flags = ::fcntl(listener_fd, F_GETFL);
        assert(file_status_flags != -1);
        assert((file_status_flags & O_NONBLOCK) != 0);

        int is_accepting_connections = 0;
        auto option_size = static_cast<socklen_t>(sizeof(is_accepting_connections));

        assert(::getsockopt(listener_fd, SOL_SOCKET, SO_ACCEPTCONN, &is_accepting_connections, &option_size) == 0);
        assert(is_accepting_connections != 0);

        sockaddr_in address{};
        auto address_size = static_cast<socklen_t>(sizeof(address));

        assert(::getsockname(listener_fd, reinterpret_cast<sockaddr*>(&address), &address_size) == 0);
        assert(address.sin_port != 0);
    }
} // namespace

void run_tcp_listener_tests()
{
    test_creates_a_non_blocking_listener();
}
