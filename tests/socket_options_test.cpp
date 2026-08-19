#include "snf/net/socket_options.hpp"
#include "snf/net/unique_file_descriptor.hpp"

#include <cassert>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

namespace
{
    void test_enables_tcp_no_delay()
    {
        const snf::net::UniqueFileDescriptor socket{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};

        assert(socket.isValid());

        snf::net::enable_tcp_no_delay(socket.getDescriptor());

        int option_value = 0;
        auto option_size = static_cast<socklen_t>(sizeof(option_value));

        assert(::getsockopt(socket.getDescriptor(), IPPROTO_TCP, TCP_NODELAY, &option_value, &option_size) == 0);
        assert(option_value != 0);
    }

    void test_sets_socket_send_buffer_size()
    {
        const snf::net::UniqueFileDescriptor socket{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
        assert(socket.isValid());

        constexpr int requested_byte_count = 4096;
        snf::net::set_socket_send_buffer_size(socket.getDescriptor(), requested_byte_count);

        int actual_byte_count = 0;
        auto option_size = static_cast<socklen_t>(sizeof(actual_byte_count));
        assert(::getsockopt(socket.getDescriptor(), SOL_SOCKET, SO_SNDBUF, &actual_byte_count, &option_size) == 0);
        assert(actual_byte_count >= requested_byte_count);
    }
}

void run_socket_options_tests()
{
    test_enables_tcp_no_delay();
    test_sets_socket_send_buffer_size();
}
