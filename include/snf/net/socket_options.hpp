#pragma once

namespace snf::net
{
    void enable_tcp_no_delay(int socket_descriptor);
    void set_socket_send_buffer_size(int socket_descriptor, int byte_count);
}
