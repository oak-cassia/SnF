#include "snf/net/termination_signal.hpp"
#include "snf/server/tcp_server.hpp"

#include <exception>
#include <iostream>

int main()
{
    try
    {
        const auto termination_signal = snf::net::create_termination_signal_listener();
        snf::server::TcpServer server{7777};
        std::cout << "Listening on port 7777\n";
        server.run(termination_signal.getDescriptor());
    }
    catch (const std::exception& error)
    {
        std::cerr << "Server error: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
