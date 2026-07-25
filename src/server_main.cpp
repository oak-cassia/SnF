#include "snf/server/tcp_server.hpp"

#include <exception>
#include <iostream>

int main()
{
    try
    {
        snf::server::TcpServer server{7777};
        std::cout << "Listening on port 7777\n";
        server.run();
    }
    catch (const std::exception& error)
    {
        std::cerr << "Server error: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
