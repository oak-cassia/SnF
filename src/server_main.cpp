#include "snf/net/termination_signal.hpp"
#include "snf/server/game_server.hpp"

#include <exception>
#include <iostream>

int main()
{
    try
    {
        const auto termination_signal = snf::net::create_termination_signal_listener();
        snf::server::GameServer server{7777};
        std::cout << "Listening on port 7777\n";
        server.run(termination_signal.getDescriptor());

        const auto& stats = server.getStats();
        std::cout << "Server summary: " << stats.accepted_connections << " accepted, "
                  << stats.closed_connections << " closed, " << stats.received_frames
                  << " frames received, " << stats.sent_frames << " frames sent, "
                  << stats.protocol_errors << " protocol errors, "
                  << stats.game_queue_overflows << " game queue overflows, "
                  << stats.stale_network_actions << " stale network actions\n";
    }
    catch (const std::exception& error)
    {
        std::cerr << "Server error: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
