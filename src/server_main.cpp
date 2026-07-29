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
                  << stats.protocol_errors << " protocol errors, " << stats.actor_queue_overflows
                  << " actor queue overflows, " << stats.stale_outbound_actions
                  << " stale outbound actions, " << stats.connection_lifecycle_rejections
                  << " connection lifecycle rejections, "
                  << stats.pending_connection_closes_high_water_mark
                  << " pending connection closes high-water\n";

        const auto actor_runtime_stats = server.getActorRuntimeStats();
        for (std::size_t worker_index = 0; worker_index < actor_runtime_stats.workers.size();
             ++worker_index)
        {
            const auto& worker = actor_runtime_stats.workers[worker_index];
            std::cout << "Actor worker " << worker_index << ": " << worker.accepted << " accepted, "
                      << worker.processed << " processed, " << worker.rejected_full
                      << " rejected-full, depth " << worker.queue_depth << ", high-water "
                      << worker.queue_high_water_mark << ", actors " << worker.actor_count
                      << ", ready " << worker.ready_actor_count << ", mailbox depth/high-water "
                      << worker.mailbox_depth << "/" << worker.mailbox_high_water_mark
                      << ", budget yields " << worker.budget_yield_turns << ", queue wait avg/max "
                      << worker.average_queue_wait.count() << "/" << worker.max_queue_wait.count()
                      << " ns\n";
        }
    }
    catch (const std::exception& error)
    {
        std::cerr << "Server error: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
