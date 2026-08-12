#include "snf/net/termination_signal.hpp"
#include "snf/server/game_server.hpp"

#include <chrono>
#include <cstddef>
#include <exception>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

namespace
{
    constexpr std::chrono::seconds METRICS_REPORT_INTERVAL{5};

    std::string format_distribution(const snf::runtime::DistributionSnapshot& distribution)
    {
        std::ostringstream formatted;
        formatted << "p50/p95/p99/max " << distribution.p50 << '/' << distribution.p95 << '/'
                  << distribution.p99 << '/' << distribution.max << " ("
                  << distribution.sample_count << " samples)";
        return std::move(formatted).str();
    }

    void print_metrics(const snf::server::ServerMetricsSnapshot& metrics)
    {
        const auto& counters = metrics.counters;
        std::cout << "Counters: " << counters.accepted_connections << " accepted, "
                  << counters.closed_connections << " closed, " << counters.received_frames
                  << " frames received, " << counters.sent_frames << " frames sent, "
                  << counters.protocol_errors << " protocol errors, "
                  << counters.actor_queue_overflows << " actor queue overflows, "
                  << counters.outbound_admission_failures << " outbound admission failures, "
                  << counters.outbound_admission_failure_fallbacks
                  << " outbound admission failure fallbacks, " << counters.stale_outbound_actions
                  << " stale outbound actions, " << counters.connection_lifecycle_rejections
                  << " connection lifecycle rejections, "
                  << counters.pending_connection_closes_high_water_mark
                  << " pending connection closes high-water\n";

        const auto& network = metrics.network;
        std::cout << "Reactor turn ns: " << format_distribution(network.reactor_turn_nanoseconds)
                  << '\n'
                  << "Connections: " << network.session_count << " active, "
                  << network.sessions_with_pending_send << " with pending send, "
                  << network.total_pending_send_bytes << " pending bytes\n"
                  << "Session pending send bytes: "
                  << format_distribution(network.session_pending_send_bytes) << '\n'
                  << "Outbound queue depth: " << network.current_outbound_queue_depth << " now, "
                  << network.outbound_queue_high_water_mark << " high-water, "
                  << format_distribution(network.outbound_queue_depth) << '\n'
                  << "Outbound reservations: " << network.reserved_outbound_slots
                  << " slots reserved, " << network.pending_outbound_reservations
                  << " actors waiting, " << network.tracked_outbound_connections
                  << " connections tracked\n"
                  // Commit to consumption only. The wait for capacity is the actor's
                  // suspension, reported per Worker below, so the two together are what
                  // compares against the 3.9 baseline's single blocking figure.
                  << "Outbound hand-off wait ns: "
                  << format_distribution(network.outbound_queue_wait_nanoseconds) << '\n'
                  << "Commands: " << metrics.command_terminals << " reached a result, "
                  << metrics.command_admission_rejections << " refused admission\n";

        const auto& timers = metrics.zone_timers;
        std::cout << "Zone timers: " << timers.active_timers << " active, "
                  << timers.pending_cancellations << " cancelling, " << timers.scheduled
                  << " scheduled, " << timers.cancelled << " cancelled, " << timers.fired
                  << " ticks posted, " << timers.dropped_full << " dropped-full, "
                  << timers.skipped_intervals << " intervals skipped, " << timers.failures
                  << " failures\n";

        const auto& zones = metrics.zone_actors;
        std::cout << "Zone command execution ns: "
                  << format_distribution(zones.command_execution_nanoseconds) << '\n'
                  << "Zone tick execution ns: "
                  << format_distribution(zones.tick_execution_nanoseconds) << ", "
                  << zones.tick_overruns << " budget overruns\n";

        const auto& workers = metrics.actor_runtime.workers;
        for (std::size_t worker_index = 0; worker_index < workers.size(); ++worker_index)
        {
            const auto& worker = workers[worker_index];
            std::cout << "Actor worker " << worker_index << ": " << worker.accepted << " accepted, "
                      << worker.processed << " processed, " << worker.rejected_full
                      << " rejected-full, depth " << worker.queue_depth << ", high-water "
                      << worker.queue_high_water_mark << ", actors " << worker.actor_count
                      << ", ready " << worker.ready_actor_count << ", mailbox depth/high-water "
                      << worker.mailbox_depth << "/" << worker.mailbox_high_water_mark
                      << ", budget yields " << worker.budget_yield_turns << ", queue wait ns "
                      << format_distribution(worker.queue_wait_nanoseconds) << '\n';
            std::cout << "Actor worker " << worker_index
                      << " coroutine: " << worker.suspended_commands
                      << " suspended commands, in-flight " << worker.in_flight_operations << "/"
                      << worker.in_flight_high_water_mark << ", suspended now "
                      << worker.suspended_task_count << ", continuations "
                      << worker.continuation_queue_depth << ", passivatable "
                      << worker.scheduler_passivatable_actor_count << ", cancelled "
                      << worker.cancelled_operations << ", reservation rejections "
                      << worker.reservation_rejections << ", double completions "
                      << worker.double_completions << ", discarded late completions "
                      << worker.discarded_late_completions << ", suspend duration ns "
                      << format_distribution(worker.suspend_duration_nanoseconds) << '\n';
        }
    }
}

int main()
{
    try
    {
        const auto termination_signal = snf::net::create_termination_signal_listener();
        snf::server::GameServer server{snf::server::GameServerConfig{
            .port = 7777,
            .metrics_report_interval = METRICS_REPORT_INTERVAL,
            // Runs on the reactor thread. Writing to a terminal is acceptable for
            // a development binary; a deployment that ships metrics elsewhere must
            // post to a bounded logger queue instead of doing the I/O here.
            .metrics_reporter =
                [](const snf::server::ServerMetricsSnapshot& metrics)
            {
                std::cout << "--- metrics ---\n";
                print_metrics(metrics);
            },
        }};

        std::cout << "Listening on port 7777\n";
        server.run(termination_signal.getDescriptor());

        std::cout << "--- server summary ---\n";
        print_metrics(server.getMetricsSnapshot());
    }
    catch (const std::exception& error)
    {
        std::cerr << "Server error: " << error.what() << '\n';
        return 1;
    }

    return 0;
}
