#include "snf/net/termination_signal.hpp"
#include "snf/server/game_server.hpp"
#include "snf/server/mysql_player_repository.hpp"

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>

namespace
{
    constexpr std::chrono::seconds METRICS_REPORT_INTERVAL{5};

    std::optional<std::string> environment(const char* const name)
    {
        const char* const value = std::getenv(name);
        return value == nullptr ? std::nullopt : std::optional<std::string>{value};
    }

    std::uint16_t mysql_port_from_environment()
    {
        const auto text = environment("SNF_MYSQL_PORT");
        if (!text)
        {
            return 3306;
        }
        std::uint32_t value = 0;
        const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), value);
        if (error != std::errc{} || end != text->data() + text->size() || value == 0 || value > std::numeric_limits<std::uint16_t>::max())
        {
            throw std::invalid_argument{"SNF_MYSQL_PORT is invalid"};
        }
        return static_cast<std::uint16_t>(value);
    }

    std::optional<snf::server::MySqlPlayerRepositoryConfig> mysql_from_environment()
    {
        const auto host = environment("SNF_MYSQL_HOST");
        if (!host)
        {
            return std::nullopt;
        }
        const auto user = environment("SNF_MYSQL_USER");
        if (!user || user->empty())
        {
            throw std::invalid_argument{"SNF_MYSQL_USER is required when MySQL is enabled"};
        }
        return snf::server::MySqlPlayerRepositoryConfig{
            .host = *host,
            .port = mysql_port_from_environment(),
            .user = *user,
            .password = environment("SNF_MYSQL_PASSWORD").value_or(""),
            .database = environment("SNF_MYSQL_DATABASE").value_or("snf"),
            .worker_count = 2,
            .queue_capacity = 4096,
            .connect_timeout = std::chrono::seconds{5},
            .read_timeout = std::chrono::seconds{5},
            .write_timeout = std::chrono::seconds{5},
        };
    }

    std::string format_distribution(const snf::runtime::DistributionSnapshot& distribution)
    {
        std::ostringstream formatted;
        formatted << "p50/p95/p99/max " << distribution.p50 << '/' << distribution.p95 << '/' << distribution.p99 << '/' << distribution.max << " ("
                  << distribution.sample_count << " samples)";
        return std::move(formatted).str();
    }

    void print_metrics(const snf::server::ServerMetricsSnapshot& metrics)
    {
        const auto& counters = metrics.counters;
        std::cout << "Counters: " << counters.accepted_connections << " accepted, " << counters.closed_connections << " closed, "
                  << counters.received_frames << " frames received, " << counters.sent_frames << " frames sent, " << counters.protocol_errors
                  << " protocol errors, " << counters.actor_queue_overflows << " actor queue overflows, " << counters.outbound_admission_failures
                  << " outbound admission failures, " << counters.outbound_admission_failure_fallbacks << " outbound admission failure fallbacks, "
                  << counters.stale_outbound_actions << " stale outbound actions, " << counters.connection_lifecycle_rejections
                  << " connection lifecycle rejections, " << counters.pending_connection_closes_high_water_mark
                  << " pending connection closes high-water\n";

        const auto& network = metrics.network;
        std::cout << "Reactor turn ns: " << format_distribution(network.reactor_turn_nanoseconds) << '\n'
                  << "Connections: " << network.session_count << " active, " << network.sessions_with_pending_send << " with pending send, "
                  << network.total_pending_send_bytes << " pending bytes\n"
                  << "Session pending send bytes: " << format_distribution(network.session_pending_send_bytes) << '\n'
                  << "Outbound queue depth: " << network.current_outbound_queue_depth << " now, " << network.outbound_queue_high_water_mark
                  << " high-water, " << format_distribution(network.outbound_queue_depth) << '\n'
                  << "Outbound reservations: " << network.reserved_outbound_slots << " slots reserved, " << network.pending_outbound_reservations
                  << " actors waiting, " << network.tracked_outbound_connections
                  << " connections tracked\n"
                  // Commit to consumption only. The wait for capacity is the actor's
                  // suspension, reported per Worker below, so the two together are what
                  // compares against the 3.9 baseline's single blocking figure.
                  << "Outbound hand-off wait ns: " << format_distribution(network.outbound_queue_wait_nanoseconds) << '\n'
                  << "Commands: " << metrics.command_terminals << " reached a result, " << metrics.command_admission_rejections
                  << " refused admission\n";

        const auto& zones = metrics.zone_actors;
        std::cout << "Zone command execution ns: " << format_distribution(zones.command_execution_nanoseconds) << '\n'
                  << "Zone tick execution ns: " << format_distribution(zones.tick_execution_nanoseconds) << ", " << zones.tick_overruns
                  << " budget overruns\n";

        const auto& rooms = metrics.room_actors;
        std::cout << "Room command execution ns: " << format_distribution(rooms.command_execution_nanoseconds) << '\n'
                  << "Room tick execution ns: " << format_distribution(rooms.tick_execution_nanoseconds) << '\n'
                  << "Room tick publish ns (payload + outbound enqueue; excludes reactor encode/TCP): "
                  << format_distribution(rooms.tick_publish_nanoseconds) << '\n'
                  << "Room tick turn ns: " << format_distribution(rooms.tick_turn_nanoseconds) << ", " << rooms.tick_overruns << " budget overruns, "
                  << rooms.tick_schedule_rejections << " schedule rejections\n"
                  << "Room oversized battle digests: " << metrics.room_protocol.oversized_battle_digests << '\n'
                  << "Room battle digest frames: " << metrics.room_protocol.battle_digest_frames << '\n'
                  << "Room battle digest fanout bytes (frame header + payload, before TCP/IP): " << metrics.room_protocol.battle_digest_fanout_bytes
                  << '\n';

        const auto& players = metrics.player_actors;
        std::cout << "Reward handoff: " << rooms.grant_tell_rejections << " grant tell rejections, " << players.reward_snapshot_admission_rejections
                  << " snapshot admission rejections, " << players.reward_snapshot_retry_giveups << " snapshot retry give-ups, "
                  << players.grant_load_failures << " grant load failures\n";

        const auto& parties = metrics.party_actors;
        std::cout << "Party actors: " << parties.commands << " commands, " << parties.rejected << " rejected, " << parties.passivation_requests
                  << " passivation requests\n";

        const auto& repository = metrics.player_repository;
        std::cout << "Player repository: " << repository.queue_depth << " queued, " << repository.queue_high_water_mark << " high-water, "
                  << repository.accepted << " accepted, " << repository.rejected << " rejected, " << repository.operation_failures
                  << " operation failures, latency ns " << format_distribution(repository.operation_latency_nanoseconds) << '\n';

        const auto& workers = metrics.actor_runtime.workers;
        for (std::size_t worker_index = 0; worker_index < workers.size(); ++worker_index)
        {
            const auto& worker = workers[worker_index];
            std::cout << "Actor worker " << worker_index << ": " << worker.accepted << " accepted, " << worker.processed << " processed, "
                      << worker.rejected_full << " rejected-full, depth " << worker.queue_depth << ", high-water " << worker.queue_high_water_mark
                      << ", actors " << worker.actor_count << ", ready " << worker.ready_actor_count << ", mailbox depth/high-water "
                      << worker.mailbox_depth << "/" << worker.mailbox_high_water_mark << ", budget yields " << worker.budget_yield_turns
                      << ", queue wait ns " << format_distribution(worker.queue_wait_nanoseconds) << '\n';
            std::cout << "Actor worker " << worker_index << " coroutine: " << worker.suspended_commands << " suspended commands, in-flight "
                      << worker.in_flight_operations << "/" << worker.in_flight_high_water_mark << ", suspended now " << worker.suspended_task_count
                      << ", continuations " << worker.continuation_queue_depth << ", passivatable " << worker.scheduler_passivatable_actor_count
                      << ", cancelled " << worker.cancelled_operations << ", reservation rejections " << worker.reservation_rejections
                      << ", double completions " << worker.double_completions << ", discarded late completions " << worker.discarded_late_completions
                      << ", suspend duration ns " << format_distribution(worker.suspend_duration_nanoseconds) << '\n';
            std::cout << "Actor worker " << worker_index << " timers: " << worker.active_timers << " active, " << worker.timers_scheduled
                      << " scheduled, " << worker.timers_rejected_full << " rejected-full, " << worker.timers_fired << " fired, "
                      << worker.timers_cancelled << " cancelled, " << worker.timers_discarded_stale << " discarded stale, lateness ns "
                      << format_distribution(worker.timer_lateness_nanoseconds) << '\n';
        }
    }
}

int main()
{
    try
    {
        const auto termination_signal = snf::net::create_termination_signal_listener();
        snf::server::GameServerConfig config;
        config.port = 7777;
        if (auto mysql = mysql_from_environment())
        {
            // Choosing the backend, and linking it, stays here rather than in the
            // server: GameServer only asks for a repository.
            config.player_repository_factory = [settings = *std::move(mysql)]
            {
                return std::make_unique<snf::server::MySqlPlayerRepository>(settings);
            };
        }
        config.metrics_report_interval = METRICS_REPORT_INTERVAL;
        // Runs on the reactor thread. Writing to a terminal is acceptable for
        // a development binary; a deployment that ships metrics elsewhere must
        // post to a bounded logger queue instead of doing the I/O here.
        config.metrics_reporter = [](const snf::server::ServerMetricsSnapshot& metrics)
        {
            std::cout << "--- metrics ---\n";
            print_metrics(metrics);
        };
        snf::server::GameServer server{config};

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
