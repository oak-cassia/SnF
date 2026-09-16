#pragma once

#include "snf/worker/worker.hpp"

#include <array>
#include <cstddef>
#include <ostream>
#include <string_view>

namespace snf::test
{
    inline void printWorkerReport(std::ostream& out, const snf::worker::WorkerMetrics& worker, const snf::worker::DbClientMetrics& db)
    {
        constexpr std::array<std::string_view, snf::worker::WORKER_PHASE_COUNT> PHASE_NAMES{
            "Starting",
            "PollWait",
            "Poll",
            "Inbox",
            "Timers",
            "Db",
            "Actors",
            "Writes",
            "ShutdownA",
            "ShutdownB",
            "ShutdownC",
            "ShutdownD",
            "Stopped",
        };
        const auto loop = worker.loop_iteration_ns.snapshot();
        const auto turns = worker.actor.turn_slice_ns.snapshot();
        const auto operations = db.operation_latency_ns.snapshot();
        const auto queue_wait = db.queue_wait_ns.snapshot();

        out << "worker_report.loop_iterations=" << worker.loop_iterations << '\n';
        out << "worker_report.thread_execution_sample_failures=" << worker.thread_execution_sample_failures << '\n';
        out << "worker_report.loop_ns=count:" << loop.count << ",max:" << loop.max << ",p50:" << loop.p50 << ",p95:" << loop.p95
            << ",p99:" << loop.p99 << '\n';
        out << "worker_report.actor_turn_ns=count:" << turns.count << ",max:" << turns.max << ",p50:" << turns.p50 << ",p95:" << turns.p95
            << ",p99:" << turns.p99 << '\n';
        out << "worker_report.db_operation_ns=count:" << operations.count << ",max:" << operations.max << ",p50:" << operations.p50
            << ",p95:" << operations.p95 << ",p99:" << operations.p99 << '\n';
        out << "worker_report.db_queue_wait_ns=count:" << queue_wait.count << ",max:" << queue_wait.max << ",p50:" << queue_wait.p50
            << ",p95:" << queue_wait.p95 << ",p99:" << queue_wait.p99 << '\n';
        out << "worker_report.network=accepted:" << worker.network.accepted_connections << ",closed:" << worker.network.closed_connections
            << ",received:" << worker.network.received_frames << ",sent:" << worker.network.sent_frames
            << ",protocol_errors:" << worker.network.protocol_errors << ",invariant_violations:" << worker.network.invariant_violations
            << ",epollout_waits:" << worker.network.epollout_waits << ",hard_limit_sends:" << worker.network.hard_limit_sends << '\n';
        out << "worker_report.db=completed:" << db.operations_completed << ",failed:" << db.operations_failed
            << ",queued_timeouts:" << db.queued_timeouts << ",in_flight_timeouts:" << db.in_flight_timeouts
            << ",submit_rejections:" << db.submit_rejections << '\n';
        out << "worker_report.hwm_exact=read_buffer:" << worker.high_water_marks.connection_read_buffer_bytes
            << ",write_queue:" << worker.high_water_marks.connection_write_queued_bytes
            << ",db_queued_ops:" << worker.high_water_marks.db_queued_operations << ",db_queued_bytes:" << worker.high_water_marks.db_queued_bytes
            << ",db_in_flight:" << worker.high_water_marks.db_in_flight << '\n';
        out << "worker_report.hwm_sampled=connections:" << worker.high_water_marks.sampled_connections
            << ",actors:" << worker.high_water_marks.sampled_actors << ",loading:" << worker.high_water_marks.sampled_loading
            << ",ready:" << worker.high_water_marks.sampled_ready_actors
            << ",mailbox_messages:" << worker.high_water_marks.sampled_mailbox_messages_total
            << ",mailbox_bytes:" << worker.high_water_marks.sampled_mailbox_bytes_total << ",timers:" << worker.high_water_marks.sampled_timer_entries
            << ",timer_bytes:" << worker.high_water_marks.sampled_application_timer_bytes
            << ",inbox_bytes:" << worker.high_water_marks.sampled_inbox_queued_bytes << '\n';

        for (std::size_t index = 0; index < worker.phases.size(); ++index)
        {
            const auto& phase = worker.phases[index];
            out << "worker_report.phase." << PHASE_NAMES[index] << "=entries:" << phase.entries << ",budget_stops:" << phase.budget_stops
                << ",max_wall_residence_ns:" << phase.max_residence.count() << ",max_cpu_residence_ns:" << phase.max_cpu_residence.count()
                << ",max_entry_gap_ns:" << phase.max_entry_gap.count() << ",voluntary_context_switches:" << phase.voluntary_context_switches
                << ",involuntary_context_switches:" << phase.involuntary_context_switches
                << ",wall_max_cpu_ns:" << phase.max_wall_residence_cpu.count()
                << ",wall_max_voluntary_context_switches:" << phase.max_wall_residence_voluntary_context_switches
                << ",wall_max_involuntary_context_switches:" << phase.max_wall_residence_involuntary_context_switches << '\n';
        }

        const auto& shutdown = worker.shutdown;
        out << "worker_report.shutdown=actor_timeout_ns:" << shutdown.actor_configured_timeout.count()
            << ",actor_duration_ns:" << shutdown.actor_phases_duration.count() << ",actor_deadline_exceeded:" << shutdown.actor_deadline_exceeded
            << ",db_timeout_ns:" << shutdown.db_configured_timeout.count() << ",db_duration_ns:" << shutdown.db_shutdown_duration.count()
            << ",db_deadline_exceeded:" << shutdown.db_deadline_exceeded << ",total_ns:" << shutdown.total_duration.count() << '\n';
        const auto print_phase = [&out](const std::string_view name, const snf::worker::WorkerShutdownPhaseRecord& phase)
        {
            out << "worker_report.shutdown." << name << "=duration_ns:" << phase.duration.count() << ",deadline_hit:" << phase.deadline_hit
                << ",connections:" << phase.remaining.connections << ",actors:" << phase.remaining.actors
                << ",blocked:" << phase.remaining.blocked_actors << ",loading:" << phase.remaining.loading
                << ",inbox_bytes:" << phase.remaining.inbox_bytes << ",timers:" << phase.remaining.timer_entries
                << ",application_timer_bytes:" << phase.remaining.application_timer_bytes << ",db_in_flight:" << phase.remaining.db_in_flight
                << ",db_queued:" << phase.remaining.db_queued << '\n';
        };
        print_phase("A", shutdown.phase_a);
        print_phase("B", shutdown.phase_b);
        print_phase("C", shutdown.phase_c);
        print_phase("D", shutdown.phase_d);
        out << "worker_report.shutdown.final=db_in_flight:" << shutdown.final_resources.db_in_flight
            << ",db_queued:" << shutdown.final_resources.db_queued << ",forced_connections:" << shutdown.forced_connection_closes
            << ",forced_actors:" << shutdown.forced_actor_removals << ",forced_ready:" << shutdown.forced_ready_queue_drops << '\n';
    }
}
