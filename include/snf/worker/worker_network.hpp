#pragma once

#include "snf/worker/budget.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/connection_table.hpp"
#include "snf/worker/inbox.hpp"
#include "snf/worker/poll_token.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace snf::worker
{
    struct WorkerNetworkConfig
    {
        ConnectionTableConfig table{};
        std::size_t poll_registration_capacity{1025};
        std::size_t max_accepts_per_poll{64};
        std::size_t receive_chunk_bytes{16ull * 1024};
    };

    struct WorkerNetworkMetrics
    {
        std::uint64_t accepted_connections{0};
        std::uint64_t closed_connections{0};
        std::uint64_t received_frames{0};
        std::uint64_t sent_frames{0};
        std::uint64_t protocol_errors{0};
        std::uint64_t rejected_requests{0};
        std::uint64_t stale_poll_events{0};
        std::uint64_t stale_work_items{0};
        std::uint64_t misrouted_events{0};
        std::uint64_t invariant_violations{0};
        std::uint64_t soft_limit_sends{0};
        std::uint64_t hard_limit_sends{0};
        std::uint64_t read_budget_stops{0};
        std::uint64_t write_budget_stops{0};
        std::uint64_t listener_pauses{0};
        std::uint64_t listener_resumes{0};
        std::uint64_t graceful_closes{0};
        std::uint64_t immediate_closes{0};
        std::uint64_t close_deadline_expirations{0};
    };

    [[nodiscard]] inline bool isValid(const WorkerBudgets& budgets) noexcept
    {
        return budgets.poll.max_poll_events > 0 && budgets.poll.max_accepts > 0 && budgets.poll.max_read_connections > 0 &&
               budgets.poll.max_frames > 0 && budgets.poll.max_read_bytes > 0 && budgets.poll.max_duration > std::chrono::nanoseconds::zero() &&
               budgets.inbox.max_events > 0 && budgets.inbox.max_per_lane > 0 && budgets.inbox.max_duration > std::chrono::nanoseconds::zero() &&
               budgets.timers.max_count > 0 && budgets.timers.max_duration > std::chrono::nanoseconds::zero() && budgets.actors.max_count > 0 &&
               budgets.actors.max_duration > std::chrono::nanoseconds::zero() && budgets.writes.max_bytes > 0 &&
               budgets.writes.max_duration > std::chrono::nanoseconds::zero() && budgets.max_poll_timeout >= std::chrono::milliseconds::zero();
    }

    [[nodiscard]] inline bool isValid(const WorkerNetworkConfig& config) noexcept
    {
        return config.table.capacity > 0 && config.table.capacity <= static_cast<std::size_t>(MAX_POLL_INDEX) + 1 &&
               config.table.limits.max_read_buffer_bytes > 0 && config.table.limits.write_soft_watermark_bytes > 0 &&
               config.table.limits.write_hard_limit_bytes >= config.table.limits.write_soft_watermark_bytes &&
               config.table.limits.close_drain_deadline >= std::chrono::milliseconds::zero() &&
               config.poll_registration_capacity > config.table.capacity &&
               config.poll_registration_capacity <= static_cast<std::size_t>(MAX_POLL_INDEX) + 1 && config.max_accepts_per_poll > 0 &&
               config.receive_chunk_bytes > 0;
    }

    struct WorkerGroupConfig
    {
        std::uint16_t worker_count{1};
        std::uint16_t max_workers{32};
        std::uint16_t port{7777};
        WorkerBudgets budgets{WorkerBudgets::defaults()};
        WorkerInboxConfig inbox{};
        WorkerNetworkConfig network{};
    };

    [[nodiscard]] inline bool isValid(const WorkerGroupConfig& config) noexcept
    {
        return config.worker_count > 0 && config.max_workers > 0 && config.worker_count <= config.max_workers && config.inbox.max_workers > 0 &&
               config.worker_count <= config.inbox.max_workers && config.inbox.max_bytes_per_worker > 0 && isValid(config.budgets) &&
               isValid(config.network);
    }
}
