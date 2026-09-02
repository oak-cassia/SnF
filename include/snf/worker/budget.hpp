#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace snf::worker
{
    struct CountTimeBudget
    {
        std::size_t max_count;
        std::chrono::nanoseconds max_duration;
    };

    struct ByteTimeBudget
    {
        std::uint64_t max_bytes;
        std::chrono::nanoseconds max_duration;
    };

    struct IoBudget
    {
        // These are independent upper bounds for one I/O phase. The phase
        // duration is shared by poll dispatch, accepts, and read work.
        std::size_t max_poll_events;
        std::size_t max_accepts;
        std::size_t max_read_connections;
        std::size_t max_frames;
        std::uint64_t max_read_bytes;
        std::chrono::nanoseconds max_duration;
    };

    struct InboxBudget
    {
        std::size_t max_events;
        std::size_t max_per_lane;
        std::chrono::nanoseconds max_duration;
    };

    // Streaming a result set can hand back row after row without ever waiting on the
    // socket, so one readiness event could otherwise run to the end of a large query
    // and starve every other phase. These are independent upper bounds for one DB
    // phase; whatever is left over resumes on the next loop iteration.
    struct DbProgressBudget
    {
        std::size_t max_steps;
        std::size_t max_rows;
        std::uint64_t max_bytes;
        std::chrono::nanoseconds max_duration;
    };

    // Stage 10 quality-gate constants. Budgets are checked between indivisible
    // items, so each phase gets one explicit single-item overshoot allowance.
    struct WorkerGateThresholds
    {
        static constexpr std::size_t DEBUG_MULTIPLIER = 8;
        static constexpr std::size_t ASAN_UBSAN_MULTIPLIER = 20;
        static constexpr std::size_t TSAN_MULTIPLIER = 40;
        static constexpr std::chrono::microseconds POLL_ITEM_ALLOWANCE{500};
        static constexpr std::chrono::microseconds INBOX_ITEM_ALLOWANCE{500};
        static constexpr std::chrono::microseconds TIMER_ITEM_ALLOWANCE{500};
        static constexpr std::chrono::microseconds DB_ITEM_ALLOWANCE{500};
        static constexpr std::chrono::milliseconds ACTOR_ITEM_ALLOWANCE{2};
        static constexpr std::chrono::microseconds WRITE_ITEM_ALLOWANCE{500};
    };

    // 시간 측정은 steady_clock::now()를 phase 시작에 한 번 읽고 이후 64개마다 다시 읽는다.
    // 항목마다 now()를 부르면 그 자체가 비용이 되기 때문이다.
    struct WorkerBudgets
    {
        IoBudget poll;
        InboxBudget inbox;
        CountTimeBudget timers;
        CountTimeBudget actors;
        ByteTimeBudget writes;
        DbProgressBudget db;
        std::chrono::milliseconds max_poll_timeout;

        [[nodiscard]] static constexpr WorkerBudgets defaults() noexcept
        {
            using namespace std::chrono_literals;
            return WorkerBudgets{
                .poll =
                    {
                        .max_poll_events = 1024,
                        .max_accepts = 64,
                        .max_read_connections = 1024,
                        .max_frames = 1024,
                        .max_read_bytes = 4ull * 1024 * 1024,
                        .max_duration = 500us,
                    },
                .inbox =
                    {
                        .max_events = 4096,
                        .max_per_lane = 8,
                        .max_duration = 250us,
                    },
                .timers =
                    {
                        .max_count = 2048,
                        .max_duration = 250us,
                    },
                .actors =
                    {
                        .max_count = 1024,
                        .max_duration = 1000us,
                    },
                .writes =
                    {
                        .max_bytes = 4ull * 1024 * 1024,
                        .max_duration = 500us,
                    },
                .db =
                    {
                        .max_steps = 64,
                        .max_rows = 1024,
                        .max_bytes = 1ull * 1024 * 1024,
                        .max_duration = 500us,
                    },
                .max_poll_timeout = 50ms,
            };
        }
    };
}
