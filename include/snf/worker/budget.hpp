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
        // PollWait is a blocking wait by design, so its wall time cannot separate
        // waiting from blocking and needs a fixed allowance on top of the poll
        // timeout. 400 ms is max_poll_timeout * 8, about three times the worst
        // late reschedule measured during calibration (a 124 ms ASan PollWait
        // sample). Active phases do NOT get this: there the environment witness
        // supplies a per-run measured allowance instead, which keeps a real
        // blocking wait inside an active phase detectable.
        static constexpr std::chrono::milliseconds SANITIZER_POLL_WAIT_SLACK{400};
    };

    // 시간 상한의 clock 읽기 빈도는 phase마다 다르다. poll/inbox/writes는 항목마다
    // steady_clock::now()를 읽고, TimerQueue::expire만 callback 64개마다 읽는다.
    // 후자에서는 분할 불가능한 단위가 항목 1개가 아니라 최대 64개다.
    struct WorkerBudgets
    {
        IoBudget poll;
        InboxBudget inbox;
        CountTimeBudget timers;
        CountTimeBudget actors;
        ByteTimeBudget writes;
        DbProgressBudget db;
        std::chrono::milliseconds max_poll_timeout;
        // Off by default. Sampling getrusage(RUSAGE_THREAD) on every phase
        // transition costs one syscall per transition, and only the Stage 10
        // quality gates need the per-phase CPU and context-switch attribution.
        // The packed progress word and the watchdog stay on either way: they read
        // steady_clock only, so live stall detection does not depend on this.
        bool sample_phase_execution{false};

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
