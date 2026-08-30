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
        std::size_t max_events;
        std::size_t max_frames;
        std::uint64_t max_bytes;
        std::chrono::nanoseconds max_duration;
    };

    struct InboxBudget
    {
        std::size_t max_events;
        std::size_t max_per_lane;
        std::chrono::nanoseconds max_duration;
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
        std::chrono::milliseconds max_poll_timeout;

        [[nodiscard]] static constexpr WorkerBudgets defaults() noexcept
        {
            using namespace std::chrono_literals;
            return WorkerBudgets{
                .poll =
                    {
                        .max_events = 1024,
                        .max_frames = 1024,
                        .max_bytes = 4ull * 1024 * 1024,
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
                .max_poll_timeout = 50ms,
            };
        }
    };
}
