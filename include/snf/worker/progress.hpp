#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace snf::worker
{
    struct WorkerProgressTestAccess;

    enum class WorkerPhase : std::uint8_t
    {
        Starting = 0,
        PollWait,
        Poll,
        Inbox,
        Timers,
        Db,
        Actors,
        Writes,
        ShutdownA,
        ShutdownB,
        ShutdownC,
        ShutdownD,
        Stopped,
    };

    inline constexpr std::size_t WORKER_PHASE_COUNT = static_cast<std::size_t>(WorkerPhase::Stopped) + 1;

    class WorkerProgress final
    {
    public:
        using TimePoint = std::chrono::steady_clock::time_point;

        struct Sample
        {
            WorkerPhase phase{WorkerPhase::Starting};
            TimePoint entered_at{};
        };

        void publish(const WorkerPhase phase, const TimePoint entered_at) noexcept
        {
            const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(entered_at.time_since_epoch()).count();
            _word.store(pack(phase, static_cast<std::uint64_t>(micros)), std::memory_order_relaxed);
        }

        [[nodiscard]] Sample sample() const noexcept
        {
            const std::uint64_t word = _word.load(std::memory_order_relaxed);
            const auto phase = unpackPhase(word);
            const auto micros = static_cast<std::chrono::microseconds::rep>(unpackMicros(word));
            return Sample{.phase = phase, .entered_at = TimePoint{std::chrono::microseconds{micros}}};
        }

    private:
        friend struct WorkerProgressTestAccess;

        static constexpr std::uint64_t PHASE_SHIFT = 60;
        static constexpr std::uint64_t TIMESTAMP_MASK = (std::uint64_t{1} << PHASE_SHIFT) - 1;

        [[nodiscard]] static constexpr std::uint64_t pack(const WorkerPhase phase, const std::uint64_t epoch_micros) noexcept
        {
            return (static_cast<std::uint64_t>(phase) << PHASE_SHIFT) | (epoch_micros & TIMESTAMP_MASK);
        }

        [[nodiscard]] static constexpr WorkerPhase unpackPhase(const std::uint64_t word) noexcept
        {
            return static_cast<WorkerPhase>(word >> PHASE_SHIFT);
        }

        [[nodiscard]] static constexpr std::uint64_t unpackMicros(const std::uint64_t word) noexcept
        {
            return word & TIMESTAMP_MASK;
        }

        alignas(64) std::atomic<std::uint64_t> _word{0};
    };

    static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
    static_assert(static_cast<std::uint8_t>(WorkerPhase::Stopped) < 16);
}
