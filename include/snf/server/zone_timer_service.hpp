#pragma once

#include "snf/runtime/post_result.hpp"
#include "snf/server/timer_id.hpp"
#include "snf/server/zone_command.hpp"
#include "snf/server/zone_id.hpp"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <optional>
#include <stop_token>
#include <thread>
#include <unordered_map>

namespace snf::server
{
    using TimerTimePoint = std::chrono::steady_clock::time_point;

    class TimerClock
    {
    public:
        virtual ~TimerClock() = default;
        [[nodiscard]] virtual TimerTimePoint now() const noexcept = 0;
    };

    class SteadyTimerClock final : public TimerClock
    {
    public:
        [[nodiscard]] TimerTimePoint now() const noexcept override;
    };

    class ZoneTimerSink
    {
    public:
        virtual ~ZoneTimerSink() = default;
        [[nodiscard]] virtual snf::runtime::PostResult tryPostTimerCommand(ZoneId zone,
                                                                           ZoneCommand command) = 0;
    };

    struct ZoneTimerServiceConfig
    {
        std::chrono::milliseconds tick_interval{50};
        std::chrono::milliseconds cancellation_retry_interval{1};
        std::size_t max_timers{4096};
        std::function<void()> on_failure{};
    };

    struct ZoneTimerRegistration
    {
        snf::runtime::PostResult result{snf::runtime::PostResult::Closed};
        std::optional<TimerId> timer;
        bool created{false};
    };

    struct ZoneTimerServiceStats
    {
        std::uint64_t scheduled{0};
        std::uint64_t cancelled{0};
        std::uint64_t fired{0};
        std::uint64_t dropped_full{0};
        std::uint64_t skipped_intervals{0};
        std::uint64_t cancellation_retries{0};
        std::uint64_t failures{0};
        std::size_t active_timers{0};
        std::size_t pending_cancellations{0};
    };

    // One periodic simulation timer per Zone. The timer thread never touches Zone
    // state; it only posts typed commands to the same bounded Actor ingress used by
    // gameplay. Tests can inject a clock and call dispatchDue() without starting a
    // real thread.
    class ZoneTimerService final
    {
    public:
        ZoneTimerService(ZoneTimerSink& sink,
                         TimerClock& clock,
                         ZoneTimerServiceConfig config = {});
        ~ZoneTimerService();

        ZoneTimerService(const ZoneTimerService&) = delete;
        ZoneTimerService& operator=(const ZoneTimerService&) = delete;

        void start();
        void stop();

        [[nodiscard]] ZoneTimerRegistration tryEnsureTimer(ZoneId zone);
        [[nodiscard]] snf::runtime::PostResult tryCancelTimer(ZoneId zone);

        // Executes at most one tick per due Zone. Missed periods become a metric,
        // never an unbounded catch-up loop.
        void dispatchDue();

        [[nodiscard]] ZoneTimerServiceStats stats() const;
        void rethrowIfFailed() const;

    private:
        enum class TimerState
        {
            Active,
            CancelPending,
        };

        struct TimerRecord
        {
            TimerId id;
            TimerState state{TimerState::Active};
            TimerTimePoint next_deadline{};
            std::uint64_t tick{0};
        };

        using TimerMap = std::unordered_map<ZoneId, TimerRecord, ZoneIdHash>;

        void run(std::stop_token stop_token) noexcept;
        void dispatchDueLocked(TimerTimePoint now);
        [[nodiscard]] snf::runtime::PostResult
        tryPublishCancellationLocked(TimerMap::iterator timer, bool retry);
        [[nodiscard]] std::optional<TimerTimePoint> nextWakeLocked(TimerTimePoint now) const;
        [[nodiscard]] bool recordFailureLocked(std::exception_ptr error) noexcept;
        void notifyFailure() const noexcept;
        void closeLocked() noexcept;

        ZoneTimerSink& _sink;
        TimerClock& _clock;
        const std::chrono::milliseconds _tick_interval;
        const std::chrono::milliseconds _cancellation_retry_interval;
        const std::size_t _max_timers;
        std::function<void()> _on_failure;

        mutable std::mutex _mutex;
        std::condition_variable _wake;
        TimerMap _timers;
        std::jthread _thread;
        std::uint64_t _next_timer_id{0};
        std::uint64_t _revision{0};
        bool _started{false};
        bool _closed{false};
        bool _failure_notified{false};
        std::exception_ptr _error;

        std::uint64_t _scheduled{0};
        std::uint64_t _cancelled{0};
        std::uint64_t _fired{0};
        std::uint64_t _dropped_full{0};
        std::uint64_t _skipped_intervals{0};
        std::uint64_t _cancellation_retries{0};
        std::uint64_t _failures{0};
    };
}
