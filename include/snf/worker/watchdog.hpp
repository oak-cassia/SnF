#pragma once

#include "snf/worker/budget.hpp"
#include "snf/worker/identity.hpp"
#include "snf/worker/progress.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace snf::worker
{
    struct WorkerWatchdogConfig
    {
        std::chrono::milliseconds sample_interval{10};
        std::chrono::milliseconds phase_stall_threshold{500};
        // Zero derives max_poll_timeout * 4 + phase_stall_threshold.
        std::chrono::milliseconds poll_wait_stall_threshold{0};
    };

    [[nodiscard]] bool isValid(const WorkerWatchdogConfig& config) noexcept;

    struct WorkerStallReport
    {
        WorkerId worker{};
        WorkerPhase phase{WorkerPhase::Starting};
        std::chrono::nanoseconds stuck_for{0};
    };

    struct WorkerWatchdogMetrics
    {
        std::atomic<std::uint64_t> samples_taken{0};
        std::atomic<std::uint64_t> active_stall_episodes{0};
        std::atomic<std::uint64_t> shutdown_stall_episodes{0};
        std::atomic<std::uint64_t> longest_stall_ns{0};
        std::atomic<std::uint32_t> last_stall_phase{0};
    };

    struct WorkerWatchdogTarget
    {
        WorkerId worker{};
        const WorkerProgress* progress{nullptr};
    };

    class WorkerWatchdog final
    {
    public:
        using StallHandler = std::function<void(const WorkerStallReport&)>;

        WorkerWatchdog(WorkerWatchdogConfig config, WorkerBudgets budgets, std::vector<WorkerWatchdogTarget> targets, StallHandler handler = {});
        ~WorkerWatchdog();

        WorkerWatchdog(const WorkerWatchdog&) = delete;
        WorkerWatchdog& operator=(const WorkerWatchdog&) = delete;

        void start();
        void stop() noexcept;

        [[nodiscard]] bool running() const noexcept;
        [[nodiscard]] const WorkerWatchdogMetrics& metrics() const noexcept;
        [[nodiscard]] std::chrono::milliseconds pollWaitStallThreshold() const noexcept;
        void notifyHandler(const WorkerStallReport& report) const noexcept;

    private:
        void run() noexcept;
        void sampleOnce(std::chrono::steady_clock::time_point now) noexcept;

        WorkerWatchdogConfig _config;
        std::chrono::milliseconds _poll_wait_stall_threshold;
        std::vector<WorkerWatchdogTarget> _targets;
        StallHandler _handler;
        WorkerWatchdogMetrics _metrics;
        std::vector<bool> _stalled;
        std::atomic<bool> _stop_requested{false};
        std::atomic<bool> _running{false};
        std::mutex _wait_mutex;
        std::condition_variable _wait_cv;
        std::thread _thread;
    };
}
