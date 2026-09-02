#pragma once

#include "snf/worker/barrier.hpp"
#include "snf/worker/worker.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <span>
#include <thread>
#include <vector>

namespace snf::worker
{
    struct WorkerGroupTestAccess;

    struct WorkerJoinOverrun
    {
        WorkerId worker{};
        WorkerPhase phase{WorkerPhase::Starting};
        std::chrono::nanoseconds stuck_for{0};
    };

    class WorkerGroup final
    {
    public:
        using RequestSinkFactory = std::function<std::unique_ptr<RequestSink>(WorkerId)>;
        using ActorFactoryFactory = std::function<std::unique_ptr<ActorFactory>(WorkerId)>;

        explicit WorkerGroup(
            const WorkerGroupConfig& config,
            RequestSinkFactory request_sink_factory = {},
            ActorFactoryFactory actor_factory_factory = {},
            WorkerWatchdog::StallHandler watchdog_handler = {}
        );
        ~WorkerGroup();

        WorkerGroup(const WorkerGroup&) = delete;
        WorkerGroup& operator=(const WorkerGroup&) = delete;

        // start() only starts already validated and fully wired resources.
        // If a thread cannot be started, the threads started so far are
        // stopped and joined before the exception is rethrown.
        void start();
        // Without an earlier requestStop(), join() requests stop itself and uses
        // that instant as the absolute group-deadline origin.
        void join();
        void run();
        void requestStop() noexcept;

        [[nodiscard]] std::uint16_t port() const noexcept;
        [[nodiscard]] std::uint16_t workerCount() const noexcept;
        // Inspection-only accessor. Worker owner-thread APIs are not exposed
        // through a mutable group reference.
        [[nodiscard]] const Worker& worker(std::size_t index) const noexcept;
        [[nodiscard]] const WorkerWatchdogMetrics* watchdogMetrics() const noexcept;
        [[nodiscard]] std::span<const WorkerJoinOverrun> joinOverruns() const noexcept;
        [[nodiscard]] bool isRunning() const noexcept;

    private:
        friend struct WorkerGroupTestAccess;

        void workerMain(std::size_t index) noexcept;
        void stopAndJoinStartedThreads() noexcept;
        void observeThreadExitDeadline() noexcept;
        [[nodiscard]] bool allStartedThreadsExited() const noexcept;
        [[nodiscard]] std::chrono::steady_clock::time_point stopRequestedAt() const noexcept;

        WorkerGroupConfig _config;
        WorkerQuiescenceBarrier _barrier;
        std::uint16_t _port{0};
        std::vector<std::unique_ptr<RequestSink>> _sinks;
        std::vector<std::unique_ptr<ActorFactory>> _actor_factories;
        std::vector<std::unique_ptr<Worker>> _workers;
        // Declared after _workers so the watchdog is destroyed before the
        // progress sources it observes.
        std::unique_ptr<WorkerWatchdog> _watchdog;
        std::vector<std::thread> _threads;
        std::unique_ptr<std::atomic<bool>[]> _thread_exited;
        std::atomic<std::int64_t> _stop_requested_at_ns{0};
        std::chrono::nanoseconds _group_shutdown_budget{0};
        std::vector<WorkerJoinOverrun> _join_overruns;
        mutable std::mutex _exit_mutex;
        std::condition_variable _exit_cv;
        mutable std::mutex _failure_mutex;
        std::exception_ptr _failure;
        bool _started{false};
        bool _started_once{false};
    };
}
