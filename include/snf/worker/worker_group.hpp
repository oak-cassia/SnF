#pragma once

#include "snf/worker/barrier.hpp"
#include "snf/worker/worker.hpp"

#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace snf::worker
{
    struct WorkerGroupTestAccess;

    class WorkerGroup final
    {
    public:
        using RequestSinkFactory = std::function<std::unique_ptr<RequestSink>(WorkerId)>;
        using ActorFactoryFactory = std::function<std::unique_ptr<ActorFactory>(WorkerId)>;

        explicit WorkerGroup(
            const WorkerGroupConfig& config,
            RequestSinkFactory request_sink_factory = {},
            ActorFactoryFactory actor_factory_factory = {}
        );
        ~WorkerGroup();

        WorkerGroup(const WorkerGroup&) = delete;
        WorkerGroup& operator=(const WorkerGroup&) = delete;

        // start() only starts already validated and fully wired resources.
        // If a thread cannot be started, the threads started so far are
        // stopped and joined before the exception is rethrown.
        void start();
        void join();
        void run();
        void requestStop() noexcept;

        [[nodiscard]] std::uint16_t port() const noexcept;
        [[nodiscard]] std::uint16_t workerCount() const noexcept;
        // Inspection-only accessor. Worker owner-thread APIs are not exposed
        // through a mutable group reference.
        [[nodiscard]] const Worker& worker(std::size_t index) const noexcept;
        [[nodiscard]] bool isRunning() const noexcept;

    private:
        friend struct WorkerGroupTestAccess;

        void workerMain(std::size_t index) noexcept;
        void stopAndJoinStartedThreads() noexcept;

        WorkerGroupConfig _config;
        WorkerQuiescenceBarrier _barrier;
        std::uint16_t _port{0};
        std::vector<std::unique_ptr<RequestSink>> _sinks;
        std::vector<std::unique_ptr<ActorFactory>> _actor_factories;
        std::vector<std::unique_ptr<Worker>> _workers;
        std::vector<std::thread> _threads;
        mutable std::mutex _failure_mutex;
        std::exception_ptr _failure;
        bool _started{false};
        bool _started_once{false};
    };
}
