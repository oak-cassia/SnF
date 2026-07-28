#pragma once

#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/command_ingress.hpp"
#include "snf/server/message_dispatcher.hpp"
#include "snf/server/runtime_types.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

namespace snf::server
{
    struct ActorRuntimeWorkerStats
    {
        std::uint64_t accepted{0};
        std::uint64_t processed{0};
        std::uint64_t rejected_full{0};
        std::size_t queue_depth{0};
        std::size_t queue_high_water_mark{0};
        std::chrono::nanoseconds average_queue_wait{0};
        std::chrono::nanoseconds max_queue_wait{0};
    };

    struct ActorRuntimeStats
    {
        std::vector<ActorRuntimeWorkerStats> workers;
    };

    struct ActorRuntimeConfig
    {
        std::size_t worker_count{2};
        std::size_t queue_capacity_per_worker{4096};
        std::function<MessageDispatcher(std::size_t)> message_dispatcher_factory;
        std::function<void()> on_worker_failure;
    };

    // A fixed shard executor. An actor always maps to one worker, so its commands
    // are serialized by that worker's FIFO queue. Different shards may run in
    // parallel and intentionally have no global response order.
    class ActorRuntime final : public CommandIngress
    {
    public:
        ActorRuntime(const ActorRuntimeConfig& config,
                     snf::runtime::BoundedQueue<NetworkAction>& network_actions,
                     int outbound_event_descriptor);
        ~ActorRuntime();

        ActorRuntime(const ActorRuntime&) = delete;
        ActorRuntime& operator=(const ActorRuntime&) = delete;

        void start();
        void join();

        [[nodiscard]] PostResult tryPost(InboundCommand command) override;
        void close() noexcept override;
        void cancel() noexcept override;

        [[nodiscard]] std::size_t workerIndexFor(ActorId actor) const noexcept;
        [[nodiscard]] std::size_t workerCount() const noexcept;
        [[nodiscard]] ActorRuntimeStats getStats() const;

    private:
        struct QueuedCommand
        {
            InboundCommand command;
            std::chrono::steady_clock::time_point enqueued_at;
        };

        struct WorkerCounters
        {
            std::atomic<std::uint64_t> accepted{0};
            std::atomic<std::uint64_t> processed{0};
            std::atomic<std::uint64_t> rejected_full{0};
            std::atomic<std::uint64_t> queue_wait_samples{0};
            std::atomic<std::uint64_t> total_queue_wait_nanoseconds{0};
            std::atomic<std::uint64_t> max_queue_wait_nanoseconds{0};
        };

        struct Worker
        {
            explicit Worker(std::size_t queue_capacity)
                : commands(queue_capacity)
            {
            }

            snf::runtime::BoundedQueue<QueuedCommand> commands;
            WorkerCounters counters;
            std::jthread thread;
        };

        enum class InputState
        {
            NotStarted,
            Running,
            Closed,
            Cancelled,
        };

        enum class CompletionState
        {
            Open,
            DrainRequested,
            Cancelled,
            DrainWon,
            Failed,
        };

        void runWorker(std::size_t worker_index);
        void workerFinished() noexcept;
        void recordWorkerFailure(std::exception_ptr error) noexcept;
        [[nodiscard]] bool publish(NetworkAction action) const;
        void signalNetwork() const noexcept;
        void joinWorkers() noexcept;
        static void updateMaximum(std::atomic<std::uint64_t>& target,
                                  std::uint64_t candidate) noexcept;

        const std::size_t _worker_count;
        std::vector<std::unique_ptr<Worker>> _workers;
        snf::runtime::BoundedQueue<NetworkAction>& _network_actions;
        int _outbound_event_descriptor;
        std::function<MessageDispatcher(std::size_t)> _message_dispatcher_factory;
        std::function<void()> _on_worker_failure;

        mutable std::mutex _state_mutex;
        InputState _input_state{InputState::NotStarted};
        bool _started{false};
        std::atomic<CompletionState> _completion_state{CompletionState::Open};
        std::atomic<std::size_t> _finished_workers{0};
        std::atomic<bool> _worker_failure_recorded{false};
        mutable std::mutex _error_mutex;
        std::exception_ptr _worker_error;
    };
}
