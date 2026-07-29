#pragma once

#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/command_ingress.hpp"
#include "snf/server/player_actor.hpp"
#include "snf/server/player_effect_sink.hpp"
#include "snf/server/runtime_completion.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stop_token>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

namespace snf::server
{
    struct ActorRuntimeWorkerStats
    {
        std::uint64_t accepted{0};
        std::uint64_t processed{0};
        std::uint64_t rejected_full{0};
        std::uint64_t evicted_actors{0};
        // All accepted commands that have not completed or been discarded. This
        // includes ingress, per-actor mailboxes, and the currently executing turn.
        std::size_t queue_depth{0};
        std::size_t queue_high_water_mark{0};
        std::size_t actor_count{0};
        std::size_t ready_actor_count{0};
        std::size_t mailbox_depth{0};
        std::size_t mailbox_high_water_mark{0};
        std::uint64_t budget_yield_turns{0};
        std::chrono::nanoseconds average_queue_wait{0};
        std::chrono::nanoseconds max_queue_wait{0};
    };

    struct ActorRuntimeStats
    {
        std::vector<ActorRuntimeWorkerStats> workers;
    };

    struct ActorRuntimeConfig
    {
        explicit ActorRuntimeConfig(RuntimeId runtime_id_value) noexcept
            : runtime_id(runtime_id_value)
        {
        }

        RuntimeId runtime_id;
        std::size_t worker_count{2};
        std::size_t queue_capacity_per_worker{4096};
        // Diagnostic hooks run on the owning Worker. Production leaves them empty;
        // tests use them for deterministic scheduling and failure injection.
        std::function<void(std::size_t)> on_worker_start;
        std::function<void(std::size_t, ProvisionalActorId, const PlayerCommand&)>
            on_before_command;
        std::function<void()> on_worker_failure;
    };

    // A fixed-shard executor. Each worker first receives commands into an ingress
    // queue, then routes them to one FIFO mailbox per PlayerActor. A ready queue
    // schedules mailbox turns fairly while shards remain independent.
    class ActorRuntime final : public CommandIngress
    {
    public:
        ActorRuntime(const ActorRuntimeConfig& config,
                     PlayerEffectSink& player_effects,
                     RuntimeCompletionSink& runtime_completion);
        ~ActorRuntime();

        ActorRuntime(const ActorRuntime&) = delete;
        ActorRuntime& operator=(const ActorRuntime&) = delete;

        void start();
        void join();

        [[nodiscard]] PostResult tryPost(InboundCommand command) override;
        [[nodiscard]] PostResult tryPostConnectionClosed(ProvisionalActorId actor,
                                                         ConnectionClosed closed) override;
        void close() noexcept override;
        void cancel() noexcept override;

        [[nodiscard]] std::size_t workerIndexFor(ProvisionalActorId actor) const noexcept;
        [[nodiscard]] std::size_t workerCount() const noexcept;
        [[nodiscard]] ActorRuntimeStats getStats() const;

    private:
        struct ConnectionClosedEvent
        {
            ProvisionalActorId actor;
            ConnectionClosed closed;
        };

        struct QueuedEvent
        {
            std::variant<InboundCommand, ConnectionClosedEvent> payload;
            std::chrono::steady_clock::time_point enqueued_at;
        };

        enum class EventKind
        {
            Command,
            ConnectionClosed,
        };

        struct WorkerCounters
        {
            std::atomic<std::uint64_t> accepted{0};
            std::atomic<std::uint64_t> processed{0};
            std::atomic<std::uint64_t> rejected_full{0};
            std::atomic<std::uint64_t> evicted_actors{0};
            std::atomic<std::uint64_t> queue_wait_samples{0};
            std::atomic<std::uint64_t> total_queue_wait_nanoseconds{0};
            std::atomic<std::uint64_t> max_queue_wait_nanoseconds{0};
            std::atomic<std::uint64_t> outstanding_high_water_mark{0};
            std::atomic<std::uint64_t> mailbox_depth{0};
            std::atomic<std::uint64_t> mailbox_high_water_mark{0};
            std::atomic<std::uint64_t> budget_yield_turns{0};
        };

        struct ProvisionalActorIdHash
        {
            [[nodiscard]] std::size_t operator()(ProvisionalActorId actor) const noexcept
            {
                return std::hash<std::uint64_t>{}(actor.value);
            }
        };

        struct ActorSlot
        {
            PlayerActor actor;
            std::deque<QueuedEvent> mailbox;
            ActorExecutionState state{ActorExecutionState::Idle};
        };

        struct Worker
        {
            explicit Worker(std::size_t queue_capacity)
                : ingress(queue_capacity)
                , capacity(queue_capacity)
            {
            }

            snf::runtime::BoundedQueue<QueuedEvent> ingress;
            const std::size_t capacity;
            std::atomic<std::size_t> outstanding{0};
            WorkerCounters counters;
            mutable std::mutex scheduling_mutex;
            std::unordered_map<ProvisionalActorId, ActorSlot, ProvisionalActorIdHash> actors;
            std::deque<ProvisionalActorId> ready_actors;
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
        [[nodiscard]] bool fillIngress(Worker& worker);
        [[nodiscard]] bool routeToMailbox(Worker& worker, QueuedEvent event);
        [[nodiscard]] bool runReadyActorTurn(Worker& worker, std::size_t worker_index);
        [[nodiscard]] bool hasReadyActor(const Worker& worker) const;
        [[nodiscard]] PostResult postEvent(QueuedEvent event, EventKind kind);
        void discardWorkerCommands(Worker& worker) noexcept;
        [[nodiscard]] bool reserveOutstanding(Worker& worker) noexcept;
        void releaseOutstanding(Worker& worker, std::size_t count = 1) noexcept;
        void workerFinished() noexcept;
        void recordWorkerFailure(std::exception_ptr error) noexcept;
        void joinWorkers() noexcept;
        static void updateMaximum(std::atomic<std::uint64_t>& target,
                                  std::uint64_t candidate) noexcept;

        const std::size_t _worker_count;
        const RuntimeId _runtime_id;
        std::vector<std::unique_ptr<Worker>> _workers;
        PlayerEffectSink& _player_effects;
        RuntimeCompletionSink& _runtime_completion;
        std::function<void(std::size_t)> _on_worker_start;
        std::function<void(std::size_t, ProvisionalActorId, const PlayerCommand&)>
            _on_before_command;
        std::function<void()> _on_worker_failure;
        std::stop_source _effect_stop_source;

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
