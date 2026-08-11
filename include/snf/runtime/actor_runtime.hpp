#pragma once

#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/distribution.hpp"
#include "snf/runtime/post_result.hpp"
#include "snf/runtime/runtime_completion.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace snf::runtime
{
    enum class ActorActivation
    {
        ActivateIfMissing,
        ExistingOnly,
    };

    enum class ActorAccounting
    {
        Command,
        Control,
    };

    enum class ActorDispatchResult
    {
        KeepActive,
        Evict,
        Stopped,
    };

    // Suspended is reserved for future continuation work. The scheduler does
    // not transition an actor into it in Phase 3.8.
    enum class ActorExecutionState
    {
        Idle,
        Ready,
        Running,
        Suspended,
    };

    class ActorBinding;
    class ActorRuntime;

    // A move-only, type-erased binding submission. Its constructor is private:
    // only an ActorBinding factory can associate a typed payload with a target.
    class ActorSubmission final
    {
    public:
        ActorSubmission(const ActorSubmission&) = delete;
        ActorSubmission& operator=(const ActorSubmission&) = delete;
        ActorSubmission(ActorSubmission&&) noexcept = default;
        ActorSubmission& operator=(ActorSubmission&&) noexcept = default;

        [[nodiscard]] const ActorKey& target() const noexcept;
        [[nodiscard]] ActorActivation activation() const noexcept;
        [[nodiscard]] ActorAccounting accounting() const noexcept;

    private:
        class PayloadStorage
        {
        public:
            virtual ~PayloadStorage() = default;
        };

        template <typename Payload> class TypedPayload final : public PayloadStorage
        {
        public:
            template <typename Value>
            explicit TypedPayload(Value&& payload)
                : value(std::forward<Value>(payload))
            {
            }

            Payload value;
        };

        template <typename Payload>
        ActorSubmission(const ActorBinding* binding,
                        ActorKey target,
                        ActorActivation activation,
                        ActorAccounting accounting,
                        Payload&& payload)
            : _binding(binding)
            , _target(target)
            , _activation(activation)
            , _accounting(accounting)
            , _payload(std::make_unique<TypedPayload<std::decay_t<Payload>>>(
                  std::forward<Payload>(payload)))
        {
        }

        const ActorBinding* _binding;
        ActorKey _target;
        ActorActivation _activation;
        ActorAccounting _accounting;
        std::unique_ptr<PayloadStorage> _payload;

        friend class ActorBinding;
        friend class ActorRuntime;
    };

    // The scheduler only stores this wrapper type. Domain actors themselves do
    // not implement a common runtime base class.
    class ActorSlot
    {
    public:
        virtual ~ActorSlot() = default;

        ActorSlot(const ActorSlot&) = delete;
        ActorSlot& operator=(const ActorSlot&) = delete;

    protected:
        ActorSlot() = default;
    };

    class ActorBinding
    {
    public:
        virtual ~ActorBinding() = default;

        [[nodiscard]] virtual ActorKind kind() const noexcept = 0;

    protected:
        template <typename Payload>
        [[nodiscard]] ActorSubmission makeSubmission(const ActorKey target,
                                                     const ActorActivation activation,
                                                     const ActorAccounting accounting,
                                                     Payload&& payload) const
        {
            if (target.kind != kind())
            {
                throw std::invalid_argument{"ActorBinding factory received another actor kind"};
            }

            return ActorSubmission{
                this, target, activation, accounting, std::forward<Payload>(payload)};
        }

        template <typename Payload>
        [[nodiscard]] static const Payload& payloadAs(const ActorSubmission& submission)
        {
            const auto* payload = dynamic_cast<const ActorSubmission::TypedPayload<Payload>*>(
                submission._payload.get());
            if (payload == nullptr)
            {
                throw std::logic_error{"ActorBinding received an incompatible submission payload"};
            }

            return payload->value;
        }

        // Implementations create and use a local wrapper that owns their domain
        // actor. No Player/Zone/etc. runtime inheritance is required.
        [[nodiscard]] virtual std::unique_ptr<ActorSlot> activate(EntityId entity) = 0;
        [[nodiscard]] virtual ActorDispatchResult dispatch(ActorSlot& slot,
                                                           const ActorSubmission& submission,
                                                           std::stop_token stop_token) = 0;

        friend class ActorRuntime;
    };

    struct ActorRuntimeWorkerStats
    {
        std::uint64_t accepted{0};
        std::uint64_t processed{0};
        std::uint64_t rejected_full{0};
        std::uint64_t evicted_actors{0};
        // All accepted submissions that have not completed or been discarded:
        // ingress, mailboxes, and a currently executing dispatch.
        std::size_t queue_depth{0};
        std::size_t queue_high_water_mark{0};
        std::size_t actor_count{0};
        std::size_t ready_actor_count{0};
        std::size_t mailbox_depth{0};
        std::size_t mailbox_high_water_mark{0};
        std::uint64_t budget_yield_turns{0};
        // Nanoseconds from a command's acceptance to the start of its dispatch.
        // Control submissions are excluded, as they are in the counters above.
        DistributionSnapshot queue_wait_nanoseconds;
    };

    struct ActorRuntimeStats
    {
        std::vector<ActorRuntimeWorkerStats> workers;
    };

    struct ActorRuntimeConfig
    {
        std::size_t worker_count{2};
        std::size_t queue_capacity_per_worker{4096};
        // Diagnostic hooks run on the owning Worker. Production leaves them
        // empty; tests can use them for deterministic scheduling/failures.
        std::function<void(std::size_t)> on_worker_start;
        std::function<void(std::size_t, const ActorKey&, const ActorSubmission&)>
            on_before_dispatch;
        std::function<void()> on_worker_failure;
    };

    // Fixed-shard, actor-bound scheduler. Bindings must be registered before
    // start(), after which the registry is immutable for the runtime lifetime.
    class ActorRuntime final
    {
    public:
        ActorRuntime(const ActorRuntimeConfig& config, RuntimeCompletionSink& runtime_completion);
        ~ActorRuntime();

        ActorRuntime(const ActorRuntime&) = delete;
        ActorRuntime& operator=(const ActorRuntime&) = delete;

        void registerBinding(ActorBinding& binding);
        void start();
        void join();

        [[nodiscard]] PostResult tryPost(ActorSubmission submission);
        void close() noexcept;
        void cancel() noexcept;

        [[nodiscard]] std::size_t workerIndexFor(const ActorKey& key) const noexcept;
        [[nodiscard]] std::size_t workerCount() const noexcept;
        [[nodiscard]] ActorRuntimeStats getStats() const;

    private:
        struct QueuedSubmission;
        struct ActorSlotEntry;
        struct WorkerCounters;
        struct Worker;

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
        [[nodiscard]] bool routeToMailbox(Worker& worker, QueuedSubmission submission);
        [[nodiscard]] bool runReadyActorTurn(Worker& worker, std::size_t worker_index);
        [[nodiscard]] bool hasReadyActor(const Worker& worker) const;
        void discardWorkerSubmissions(Worker& worker) noexcept;
        void destroyWorkerActors(Worker& worker) noexcept;
        [[nodiscard]] bool reserveOutstanding(Worker& worker) noexcept;
        void releaseOutstanding(Worker& worker, std::size_t count = 1) noexcept;
        void workerFinished() noexcept;
        void recordWorkerFailure(std::exception_ptr error) noexcept;
        void joinWorkers() noexcept;
        static void updateMaximum(std::atomic<std::uint64_t>& target,
                                  std::uint64_t candidate) noexcept;

        const std::size_t _worker_count;
        std::vector<std::unique_ptr<Worker>> _workers;
        RuntimeCompletionSink& _runtime_completion;
        std::function<void(std::size_t)> _on_worker_start;
        std::function<void(std::size_t, const ActorKey&, const ActorSubmission&)>
            _on_before_dispatch;
        std::function<void()> _on_worker_failure;
        std::stop_source _dispatch_stop_source;

        mutable std::mutex _state_mutex;
        std::unordered_map<ActorKind, ActorBinding*> _bindings;
        InputState _input_state{InputState::NotStarted};
        bool _started{false};
        std::atomic<CompletionState> _completion_state{CompletionState::Open};
        std::atomic<std::size_t> _finished_workers{0};
        std::atomic<bool> _worker_failure_recorded{false};
        mutable std::mutex _error_mutex;
        std::exception_ptr _worker_error;
    };
}
