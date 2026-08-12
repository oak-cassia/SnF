#pragma once

#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/async_operation.hpp"
#include "snf/runtime/distribution.hpp"
#include "snf/runtime/post_result.hpp"
#include "snf/runtime/runtime_completion.hpp"

#include <atomic>
#include <chrono>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
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
        // Passivation request rather than a lifecycle fence. The scheduler evicts
        // only if this actor's mailbox is empty at the decision point; otherwise it
        // keeps the activation and processes the already accepted work. Unlike Evict,
        // this never discards a mailbox tail.
        PassivateIfIdle,
        Stopped,
        // The handler awaited an operation. The scheduler keeps the command's
        // capacity and turn accounting open until a terminal continuation or a
        // cancellation arrives, and leaves the actor out of the ready queue.
        Suspended,
    };

    enum class ActorExecutionState
    {
        Idle,
        Ready,
        Running,
        Suspended,
    };

    class ActorBinding;
    class ActorRuntime;

    // Handed to a binding's dispatch/resume. Its address is stable for the
    // lifetime of one actor activation: an awaiter that suspends still uses it
    // after dispatch has returned, so a per-dispatch stack object would dangle.
    class ActorContext
    {
    public:
        virtual ~ActorContext() = default;

        ActorContext(const ActorContext&) = delete;
        ActorContext& operator=(const ActorContext&) = delete;

        [[nodiscard]] virtual ActorKey key() const noexcept = 0;
        [[nodiscard]] virtual ActorIncarnation incarnation() const noexcept = 0;

        // Reserves an in-flight slot -- which is also the terminal continuation
        // slot -- allocates a TaskId, and registers the operation on the actor
        // slot so the owning Worker can cancel it later. std::nullopt means the
        // reservation failed and the operation must not be started at all.
        [[nodiscard]] virtual std::optional<ActorCompletionHandle>
        tryBeginOperation(std::shared_ptr<AsyncOperationControl> operation) = 0;

        // Undoes tryBeginOperation when submitting the operation threw. The
        // operation never started, so no completion can arrive for it.
        virtual void abortOperation() noexcept = 0;

    protected:
        ActorContext() = default;
    };

    // Suspends the actor until an external service completes the operation.
    //
    // The producer never resumes the coroutine; it only publishes. That is what
    // makes an immediate completion safe: the publish lands in the owning Worker's
    // continuation queue, and the suspension has already finished by the time that
    // Worker looks at the queue.
    template <typename T, typename Submit> class AsyncOperationAwaiter final
    {
    public:
        AsyncOperationAwaiter(ActorContext& context, Submit submit)
            : _context(context)
            , _submit(std::move(submit))
        {
        }

        [[nodiscard]] bool await_ready() const noexcept
        {
            return false;
        }

        [[nodiscard]] bool await_suspend(std::coroutine_handle<>)
        {
            _state = std::make_shared<AsyncOperationState<T>>();

            auto handle = _context.tryBeginOperation(_state);
            if (!handle)
            {
                _rejected = true;
                return false;
            }

            try
            {
                _submit(AsyncOperationProducer<T>{_state, std::move(*handle)});
            }
            catch (...)
            {
                _context.abortOperation();
                _submit_error = std::current_exception();
                return false;
            }

            return true;
        }

        [[nodiscard]] T await_resume()
        {
            if (_rejected)
            {
                throw AsyncOperationRejected{};
            }

            if (_submit_error)
            {
                std::rethrow_exception(_submit_error);
            }

            if (_state->outcome() == AsyncOperationOutcome::Cancelled)
            {
                throw AsyncOperationCancelled{};
            }

            return _state->takeResult();
        }

    private:
        ActorContext& _context;
        Submit _submit;
        std::shared_ptr<AsyncOperationState<T>> _state;
        std::exception_ptr _submit_error;
        bool _rejected{false};
    };

    // co_await awaitAsyncOperation<Result>(context, [&](auto producer) {
    //     service.submit(request, std::move(producer));
    // });
    template <typename T, typename Submit>
    [[nodiscard]] AsyncOperationAwaiter<T, std::decay_t<Submit>>
    awaitAsyncOperation(ActorContext& context, Submit&& submit)
    {
        return AsyncOperationAwaiter<T, std::decay_t<Submit>>{context,
                                                              std::forward<Submit>(submit)};
    }

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

        // Returning Suspended means the handler's task is parked on an operation
        // begun through the context. The binding keeps the task in its own slot
        // until resume() finishes it.
        [[nodiscard]] virtual ActorDispatchResult dispatch(ActorSlot& slot,
                                                           const ActorSubmission& submission,
                                                           ActorContext& context,
                                                           std::stop_token stop_token) = 0;

        // Called only after a previous dispatch/resume returned Suspended, and
        // only on the owning Worker. Returning Suspended again is allowed: a
        // handler may await more than once in sequence.
        [[nodiscard]] virtual ActorDispatchResult
        resume(ActorSlot& slot, ActorContext& context, std::stop_token stop_token) = 0;

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
        // Commands that suspended at least once, counted at their first
        // suspension rather than per suspension.
        std::uint64_t suspended_commands{0};
        // Operations refused before starting because the in-flight reservation
        // was exhausted.
        std::uint64_t reservation_rejections{0};
        // A completion that lost the claim to an already completed operation.
        std::uint64_t double_completions{0};
        // A completion that lost the claim to a cancellation, or whose
        // incarnation/task identity no longer matched the owning slot.
        std::uint64_t discarded_late_completions{0};
        // Operations the owning Worker cancelled before their producer claimed a
        // terminal outcome.
        std::uint64_t cancelled_operations{0};
        std::size_t suspended_task_count{0};
        std::size_t in_flight_operations{0};
        std::size_t in_flight_high_water_mark{0};
        std::size_t continuation_queue_depth{0};
        // Actors satisfying the scheduler-visible half of the passivation
        // condition. The runtime cannot see a binding's retained lifecycle
        // resources, so this is an observation and not a decision to passivate.
        std::size_t scheduler_passivatable_actor_count{0};
        // Nanoseconds from a command's acceptance to the start of its dispatch.
        // Control submissions are excluded, as they are in the counters above.
        // A resume is not a new acceptance, so it is not sampled again.
        DistributionSnapshot queue_wait_nanoseconds;
        // Nanoseconds from a task suspending to the owning Worker resuming it.
        DistributionSnapshot suspend_duration_nanoseconds;
    };

    struct ActorRuntimeStats
    {
        std::vector<ActorRuntimeWorkerStats> workers;
    };

    struct ActorRuntimeConfig
    {
        std::size_t worker_count{2};
        std::size_t queue_capacity_per_worker{4096};
        // Bounds suspended operations per Worker. It is also the continuation
        // queue capacity, so holding a reservation guarantees a free slot for the
        // terminal continuation and a publish can never be refused for capacity.
        std::size_t max_in_flight_operations_per_worker{1024};
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

        // Requests cancellation. It never touches a coroutine frame or a mailbox
        // itself: each owning Worker discards its own submissions, cancels its own
        // operations and destroys its own frames after being woken.
        void cancel() noexcept;

        // Requests cancellation of the operation an actor is currently suspended
        // on. Callable from any thread; the owning Worker performs the terminal
        // transition. This is the entry point a deadline primitive will use.
        void requestActorOperationCancel(const ActorKey& key) noexcept;

        [[nodiscard]] std::size_t workerIndexFor(const ActorKey& key) const noexcept;
        [[nodiscard]] std::size_t workerCount() const noexcept;
        [[nodiscard]] ActorRuntimeStats getStats() const;

    private:
        struct QueuedSubmission;
        struct ActiveCommand;
        struct ActorSlotEntry;
        struct WorkerCounters;
        struct Worker;
        class SlotContext;
        class WorkerContinuationEndpoint;

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
        // Replaces the old blocking ingress pop. A Worker now waits on its own
        // wake-up because it has two input sources -- ingress and continuations --
        // and a bounded queue can only be waited on one at a time.
        [[nodiscard]] bool pumpWorker(Worker& worker);
        void applyCancelRequests(Worker& worker);
        [[nodiscard]] std::size_t drainContinuations(Worker& worker);
        [[nodiscard]] bool routeToMailbox(Worker& worker, QueuedSubmission submission);
        [[nodiscard]] bool runReadyActorTurn(Worker& worker, std::size_t worker_index);
        [[nodiscard]] bool hasReadyActor(const Worker& worker) const;
        [[nodiscard]] bool isWorkerDrained(const Worker& worker) const;
        void cancelWorkerIngress(Worker& worker) noexcept;
        void discardWorkerSubmissions(Worker& worker) noexcept;
        void cancelSuspendedTasks(Worker& worker) noexcept;
        void destroyWorkerActors(Worker& worker) noexcept;
        [[nodiscard]] bool reserveOutstanding(Worker& worker) noexcept;
        void releaseOutstanding(Worker& worker, std::size_t count = 1) noexcept;
        [[nodiscard]] bool reserveOperation(Worker& worker) noexcept;
        void releaseOperation(Worker& worker) noexcept;
        // Applies the terminal half of a command's accounting exactly once,
        // whichever of success, failure or cancellation ended it.
        void finishActiveCommand(Worker& worker, ActorSlotEntry& slot, bool succeeded) noexcept;
        [[nodiscard]] bool publishContinuation(const ActorContinuation& continuation) noexcept;
        void reportRejectedCompletion(const ActorContinuation& continuation,
                                      ContinuationRejection rejection) noexcept;
        void workerFinished() noexcept;
        void recordWorkerFailure(std::exception_ptr error) noexcept;
        void joinWorkers() noexcept;
        static void updateMaximum(std::atomic<std::uint64_t>& target,
                                  std::uint64_t candidate) noexcept;

        const std::size_t _worker_count;
        const std::size_t _max_in_flight_operations;
        std::vector<std::unique_ptr<Worker>> _workers;
        // Kept alive by producers, so a completion that outlives the runtime finds
        // a deactivated endpoint instead of a dangling pointer.
        std::shared_ptr<WorkerContinuationEndpoint> _continuation_endpoint;
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
