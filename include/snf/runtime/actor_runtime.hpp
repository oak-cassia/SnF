#pragma once

#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/async_operation.hpp"
#include "snf/runtime/distribution.hpp"
#include "snf/runtime/post_result.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/runtime/tell_payload.hpp"

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
        PassivateIfIdle,
        Stopped,
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
    class ActorSubmission;

    struct TimerHandle
    {
        std::uint64_t id{0};

        [[nodiscard]] bool operator==(const TimerHandle&) const noexcept = default;
    };

    class ActorContext
    {
    public:
        virtual ~ActorContext() = default;

        ActorContext(const ActorContext&) = delete;
        ActorContext& operator=(const ActorContext&) = delete;

        [[nodiscard]] virtual ActorKey key() const noexcept = 0;
        [[nodiscard]] virtual ActorIncarnation incarnation() const noexcept = 0;

        [[nodiscard]] virtual std::chrono::steady_clock::time_point observedAt() const noexcept = 0;

        [[nodiscard]] virtual std::optional<ActorCompletionHandle> tryBeginOperation(std::shared_ptr<AsyncOperationControl> operation) = 0;

        virtual void abortOperation() noexcept = 0;

        [[nodiscard]] virtual PostResult tryTell(ActorKey target, TellPayload payload) = 0;

        // 소유 Worker의 dispatch/resume 안에서만 호출 가능하다.
        // nullopt = 용량 거부. 호출자는 타이머가 걸리지 않았음을 알고 대응해야 한다.
        [[nodiscard]] virtual std::optional<TimerHandle> trySchedule(std::chrono::milliseconds delay, ActorSubmission submission) = 0;

        virtual void cancelTimer(TimerHandle handle) noexcept = 0;

    protected:
        ActorContext() = default;
    };

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

    template <typename T, typename Submit>
    [[nodiscard]] AsyncOperationAwaiter<T, std::decay_t<Submit>> awaitAsyncOperation(ActorContext& context, Submit&& submit)
    {
        return AsyncOperationAwaiter<T, std::decay_t<Submit>>{context, std::forward<Submit>(submit)};
    }

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
        ActorSubmission(const ActorBinding* binding, ActorKey target, ActorActivation activation, ActorAccounting accounting, Payload&& payload)
            : _binding(binding)
            , _target(target)
            , _activation(activation)
            , _accounting(accounting)
            , _payload(std::make_unique<TypedPayload<std::decay_t<Payload>>>(std::forward<Payload>(payload)))
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

    class ActorState
    {
    public:
        virtual ~ActorState() = default;

        ActorState(const ActorState&) = delete;
        ActorState& operator=(const ActorState&) = delete;

    protected:
        ActorState() = default;
    };

    class ActorBinding
    {
    public:
        virtual ~ActorBinding() = default;

        [[nodiscard]] virtual ActorKind kind() const noexcept = 0;

    protected:
        template <typename Payload>
        [[nodiscard]] ActorSubmission
        makeSubmission(const ActorKey target, const ActorActivation activation, const ActorAccounting accounting, Payload&& payload) const
        {
            if (target.kind != kind())
            {
                throw std::invalid_argument{"ActorBinding factory received another actor kind"};
            }

            return ActorSubmission{this, target, activation, accounting, std::forward<Payload>(payload)};
        }

        template <typename Payload> [[nodiscard]] static const Payload* tryPayloadAs(const ActorSubmission& submission) noexcept
        {
            const auto* payload = dynamic_cast<const ActorSubmission::TypedPayload<Payload>*>(submission._payload.get());
            return payload == nullptr ? nullptr : &payload->value;
        }

        template <typename Payload> [[nodiscard]] static const Payload& payloadAs(const ActorSubmission& submission)
        {
            const Payload* payload = tryPayloadAs<Payload>(submission);
            if (payload == nullptr)
            {
                throw std::logic_error{"ActorBinding received an incompatible submission payload"};
            }

            return *payload;
        }

        [[nodiscard]] virtual std::unique_ptr<ActorState> activate(EntityId entity) = 0;

        [[nodiscard]] virtual ActorDispatchResult
        dispatch(ActorState& state, const ActorSubmission& submission, ActorContext& context, std::stop_token stop_token) = 0;

        [[nodiscard]] virtual std::optional<ActorSubmission> makeTell(ActorKey target, TellPayload payload)
        {
            static_cast<void>(target);
            static_cast<void>(payload);
            return std::nullopt;
        }

        [[nodiscard]] virtual ActorDispatchResult resume(ActorState& state, ActorContext& context, std::stop_token stop_token) = 0;

        friend class ActorRuntime;
    };

    struct ActorRuntimeWorkerStats
    {
        std::uint64_t accepted{0};
        std::uint64_t processed{0};
        std::uint64_t rejected_full{0};
        std::uint64_t evicted_actors{0};
        std::size_t queue_depth{0};
        std::size_t queue_high_water_mark{0};
        std::size_t actor_count{0};
        std::size_t ready_actor_count{0};
        std::size_t mailbox_depth{0};
        std::size_t mailbox_high_water_mark{0};
        std::uint64_t budget_yield_turns{0};
        std::uint64_t suspended_commands{0};
        std::uint64_t reservation_rejections{0};
        std::uint64_t double_completions{0};
        std::uint64_t discarded_late_completions{0};
        std::uint64_t cancelled_operations{0};
        std::size_t suspended_task_count{0};
        std::size_t in_flight_operations{0};
        std::size_t in_flight_high_water_mark{0};
        std::size_t continuation_queue_depth{0};
        std::size_t scheduler_passivatable_actor_count{0};
        std::uint64_t timers_scheduled{0};
        std::uint64_t timers_rejected_full{0};
        std::uint64_t timers_fired{0};
        std::uint64_t timers_cancelled{0};
        std::uint64_t timers_discarded_stale{0};
        std::size_t active_timers{0};
        DistributionSnapshot queue_wait_nanoseconds;
        DistributionSnapshot suspend_duration_nanoseconds;
        DistributionSnapshot timer_lateness_nanoseconds;
    };

    struct ActorRuntimeStats
    {
        std::vector<ActorRuntimeWorkerStats> workers;
    };

    struct ActorRuntimeConfig
    {
        std::size_t worker_count{2};
        std::size_t queue_capacity_per_worker{4096};
        std::size_t max_in_flight_operations_per_worker{1024};
        std::function<void(std::size_t)> on_worker_start;
        std::function<void(std::size_t, const ActorKey&, const ActorSubmission&)> on_before_dispatch;
        std::function<void()> on_worker_failure;
    };

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

        [[nodiscard]] PostResult tryTell(ActorKey target, TellPayload payload);

        void close() noexcept;

        void cancel() noexcept;

        void requestActorOperationCancel(const ActorKey& key) noexcept;

        [[nodiscard]] std::size_t workerIndexFor(const ActorKey& key) const noexcept;
        [[nodiscard]] std::size_t workerCount() const noexcept;
        [[nodiscard]] ActorRuntimeStats getStats() const;

    private:
        struct QueuedSubmission;
        struct ActiveCommand;
        struct ActorEntry;
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
        [[nodiscard]] bool pumpWorker(Worker& worker);
        void applyCancelRequests(Worker& worker);
        [[nodiscard]] std::size_t drainContinuations(Worker& worker);
        void dispatchDueTimers(Worker& worker);
        [[nodiscard]] bool routeToMailbox(Worker& worker, QueuedSubmission submission);
        [[nodiscard]] bool runReadyActorTurn(Worker& worker, std::size_t worker_index);
        [[nodiscard]] bool hasReadyActor(const Worker& worker) const;
        [[nodiscard]] bool isWorkerDrained(const Worker& worker) const;
        void cancelWorkerIngress(Worker& worker) noexcept;
        void discardWorkerSubmissions(Worker& worker) noexcept;
        void discardWorkerTimers(Worker& worker) noexcept;
        void cancelSuspendedTasks(Worker& worker) noexcept;
        void destroyWorkerActors(Worker& worker) noexcept;
        [[nodiscard]] bool reserveOutstanding(Worker& worker) noexcept;
        void releaseOutstanding(Worker& worker, std::size_t count = 1) noexcept;
        [[nodiscard]] bool reserveOperation(Worker& worker) noexcept;
        void releaseOperation(Worker& worker) noexcept;
        void finishActiveCommand(Worker& worker, ActorEntry& entry, bool succeeded) noexcept;
        [[nodiscard]] bool publishContinuation(const ActorContinuation& continuation) noexcept;
        void reportRejectedCompletion(const ActorContinuation& continuation, ContinuationRejection rejection) noexcept;
        void workerFinished() noexcept;
        void recordWorkerFailure(std::exception_ptr error) noexcept;
        void joinWorkers() noexcept;
        static void updateMaximum(std::atomic<std::uint64_t>& target, std::uint64_t candidate) noexcept;

        const std::size_t _worker_count;
        const std::size_t _max_in_flight_operations;
        std::vector<std::unique_ptr<Worker>> _workers;
        std::shared_ptr<WorkerContinuationEndpoint> _continuation_endpoint;
        RuntimeCompletionSink& _runtime_completion;
        std::function<void(std::size_t)> _on_worker_start;
        std::function<void(std::size_t, const ActorKey&, const ActorSubmission&)> _on_before_dispatch;
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
