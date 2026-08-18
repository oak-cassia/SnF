#include "snf/runtime/actor_runtime.hpp"

#include "snf/runtime/bounded_queue.hpp"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <optional>
#include <shared_mutex>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <vector>

namespace
{
    constexpr std::size_t INGRESS_BATCH_SIZE = 64;
    // Continuations are drained in bounded batches. Emptying the whole queue
    // would starve ingress, and the priority the coroutine contract asks for is
    // within a single actor, not across actors.
    constexpr std::size_t CONTINUATION_BATCH_SIZE = 64;
    constexpr std::size_t ACTOR_TURN_BUDGET = 16;

    // A Worker's single blocking point. It replaces the blocking ingress pop
    // because a Worker now has two input sources and a bounded queue can only be
    // waited on one at a time.
    //
    // The flag is sticky and every wake re-checks all sources, so a notification
    // that lands between two checks is never lost.
    class WorkerWakeup final
    {
    public:
        void notify() noexcept
        {
            {
                std::lock_guard lock{_mutex};
                _signalled = true;
            }

            _condition.notify_one();
        }

        void wait()
        {
            std::unique_lock lock{_mutex};
            _condition.wait(lock, [this] { return _signalled; });
            _signalled = false;
        }

        bool waitUntil(const std::chrono::steady_clock::time_point deadline)
        {
            std::unique_lock lock{_mutex};
            const bool triggered =
                _condition.wait_until(lock, deadline, [this] { return _signalled; });
            if (triggered)
            {
                _signalled = false;
                return true;
            }
            return false;
        }

    private:
        std::mutex _mutex;
        std::condition_variable _condition;
        bool _signalled{false};
    };

    // The terminal claim-to-publish window cannot allocate: a cancelling Worker
    // that loses the claim has no other path to finish. Storage is therefore
    // allocated once with the Worker's in-flight capacity and reused as a ring.
    class ReservedContinuationQueue final
    {
        static_assert(std::is_nothrow_copy_constructible_v<snf::runtime::ActorContinuation>);
        static_assert(std::is_nothrow_move_constructible_v<snf::runtime::ActorContinuation>);

    public:
        explicit ReservedContinuationQueue(const std::size_t capacity)
            : _items(capacity)
        {
        }

        [[nodiscard]] bool push(const snf::runtime::ActorContinuation& continuation) noexcept
        {
            std::lock_guard lock{_mutex};
            if (_size == _items.size())
            {
                return false;
            }

            _items[_tail].emplace(continuation);
            _tail = (_tail + 1) % _items.size();
            ++_size;
            return true;
        }

        [[nodiscard]] std::optional<snf::runtime::ActorContinuation> tryPop() noexcept
        {
            std::lock_guard lock{_mutex};
            if (_size == 0)
            {
                return std::nullopt;
            }

            std::optional<snf::runtime::ActorContinuation> continuation{std::move(_items[_head])};
            _items[_head].reset();
            _head = (_head + 1) % _items.size();
            --_size;
            return continuation;
        }

        [[nodiscard]] std::size_t size() const noexcept
        {
            std::lock_guard lock{_mutex};
            return _size;
        }

    private:
        mutable std::mutex _mutex;
        std::vector<std::optional<snf::runtime::ActorContinuation>> _items;
        std::size_t _head{0};
        std::size_t _tail{0};
        std::size_t _size{0};
    };
}

namespace snf::runtime
{
    struct ActorRuntime::QueuedSubmission
    {
        ActorSubmission submission;
        std::chrono::steady_clock::time_point enqueued_at;
    };

    // The submission an actor is currently executing, plus its accounting. It
    // outlives a single dispatch for two reasons: a suspended command keeps its
    // capacity and its turn open until a terminal outcome arrives, and a handler
    // that suspends still holds a reference into this submission, so the runtime
    // must own it for as long as the coroutine frame lives.
    struct ActorRuntime::ActiveCommand
    {
        QueuedSubmission submission;
        bool is_command{false};
        bool counted_suspension{false};
        std::chrono::steady_clock::time_point suspended_at{};
    };

    // The ActorContext an actor activation hands to its binding. It is stored in
    // the slot rather than on the dispatch stack because an awaiter that suspends
    // still holds it after dispatch has returned.
    class ActorRuntime::SlotContext final : public ActorContext
    {
    public:
        SlotContext() = default;

        void bind(ActorRuntime& runtime,
                  Worker& worker,
                  ActorSlotEntry& slot,
                  const ActorKey key) noexcept
        {
            _runtime = &runtime;
            _worker = &worker;
            _slot = &slot;
            _key = key;
        }

        [[nodiscard]] ActorKey key() const noexcept override
        {
            return _key;
        }

        [[nodiscard]] ActorIncarnation incarnation() const noexcept override;

        [[nodiscard]] std::optional<ActorCompletionHandle>
        tryBeginOperation(std::shared_ptr<AsyncOperationControl> operation) override;

        void abortOperation() noexcept override;

        [[nodiscard]] std::optional<TimerHandle> trySchedule(std::chrono::milliseconds delay,
                                                             ActorSubmission submission) override;

        void cancelTimer(TimerHandle handle) noexcept override;

    private:
        ActorRuntime* _runtime{nullptr};
        Worker* _worker{nullptr};
        ActorSlotEntry* _slot{nullptr};
        ActorKey _key{};
    };

    struct ActorRuntime::ActorSlotEntry
    {
        ActorBinding* binding;
        std::unique_ptr<ActorSlot> actor;
        std::deque<QueuedSubmission> mailbox;
        ActorExecutionState state{ActorExecutionState::Idle};
        ActorIncarnation incarnation{};
        std::unique_ptr<SlotContext> context{};
        std::optional<ActiveCommand> active_command{};
        std::optional<TaskId> expected_task{};
        std::shared_ptr<AsyncOperationControl> active_operation{};
        // Set when a cancel lost the terminal claim to a completion. The publish
        // is guaranteed to arrive, so the Worker waits for it instead of resuming.
        bool awaiting_guaranteed_publish{false};
        bool pending_resume{false};
        std::uint64_t next_task_id{0};
    };

    struct ActorRuntime::WorkerCounters
    {
        std::atomic<std::uint64_t> accepted{0};
        std::atomic<std::uint64_t> processed{0};
        std::atomic<std::uint64_t> rejected_full{0};
        std::atomic<std::uint64_t> evicted_actors{0};
        Distribution queue_wait;
        Distribution suspend_duration;
        Distribution timer_lateness;
        std::atomic<std::uint64_t> outstanding_high_water_mark{0};
        std::atomic<std::uint64_t> mailbox_depth{0};
        std::atomic<std::uint64_t> mailbox_high_water_mark{0};
        std::atomic<std::uint64_t> budget_yield_turns{0};
        std::atomic<std::uint64_t> suspended_commands{0};
        std::atomic<std::uint64_t> reservation_rejections{0};
        std::atomic<std::uint64_t> double_completions{0};
        std::atomic<std::uint64_t> discarded_late_completions{0};
        std::atomic<std::uint64_t> cancelled_operations{0};
        std::atomic<std::uint64_t> in_flight_high_water_mark{0};
        std::atomic<std::uint64_t> timers_scheduled{0};
        std::atomic<std::uint64_t> timers_rejected_full{0};
        std::atomic<std::uint64_t> timers_fired{0};
        std::atomic<std::uint64_t> timers_cancelled{0};
        std::atomic<std::uint64_t> timers_discarded_stale{0};
    };

    struct TimerEntry
    {
        ActorKey target;
        ActorIncarnation incarnation;
        std::uint64_t id{0};
        ActorSubmission submission;
    };

    struct ActorRuntime::Worker
    {
        Worker(const std::size_t queue_capacity, const std::size_t max_in_flight)
            : ingress(queue_capacity)
            , continuations(max_in_flight)
            , capacity(queue_capacity)
            , max_in_flight_operations(max_in_flight)
        {
        }

        BoundedQueue<QueuedSubmission> ingress;
        // Its capacity equals the in-flight bound, so a reserved operation always
        // has a free slot here. It is never closed or cancelled: a Worker that
        // lost a cancel race must still be able to receive the claimed completion.
        ReservedContinuationQueue continuations;
        const std::size_t capacity;
        const std::size_t max_in_flight_operations;
        std::atomic<std::size_t> outstanding{0};
        std::atomic<std::size_t> in_flight_operations{0};
        std::atomic<bool> cancel_scan_requested{false};
        WorkerCounters counters;
        WorkerWakeup wakeup;
        mutable std::mutex scheduling_mutex;
        std::multimap<std::chrono::steady_clock::time_point, TimerEntry> timers;
        std::uint64_t next_timer_id{1};
        std::unordered_map<ActorKey, ActorSlotEntry, ActorKeyHash> actors;
        std::deque<ActorKey> ready_actors;
        std::uint64_t next_incarnation{1};
        std::jthread thread;
    };

    // Producers hold this, not the runtime, so a completion that outlives the
    // runtime finds a deactivated endpoint instead of a dangling pointer.
    class ActorRuntime::WorkerContinuationEndpoint final : public ContinuationEndpoint
    {
    public:
        explicit WorkerContinuationEndpoint(ActorRuntime& runtime) noexcept
            : _runtime(&runtime)
        {
        }

        [[nodiscard]] bool publish(const ActorContinuation& continuation) noexcept override
        {
            const std::shared_lock lock{_mutex};
            if (_runtime == nullptr)
            {
                return false;
            }

            return _runtime->publishContinuation(continuation);
        }

        void reportRejectedCompletion(const ActorContinuation& continuation,
                                      const ContinuationRejection rejection) noexcept override
        {
            const std::shared_lock lock{_mutex};
            if (_runtime == nullptr)
            {
                return;
            }

            _runtime->reportRejectedCompletion(continuation, rejection);
        }

        // Only safe once every Worker has joined, which is also when no operation
        // can still be in flight. Deactivating any earlier would strand a Worker
        // waiting for an already-claimed completion.
        void deactivate() noexcept
        {
            const std::unique_lock lock{_mutex};
            _runtime = nullptr;
        }

    private:
        mutable std::shared_mutex _mutex;
        ActorRuntime* _runtime;
    };

    ActorIncarnation ActorRuntime::SlotContext::incarnation() const noexcept
    {
        return _slot->incarnation;
    }

    std::optional<ActorCompletionHandle>
    ActorRuntime::SlotContext::tryBeginOperation(std::shared_ptr<AsyncOperationControl> operation)
    {
        if (!operation)
        {
            throw std::invalid_argument{"An actor operation requires an operation state"};
        }

        // The slot fields are also read by requestActorOperationCancel from other
        // threads, so every write to them is guarded even on the owning Worker.
        std::lock_guard lock{_worker->scheduling_mutex};
        if (_slot->active_operation)
        {
            throw std::logic_error{"An actor may await only one operation at a time"};
        }

        if (!_runtime->reserveOperation(*_worker))
        {
            _worker->counters.reservation_rejections.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        const TaskId task{.value = _slot->next_task_id++};
        _slot->expected_task = task;
        _slot->active_operation = std::move(operation);

        return ActorCompletionHandle{
            .endpoint = _runtime->_continuation_endpoint,
            .continuation =
                ActorContinuation{
                    .target = _key,
                    .incarnation = _slot->incarnation,
                    .task = task,
                },
        };
    }

    void ActorRuntime::SlotContext::abortOperation() noexcept
    {
        std::lock_guard lock{_worker->scheduling_mutex};
        if (!_slot->active_operation)
        {
            return;
        }

        _slot->active_operation.reset();
        _slot->expected_task.reset();
        _runtime->releaseOperation(*_worker);
    }

    std::optional<TimerHandle>
    ActorRuntime::SlotContext::trySchedule(const std::chrono::milliseconds delay,
                                           ActorSubmission submission)
    {
        if (submission.target() != _key)
        {
            throw std::invalid_argument{"ActorContext received a submission for another actor"};
        }

        if (!_runtime->reserveOutstanding(*_worker))
        {
            _worker->counters.timers_rejected_full.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        const auto now = std::chrono::steady_clock::now();
        const auto deadline = now + delay;

        std::lock_guard lock{_worker->scheduling_mutex};
        const std::uint64_t timer_id = _worker->next_timer_id++;
        _worker->timers.emplace(deadline,
                                TimerEntry{
                                    .target = _key,
                                    .incarnation = _slot->incarnation,
                                    .id = timer_id,
                                    .submission = std::move(submission),
                                });
        _worker->counters.timers_scheduled.fetch_add(1, std::memory_order_relaxed);

        return TimerHandle{.id = timer_id};
    }

    void ActorRuntime::SlotContext::cancelTimer(const TimerHandle handle) noexcept
    {
        if (handle.id == 0)
        {
            return;
        }

        bool erased = false;
        {
            std::lock_guard lock{_worker->scheduling_mutex};
            for (auto it = _worker->timers.begin(); it != _worker->timers.end(); ++it)
            {
                if (it->second.id == handle.id && it->second.target == _key)
                {
                    _worker->timers.erase(it);
                    erased = true;
                    break;
                }
            }
        }

        if (erased)
        {
            _worker->counters.timers_cancelled.fetch_add(1, std::memory_order_relaxed);
            _runtime->releaseOutstanding(*_worker);
        }
    }

    const ActorKey& ActorSubmission::target() const noexcept
    {
        return _target;
    }

    ActorActivation ActorSubmission::activation() const noexcept
    {
        return _activation;
    }

    ActorAccounting ActorSubmission::accounting() const noexcept
    {
        return _accounting;
    }

    ActorRuntime::ActorRuntime(const ActorRuntimeConfig& config,
                               RuntimeCompletionSink& runtime_completion)
        : _worker_count(config.worker_count)
        , _max_in_flight_operations(config.max_in_flight_operations_per_worker)
        , _runtime_completion(runtime_completion)
        , _on_worker_start(config.on_worker_start)
        , _on_before_dispatch(config.on_before_dispatch)
        , _on_worker_failure(config.on_worker_failure)
    {
        if (_worker_count == 0 || config.queue_capacity_per_worker == 0 ||
            _max_in_flight_operations == 0)
        {
            throw std::invalid_argument{"Invalid ActorRuntime configuration"};
        }

        _continuation_endpoint = std::make_shared<WorkerContinuationEndpoint>(*this);

        _workers.reserve(_worker_count);
        for (std::size_t worker_index = 0; worker_index < _worker_count; ++worker_index)
        {
            _workers.push_back(std::make_unique<Worker>(config.queue_capacity_per_worker,
                                                        _max_in_flight_operations));
        }
    }

    ActorRuntime::~ActorRuntime()
    {
        cancel();
        joinWorkers();
        // Ordered after the join on purpose. Every operation is terminal by now,
        // so a late completion can be discarded without stranding a Worker.
        _continuation_endpoint->deactivate();
    }

    void ActorRuntime::registerBinding(ActorBinding& binding)
    {
        std::lock_guard lock{_state_mutex};
        if (_started)
        {
            throw std::logic_error{"ActorRuntime bindings must be registered before start"};
        }

        const bool inserted = _bindings.emplace(binding.kind(), &binding).second;
        if (!inserted)
        {
            throw std::invalid_argument{"ActorRuntime already has a binding for this ActorKind"};
        }
    }

    void ActorRuntime::start()
    {
        std::exception_ptr start_error;
        {
            std::lock_guard lock{_state_mutex};
            if (_started)
            {
                throw std::logic_error{"ActorRuntime::start may only be called once"};
            }

            _started = true;
            if (_input_state == InputState::NotStarted)
            {
                _input_state = InputState::Running;
            }

            try
            {
                for (std::size_t worker_index = 0; worker_index < _worker_count; ++worker_index)
                {
                    _workers[worker_index]->thread =
                        std::jthread{[this, worker_index] { runWorker(worker_index); }};
                }
            }
            catch (...)
            {
                start_error = std::current_exception();
            }
        }

        if (start_error)
        {
            cancel();
            joinWorkers();
            std::rethrow_exception(start_error);
        }
    }

    void ActorRuntime::join()
    {
        joinWorkers();

        std::exception_ptr error;
        {
            std::lock_guard lock{_error_mutex};
            error = _worker_error;
        }

        if (error)
        {
            std::rethrow_exception(error);
        }
    }

    PostResult ActorRuntime::tryPost(ActorSubmission submission)
    {
        Worker* target_worker = nullptr;
        {
            std::lock_guard lock{_state_mutex};
            const auto binding_iterator = _bindings.find(submission.target().kind);
            if (binding_iterator == _bindings.end() ||
                binding_iterator->second != submission._binding)
            {
                throw std::invalid_argument{
                    "ActorSubmission was not made by the registered ActorBinding"};
            }

            if (_input_state != InputState::Running)
            {
                return PostResult::Closed;
            }

            Worker& worker = *_workers[workerIndexFor(submission.target())];
            const ActorAccounting accounting = submission.accounting();
            if (!reserveOutstanding(worker))
            {
                if (accounting == ActorAccounting::Command)
                {
                    worker.counters.rejected_full.fetch_add(1, std::memory_order_relaxed);
                }
                return PostResult::Full;
            }

            bool pushed = false;
            try
            {
                pushed = worker.ingress.tryPush(QueuedSubmission{
                    .submission = std::move(submission),
                    .enqueued_at = std::chrono::steady_clock::now(),
                });
            }
            catch (...)
            {
                releaseOutstanding(worker);
                throw;
            }

            if (!pushed)
            {
                releaseOutstanding(worker);
                // The state mutex makes a close race impossible here. A failed
                // push is consequently the conservative capacity fallback.
                if (accounting == ActorAccounting::Command)
                {
                    worker.counters.rejected_full.fetch_add(1, std::memory_order_relaxed);
                }
                return PostResult::Full;
            }

            if (accounting == ActorAccounting::Command)
            {
                worker.counters.accepted.fetch_add(1, std::memory_order_relaxed);
            }
            target_worker = &worker;
        }

        // The Worker waits on its own wake-up rather than on the ingress queue, so
        // a push has to announce itself.
        target_worker->wakeup.notify();
        return PostResult::Accepted;
    }

    void ActorRuntime::close() noexcept
    {
        std::lock_guard lock{_state_mutex};
        if (_input_state == InputState::Closed || _input_state == InputState::Cancelled)
        {
            return;
        }

        _input_state = InputState::Closed;
        _completion_state.store(CompletionState::DrainRequested, std::memory_order_release);
        for (const auto& worker : _workers)
        {
            worker->ingress.close();
            worker->wakeup.notify();
        }
    }

    void ActorRuntime::cancel() noexcept
    {
        {
            std::lock_guard lock{_state_mutex};
            if (_input_state == InputState::Cancelled)
            {
                return;
            }

            _input_state = InputState::Cancelled;
            CompletionState completion = _completion_state.load(std::memory_order_acquire);
            while (completion != CompletionState::Cancelled &&
                   completion != CompletionState::DrainWon &&
                   completion != CompletionState::Failed &&
                   !_completion_state.compare_exchange_weak(completion,
                                                            CompletionState::Cancelled,
                                                            std::memory_order_acq_rel,
                                                            std::memory_order_acquire))
            {
            }
        }

        for (const auto& worker : _workers)
        {
            // Producers are stopped from here, but mailboxes, operation states and
            // coroutine frames belong to the owning Worker and are left to it.
            cancelWorkerIngress(*worker);
            worker->wakeup.notify();
        }

        // A binding may be waiting on an external backpressure point. Its token
        // is runtime-local, so cancelling it never cancels a shared outbound queue.
        _dispatch_stop_source.request_stop();
    }

    void ActorRuntime::requestActorOperationCancel(const ActorKey& key) noexcept
    {
        Worker& worker = *_workers[workerIndexFor(key)];
        {
            std::lock_guard lock{worker.scheduling_mutex};
            const auto actor_iterator = worker.actors.find(key);
            if (actor_iterator == worker.actors.end() || !actor_iterator->second.active_operation)
            {
                return;
            }

            // Only a flag. The owning Worker performs the Pending -> Cancelled
            // transition and the resume, so no coroutine is touched from here.
            actor_iterator->second.active_operation->requestCancel();
        }

        worker.cancel_scan_requested.store(true, std::memory_order_release);
        worker.wakeup.notify();
    }

    std::size_t ActorRuntime::workerIndexFor(const ActorKey& key) const noexcept
    {
        return ActorKeyHash{}(key) % _worker_count;
    }

    std::size_t ActorRuntime::workerCount() const noexcept
    {
        return _worker_count;
    }

    ActorRuntimeStats ActorRuntime::getStats() const
    {
        ActorRuntimeStats stats;
        stats.workers.reserve(_workers.size());

        for (const auto& worker : _workers)
        {
            std::size_t actor_count = 0;
            std::size_t ready_actor_count = 0;
            std::size_t suspended_task_count = 0;
            std::size_t passivatable_actor_count = 0;
            std::size_t active_timers = 0;
            {
                std::lock_guard lock{worker->scheduling_mutex};
                actor_count = worker->actors.size();
                ready_actor_count = worker->ready_actors.size();
                active_timers = worker->timers.size();
                for (const auto& [key, slot] : worker->actors)
                {
                    static_cast<void>(key);
                    if (slot.state == ActorExecutionState::Suspended || slot.pending_resume)
                    {
                        ++suspended_task_count;
                    }

                    if (slot.state == ActorExecutionState::Idle && slot.mailbox.empty() &&
                        !slot.active_command && !slot.active_operation && !slot.pending_resume)
                    {
                        ++passivatable_actor_count;
                    }
                }
            }

            stats.workers.push_back(ActorRuntimeWorkerStats{
                .accepted = worker->counters.accepted.load(std::memory_order_relaxed),
                .processed = worker->counters.processed.load(std::memory_order_relaxed),
                .rejected_full = worker->counters.rejected_full.load(std::memory_order_relaxed),
                .evicted_actors = worker->counters.evicted_actors.load(std::memory_order_relaxed),
                .queue_depth = worker->outstanding.load(std::memory_order_relaxed),
                .queue_high_water_mark = static_cast<std::size_t>(
                    worker->counters.outstanding_high_water_mark.load(std::memory_order_relaxed)),
                .actor_count = actor_count,
                .ready_actor_count = ready_actor_count,
                .mailbox_depth = static_cast<std::size_t>(
                    worker->counters.mailbox_depth.load(std::memory_order_relaxed)),
                .mailbox_high_water_mark = static_cast<std::size_t>(
                    worker->counters.mailbox_high_water_mark.load(std::memory_order_relaxed)),
                .budget_yield_turns =
                    worker->counters.budget_yield_turns.load(std::memory_order_relaxed),
                .suspended_commands =
                    worker->counters.suspended_commands.load(std::memory_order_relaxed),
                .reservation_rejections =
                    worker->counters.reservation_rejections.load(std::memory_order_relaxed),
                .double_completions =
                    worker->counters.double_completions.load(std::memory_order_relaxed),
                .discarded_late_completions =
                    worker->counters.discarded_late_completions.load(std::memory_order_relaxed),
                .cancelled_operations =
                    worker->counters.cancelled_operations.load(std::memory_order_relaxed),
                .suspended_task_count = suspended_task_count,
                .in_flight_operations =
                    worker->in_flight_operations.load(std::memory_order_relaxed),
                .in_flight_high_water_mark = static_cast<std::size_t>(
                    worker->counters.in_flight_high_water_mark.load(std::memory_order_relaxed)),
                .continuation_queue_depth = worker->continuations.size(),
                .scheduler_passivatable_actor_count = passivatable_actor_count,
                .timers_scheduled =
                    worker->counters.timers_scheduled.load(std::memory_order_relaxed),
                .timers_rejected_full =
                    worker->counters.timers_rejected_full.load(std::memory_order_relaxed),
                .timers_fired = worker->counters.timers_fired.load(std::memory_order_relaxed),
                .timers_cancelled =
                    worker->counters.timers_cancelled.load(std::memory_order_relaxed),
                .timers_discarded_stale =
                    worker->counters.timers_discarded_stale.load(std::memory_order_relaxed),
                .active_timers = active_timers,
                .queue_wait_nanoseconds = worker->counters.queue_wait.snapshot(),
                .suspend_duration_nanoseconds = worker->counters.suspend_duration.snapshot(),
                .timer_lateness_nanoseconds = worker->counters.timer_lateness.snapshot(),
            });
        }

        return stats;
    }

    void ActorRuntime::runWorker(const std::size_t worker_index)
    {
        Worker& worker = *_workers[worker_index];
        try
        {
            if (_on_worker_start)
            {
                _on_worker_start(worker_index);
            }

            while (pumpWorker(worker))
            {
                if (!runReadyActorTurn(worker, worker_index))
                {
                    if (worker.ingress.isCancelled())
                    {
                        break;
                    }

                    throw std::runtime_error{"ActorRuntime binding stopped without cancellation"};
                }
            }

            // A cancelled Worker cleans up after itself: the thread that requested
            // the cancel only asked. On the drained path these are all no-ops.
            discardWorkerSubmissions(worker);
            discardWorkerTimers(worker);
            cancelSuspendedTasks(worker);
            destroyWorkerActors(worker);
            workerFinished();
        }
        catch (...)
        {
            recordWorkerFailure(std::current_exception());
            // A failed Worker never returns to the pump, so it must transition its
            // own operations and destroy its own frames right here.
            discardWorkerTimers(worker);
            cancelSuspendedTasks(worker);
            destroyWorkerActors(worker);
        }
    }

    bool ActorRuntime::pumpWorker(Worker& worker)
    {
        while (true)
        {
            if (worker.ingress.isCancelled())
            {
                return false;
            }

            applyCancelRequests(worker);
            static_cast<void>(drainContinuations(worker));

            std::size_t routed = 0;
            while (routed < INGRESS_BATCH_SIZE)
            {
                auto submission = worker.ingress.tryPop();
                if (!submission)
                {
                    break;
                }

                if (!routeToMailbox(worker, std::move(*submission)))
                {
                    return false;
                }
                ++routed;
            }

            dispatchDueTimers(worker);

            if (hasReadyActor(worker))
            {
                return true;
            }

            if (worker.ingress.isClosed())
            {
                discardWorkerTimers(worker);
            }

            if (isWorkerDrained(worker))
            {
                return false;
            }

            std::optional<std::chrono::steady_clock::time_point> earliest_deadline;
            {
                std::lock_guard lock{worker.scheduling_mutex};
                if (!worker.timers.empty())
                {
                    earliest_deadline = worker.timers.begin()->first;
                }
            }

            if (earliest_deadline)
            {
                worker.wakeup.waitUntil(*earliest_deadline);
            }
            else
            {
                worker.wakeup.wait();
            }
        }
    }

    void ActorRuntime::applyCancelRequests(Worker& worker)
    {
        if (!worker.cancel_scan_requested.exchange(false, std::memory_order_acq_rel))
        {
            return;
        }

        std::lock_guard lock{worker.scheduling_mutex};
        for (auto& [key, slot] : worker.actors)
        {
            if (slot.state != ActorExecutionState::Suspended || !slot.active_operation ||
                slot.awaiting_guaranteed_publish)
            {
                continue;
            }

            if (!slot.active_operation->isCancelRequested())
            {
                continue;
            }

            if (slot.active_operation->claimCancelled())
            {
                // Winning the claim means no producer will ever write a result, so
                // the awaiter can be resumed with a cancellation.
                slot.active_operation.reset();
                slot.expected_task.reset();
                slot.pending_resume = true;
                slot.state = ActorExecutionState::Ready;
                worker.ready_actors.push_back(key);
                worker.counters.cancelled_operations.fetch_add(1, std::memory_order_relaxed);
                releaseOperation(worker);
            }
            else
            {
                // A completion already claimed the terminal transition, and its
                // publish is guaranteed to arrive. Resuming now would race the
                // producer's result write, so the Worker waits for the publish.
                slot.awaiting_guaranteed_publish = true;
            }
        }
    }

    std::size_t ActorRuntime::drainContinuations(Worker& worker)
    {
        std::size_t handled = 0;
        while (handled < CONTINUATION_BATCH_SIZE)
        {
            auto continuation = worker.continuations.tryPop();
            if (!continuation)
            {
                return handled;
            }

            bool matched = false;
            {
                std::lock_guard lock{worker.scheduling_mutex};
                const auto actor_iterator = worker.actors.find(continuation->target);
                if (actor_iterator != worker.actors.end())
                {
                    ActorSlotEntry& slot = actor_iterator->second;
                    if (slot.state == ActorExecutionState::Suspended &&
                        slot.incarnation == continuation->incarnation &&
                        slot.expected_task == continuation->task)
                    {
                        matched = true;
                        slot.active_operation.reset();
                        slot.expected_task.reset();
                        slot.awaiting_guaranteed_publish = false;
                        slot.pending_resume = true;
                        slot.state = ActorExecutionState::Ready;
                        worker.ready_actors.push_back(actor_iterator->first);
                    }
                }
            }

            // The reservation is released whether or not the completion applied:
            // it was taken for exactly this continuation.
            releaseOperation(worker);

            if (!matched)
            {
                // The activation or the task is gone, so the result belongs to
                // nobody. Only the operation state is cleaned up, by its own
                // shared ownership.
                worker.counters.discarded_late_completions.fetch_add(1, std::memory_order_relaxed);
            }
            ++handled;
        }

        return handled;
    }

    void ActorRuntime::dispatchDueTimers(Worker& worker)
    {
        std::vector<TimerEntry> due_entries;
        std::vector<std::chrono::steady_clock::time_point> due_deadlines;

        const auto now = std::chrono::steady_clock::now();
        {
            std::lock_guard lock{worker.scheduling_mutex};
            while (!worker.timers.empty() && worker.timers.begin()->first <= now)
            {
                auto it = worker.timers.begin();
                due_deadlines.push_back(it->first);
                due_entries.push_back(std::move(it->second));
                worker.timers.erase(it);
            }

            for (std::size_t i = 0; i < due_entries.size(); ++i)
            {
                auto& entry = due_entries[i];
                const auto deadline = due_deadlines[i];

                const auto actor_it = worker.actors.find(entry.target);
                if (actor_it == worker.actors.end() ||
                    actor_it->second.incarnation != entry.incarnation)
                {
                    // Stale timer
                    releaseOutstanding(worker);
                    worker.counters.timers_discarded_stale.fetch_add(1, std::memory_order_relaxed);
                    continue;
                }

                ActorSlotEntry& slot = actor_it->second;
                slot.mailbox.push_back(QueuedSubmission{
                    .submission = std::move(entry.submission),
                    .enqueued_at = now,
                });
                worker.counters.timers_fired.fetch_add(1, std::memory_order_relaxed);

                const auto depth =
                    worker.counters.mailbox_depth.fetch_add(1, std::memory_order_relaxed) + 1;
                updateMaximum(worker.counters.mailbox_high_water_mark, depth);

                if (slot.state == ActorExecutionState::Idle)
                {
                    slot.state = ActorExecutionState::Ready;
                    worker.ready_actors.push_back(entry.target);
                }

                const auto lateness =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(now - deadline);
                const auto lateness_ns =
                    lateness.count() > 0 ? static_cast<std::uint64_t>(lateness.count()) : 0;
                worker.counters.timer_lateness.record(lateness_ns);
            }
        }
    }

    bool ActorRuntime::routeToMailbox(Worker& worker, QueuedSubmission submission)
    {
        if (worker.ingress.isCancelled())
        {
            releaseOutstanding(worker);
            return false;
        }

        const ActorKey key = submission.submission.target();
        const auto binding_iterator = _bindings.find(key.kind);
        if (binding_iterator == _bindings.end())
        {
            releaseOutstanding(worker);
            throw std::logic_error{"ActorRuntime binding registry changed while running"};
        }
        ActorBinding* const binding = binding_iterator->second;

        bool discarded = false;
        bool consumed_without_slot = false;
        bool activation_required = false;
        {
            std::lock_guard lock{worker.scheduling_mutex};
            if (worker.ingress.isCancelled())
            {
                discarded = true;
            }
            else if (worker.actors.find(key) == worker.actors.end())
            {
                if (submission.submission.activation() == ActorActivation::ExistingOnly)
                {
                    consumed_without_slot = true;
                }
                else
                {
                    activation_required = true;
                }
            }
        }

        if (discarded || consumed_without_slot)
        {
            releaseOutstanding(worker);
            return !discarded;
        }

        // Binding activation is external domain code. It must not run while the
        // scheduler mutex is held: an activation hook may post more work or
        // request cancellation through the runtime.
        std::unique_ptr<ActorSlot> activated_actor;
        std::unique_ptr<SlotContext> activated_context;
        try
        {
            if (activation_required)
            {
                activated_actor = binding->activate(key.entity);
                if (!activated_actor)
                {
                    throw std::logic_error{"ActorBinding::activate returned no slot"};
                }

                // Allocated before the lock so that nothing under it can throw
                // after the slot has been inserted.
                activated_context = std::make_unique<SlotContext>();
            }

            std::lock_guard lock{worker.scheduling_mutex};
            if (worker.ingress.isCancelled())
            {
                discarded = true;
            }
            else
            {
                auto actor_iterator = worker.actors.find(key);
                if (actor_iterator == worker.actors.end())
                {
                    if (!activated_actor)
                    {
                        throw std::logic_error{"ActorRuntime lost an existing slot while routing"};
                    }

                    actor_iterator =
                        worker.actors
                            .emplace(key,
                                     ActorSlotEntry{
                                         .binding = binding,
                                         .actor = std::move(activated_actor),
                                         .mailbox = {},
                                         .state = ActorExecutionState::Idle,
                                         .incarnation =
                                             ActorIncarnation{.value = worker.next_incarnation++},
                                     })
                            .first;
                    actor_iterator->second.context = std::move(activated_context);
                    actor_iterator->second.context->bind(
                        *this, worker, actor_iterator->second, actor_iterator->first);
                }

                ActorSlotEntry& slot = actor_iterator->second;
                if (slot.binding != binding)
                {
                    throw std::logic_error{"ActorRuntime slot binding invariant violated"};
                }

                slot.mailbox.push_back(std::move(submission));
                try
                {
                    if (slot.state == ActorExecutionState::Idle)
                    {
                        worker.ready_actors.push_back(actor_iterator->first);
                        slot.state = ActorExecutionState::Ready;
                    }
                }
                catch (...)
                {
                    slot.mailbox.pop_back();
                    throw;
                }

                const std::uint64_t mailbox_depth =
                    worker.counters.mailbox_depth.fetch_add(1, std::memory_order_relaxed) + 1;
                updateMaximum(worker.counters.mailbox_high_water_mark, mailbox_depth);
            }
        }
        catch (...)
        {
            releaseOutstanding(worker);
            throw;
        }

        if (discarded)
        {
            releaseOutstanding(worker);
            return false;
        }

        return true;
    }

    bool ActorRuntime::runReadyActorTurn(Worker& worker, const std::size_t worker_index)
    {
        ActorKey key;
        ActorSlotEntry* slot = nullptr;
        {
            std::lock_guard lock{worker.scheduling_mutex};
            if (worker.ingress.isCancelled())
            {
                return false;
            }

            if (worker.ready_actors.empty())
            {
                return true;
            }

            key = worker.ready_actors.front();
            worker.ready_actors.pop_front();
            const auto actor_iterator = worker.actors.find(key);
            if (actor_iterator == worker.actors.end() ||
                actor_iterator->second.state != ActorExecutionState::Ready ||
                (actor_iterator->second.mailbox.empty() && !actor_iterator->second.pending_resume))
            {
                throw std::logic_error{"ActorRuntime ready queue invariant violated"};
            }

            slot = &actor_iterator->second;
            slot->state = ActorExecutionState::Running;
        }

        std::size_t handled_submissions = 0;
        while (handled_submissions < ACTOR_TURN_BUDGET)
        {
            bool resuming = false;
            {
                std::lock_guard lock{worker.scheduling_mutex};
                if (worker.ingress.isCancelled())
                {
                    slot->state = ActorExecutionState::Idle;
                    return false;
                }

                if (slot->pending_resume)
                {
                    // A resume takes priority over this actor's queued commands.
                    // That is the only priority the coroutine contract requires.
                    slot->pending_resume = false;
                    resuming = true;
                    if (slot->active_command)
                    {
                        worker.counters.suspend_duration.record(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() -
                                slot->active_command->suspended_at));
                    }
                }
                else if (slot->mailbox.empty())
                {
                    slot->state = ActorExecutionState::Idle;
                    return true;
                }
                else
                {
                    QueuedSubmission queued = std::move(slot->mailbox.front());
                    slot->mailbox.pop_front();
                    worker.counters.mailbox_depth.fetch_sub(1, std::memory_order_relaxed);

                    const bool is_command =
                        queued.submission.accounting() == ActorAccounting::Command;
                    if (is_command)
                    {
                        // Sampled once per command. A resume is not a new
                        // acceptance, so it never lands in this distribution.
                        worker.counters.queue_wait.record(
                            std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - queued.enqueued_at));
                    }

                    slot->active_command = ActiveCommand{
                        .submission = std::move(queued),
                        .is_command = is_command,
                    };
                }
            }

            ActorDispatchResult result;
            try
            {
                if (!resuming && _on_before_dispatch)
                {
                    _on_before_dispatch(
                        worker_index, key, slot->active_command->submission.submission);
                }

                result = resuming
                             ? slot->binding->resume(
                                   *slot->actor, *slot->context, _dispatch_stop_source.get_token())
                             : slot->binding->dispatch(*slot->actor,
                                                       slot->active_command->submission.submission,
                                                       *slot->context,
                                                       _dispatch_stop_source.get_token());
            }
            catch (...)
            {
                // The coroutine frame may still hold references into the active
                // submission. Failure cleanup destroys the owning ActorSlot first
                // and only then releases this command and its accounting.
                throw;
            }

            // Post-dispatch bookkeeping touches slot fields that other threads
            // read, so all of it happens under the scheduling mutex.
            if (result == ActorDispatchResult::Suspended)
            {
                std::lock_guard lock{worker.scheduling_mutex};
                if (!slot->active_operation)
                {
                    throw std::logic_error{
                        "ActorBinding suspended without an operation begun through its context"};
                }

                slot->state = ActorExecutionState::Suspended;
                if (slot->active_command)
                {
                    slot->active_command->suspended_at = std::chrono::steady_clock::now();
                    if (!slot->active_command->counted_suspension)
                    {
                        slot->active_command->counted_suspension = true;
                        worker.counters.suspended_commands.fetch_add(1, std::memory_order_relaxed);
                    }
                }

                // The command keeps its outstanding capacity and its turn stays
                // closed until a terminal outcome arrives. The actor is left out
                // of the ready queue, so its queued commands wait.
                return true;
            }

            if (result == ActorDispatchResult::Stopped)
            {
                {
                    std::lock_guard lock{worker.scheduling_mutex};
                    finishActiveCommand(worker, *slot, false);
                }

                if (!worker.ingress.isCancelled())
                {
                    throw std::logic_error{"ActorBinding returned Stopped before cancellation"};
                }
                return false;
            }

            {
                std::lock_guard lock{worker.scheduling_mutex};
                if (slot->active_operation)
                {
                    throw std::logic_error{
                        "ActorBinding completed a command with an operation still registered"};
                }

                finishActiveCommand(worker, *slot, true);
            }
            ++handled_submissions;

            if (result == ActorDispatchResult::Evict ||
                result == ActorDispatchResult::PassivateIfIdle)
            {
                std::size_t discarded_mailbox_submissions = 0;
                std::unique_ptr<ActorSlot> actor_to_destroy;
                bool retained_for_accepted_work = false;
                {
                    std::lock_guard lock{worker.scheduling_mutex};
                    if (result == ActorDispatchResult::PassivateIfIdle && !slot->mailbox.empty())
                    {
                        retained_for_accepted_work = true;
                    }
                    else
                    {
                        discarded_mailbox_submissions = slot->mailbox.size();
                        slot->mailbox.clear();
                        if (discarded_mailbox_submissions != 0)
                        {
                            worker.counters.mailbox_depth.fetch_sub(discarded_mailbox_submissions,
                                                                    std::memory_order_relaxed);
                            releaseOutstanding(worker, discarded_mailbox_submissions);
                        }

                        std::size_t purged_timers = 0;
                        for (auto it = worker.timers.begin(); it != worker.timers.end();)
                        {
                            if (it->second.target == key)
                            {
                                it = worker.timers.erase(it);
                                ++purged_timers;
                            }
                            else
                            {
                                ++it;
                            }
                        }
                        if (purged_timers != 0)
                        {
                            worker.counters.timers_cancelled.fetch_add(purged_timers,
                                                                       std::memory_order_relaxed);
                            releaseOutstanding(worker, purged_timers);
                        }

                        const auto actor_iterator = worker.actors.find(key);
                        if (actor_iterator == worker.actors.end() ||
                            &actor_iterator->second != slot)
                        {
                            throw std::logic_error{
                                "ActorRuntime actor eviction invariant violated"};
                        }
                        actor_to_destroy = std::move(actor_iterator->second.actor);
                        worker.actors.erase(actor_iterator);
                        worker.counters.evicted_actors.fetch_add(1, std::memory_order_relaxed);
                    }
                }
                if (retained_for_accepted_work)
                {
                    continue;
                }
                actor_to_destroy.reset();
                return true;
            }
        }

        std::lock_guard lock{worker.scheduling_mutex};
        if (worker.ingress.isCancelled())
        {
            slot->state = ActorExecutionState::Idle;
            return false;
        }

        if (slot->mailbox.empty())
        {
            slot->state = ActorExecutionState::Idle;
        }
        else
        {
            slot->state = ActorExecutionState::Ready;
            worker.ready_actors.push_back(key);
            worker.counters.budget_yield_turns.fetch_add(1, std::memory_order_relaxed);
        }

        return true;
    }

    bool ActorRuntime::hasReadyActor(const Worker& worker) const
    {
        std::lock_guard lock{worker.scheduling_mutex};
        return !worker.ready_actors.empty();
    }

    bool ActorRuntime::isWorkerDrained(const Worker& worker) const
    {
        // The coroutine contract's ActorRuntimeDrained, checked term by term. The
        // running-task term is implied: this only runs between actor turns.
        if (!worker.ingress.isClosed() || worker.ingress.size() != 0)
        {
            return false;
        }

        if (worker.in_flight_operations.load(std::memory_order_acquire) != 0 ||
            worker.continuations.size() != 0)
        {
            return false;
        }

        std::lock_guard lock{worker.scheduling_mutex};
        if (!worker.ready_actors.empty())
        {
            return false;
        }

        for (const auto& [key, slot] : worker.actors)
        {
            static_cast<void>(key);
            if (!slot.mailbox.empty() || slot.state == ActorExecutionState::Suspended ||
                slot.active_command || slot.active_operation || slot.pending_resume)
            {
                return false;
            }
        }

        return true;
    }

    void ActorRuntime::cancelWorkerIngress(Worker& worker) noexcept
    {
        releaseOutstanding(worker, worker.ingress.cancel());
    }

    void ActorRuntime::discardWorkerSubmissions(Worker& worker) noexcept
    {
        std::size_t discarded_mailbox_submissions = 0;
        {
            std::lock_guard lock{worker.scheduling_mutex};
            for (auto& [key, slot] : worker.actors)
            {
                static_cast<void>(key);
                discarded_mailbox_submissions += slot.mailbox.size();
                slot.mailbox.clear();
                if (slot.state == ActorExecutionState::Ready)
                {
                    slot.state = ActorExecutionState::Idle;
                }
            }
            worker.ready_actors.clear();
        }

        if (discarded_mailbox_submissions != 0)
        {
            worker.counters.mailbox_depth.fetch_sub(discarded_mailbox_submissions,
                                                    std::memory_order_relaxed);
            releaseOutstanding(worker, discarded_mailbox_submissions);
        }
    }

    void ActorRuntime::discardWorkerTimers(Worker& worker) noexcept
    {
        std::size_t discarded_timers = 0;
        {
            std::lock_guard lock{worker.scheduling_mutex};
            discarded_timers = worker.timers.size();
            worker.timers.clear();
        }

        if (discarded_timers != 0)
        {
            releaseOutstanding(worker, discarded_timers);
        }
    }

    void ActorRuntime::cancelSuspendedTasks(Worker& worker) noexcept
    {
        // Owning Worker only, on the cancel and failure paths. The frame is not
        // resumed: destroying it with the slot still runs the handler's scoped
        // destructors, and a cancelled runtime has no use for the result. A
        // per-operation cancel does resume, so the handler observes the
        // cancellation there.
        while (true)
        {
            bool awaiting_guaranteed_publish = false;
            {
                std::lock_guard lock{worker.scheduling_mutex};
                for (auto& [key, slot] : worker.actors)
                {
                    static_cast<void>(key);
                    if (slot.active_operation)
                    {
                        if (slot.active_operation->claimCancelled())
                        {
                            worker.counters.cancelled_operations.fetch_add(
                                1, std::memory_order_relaxed);
                            slot.active_operation.reset();
                            slot.expected_task.reset();
                            releaseOperation(worker);
                        }
                        else
                        {
                            // The completion owns the terminal transition. Even on
                            // hard cancel or Worker failure its reserved publish must
                            // be consumed before the frame and endpoint can go away.
                            slot.awaiting_guaranteed_publish = true;
                            awaiting_guaranteed_publish = true;
                            continue;
                        }
                    }

                    slot.pending_resume = false;
                    slot.awaiting_guaranteed_publish = false;
                    if (slot.state == ActorExecutionState::Suspended ||
                        slot.state == ActorExecutionState::Ready)
                    {
                        slot.state = ActorExecutionState::Idle;
                    }

                    // active_command deliberately stays alive. A lazy coroutine
                    // frame may still reference its payload, so destroyWorkerActors
                    // destroys the ActorSlot before closing command accounting.
                }
            }

            if (!awaiting_guaranteed_publish)
            {
                return;
            }

            // A publish may already be queued or may still be inside the contract's
            // narrow claim-to-publish window. Drain first, then wait on the sticky
            // Worker wake-up if the claimer has not published yet.
            if (drainContinuations(worker) == 0)
            {
                worker.wakeup.wait();
            }
        }
    }

    void ActorRuntime::destroyWorkerActors(Worker& worker) noexcept
    {
        try
        {
            decltype(worker.actors) actors_to_destroy;
            {
                std::lock_guard lock{worker.scheduling_mutex};
                worker.ready_actors.clear();
                actors_to_destroy.swap(worker.actors);
            }

            for (auto& [key, slot] : actors_to_destroy)
            {
                static_cast<void>(key);
                // A handler's frame may hold references into active_command. The
                // explicit reset fixes the order instead of relying on
                // ActorSlotEntry's reverse member-destruction order.
                slot.actor.reset();
                finishActiveCommand(worker, slot, false);
            }
            actors_to_destroy.clear();
        }
        catch (...)
        {
            // Actor destructors must not make a worker failure unreportable.
        }
    }

    bool ActorRuntime::reserveOutstanding(Worker& worker) noexcept
    {
        std::size_t current = worker.outstanding.load(std::memory_order_relaxed);
        while (current < worker.capacity)
        {
            if (worker.outstanding.compare_exchange_weak(
                    current, current + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                updateMaximum(worker.counters.outstanding_high_water_mark,
                              static_cast<std::uint64_t>(current + 1));
                return true;
            }
        }

        return false;
    }

    void ActorRuntime::releaseOutstanding(Worker& worker, const std::size_t count) noexcept
    {
        if (count != 0)
        {
            worker.outstanding.fetch_sub(count, std::memory_order_acq_rel);
        }
    }

    bool ActorRuntime::reserveOperation(Worker& worker) noexcept
    {
        // One reservation covers both the in-flight slot and the terminal
        // continuation slot, because the continuation queue's capacity is this
        // same bound. A publish therefore cannot be refused for capacity.
        std::size_t current = worker.in_flight_operations.load(std::memory_order_relaxed);
        while (current < worker.max_in_flight_operations)
        {
            if (worker.in_flight_operations.compare_exchange_weak(
                    current, current + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
            {
                updateMaximum(worker.counters.in_flight_high_water_mark,
                              static_cast<std::uint64_t>(current + 1));
                return true;
            }
        }

        return false;
    }

    void ActorRuntime::releaseOperation(Worker& worker) noexcept
    {
        worker.in_flight_operations.fetch_sub(1, std::memory_order_acq_rel);
    }

    // The caller holds the Worker's scheduling mutex, unless the slot has already
    // been detached from worker.actors during final Worker cleanup.
    void ActorRuntime::finishActiveCommand(Worker& worker,
                                           ActorSlotEntry& slot,
                                           const bool succeeded) noexcept
    {
        if (!slot.active_command)
        {
            return;
        }

        const bool is_command = slot.active_command->is_command;
        slot.active_command.reset();
        releaseOutstanding(worker);
        if (succeeded && is_command)
        {
            worker.counters.processed.fetch_add(1, std::memory_order_relaxed);
        }
    }

    bool ActorRuntime::publishContinuation(const ActorContinuation& continuation) noexcept
    {
        Worker& worker = *_workers[workerIndexFor(continuation.target)];
        if (!worker.continuations.push(continuation))
        {
            // An in-flight reservation owns one slot in this fixed queue until
            // this exact continuation is consumed. Full therefore means a broken
            // scheduler invariant, and returning would strand the owning Worker.
            std::terminate();
        }

        worker.wakeup.notify();
        return true;
    }

    void ActorRuntime::reportRejectedCompletion(const ActorContinuation& continuation,
                                                const ContinuationRejection rejection) noexcept
    {
        // A rejected completion never releases a reservation: whoever won the
        // terminal claim already did, or will when its continuation is consumed.
        Worker& worker = *_workers[workerIndexFor(continuation.target)];
        if (rejection == ContinuationRejection::DuplicateCompletion)
        {
            worker.counters.double_completions.fetch_add(1, std::memory_order_relaxed);
        }
        else
        {
            worker.counters.discarded_late_completions.fetch_add(1, std::memory_order_relaxed);
        }
    }

    void ActorRuntime::workerFinished() noexcept
    {
        const std::size_t completed = _finished_workers.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (completed != _worker_count)
        {
            return;
        }

        CompletionState expected = CompletionState::DrainRequested;
        if (_completion_state.compare_exchange_strong(expected,
                                                      CompletionState::DrainWon,
                                                      std::memory_order_acq_rel,
                                                      std::memory_order_acquire))
        {
            _runtime_completion.notifyDrained(RuntimeId::Logic);
        }
    }

    void ActorRuntime::recordWorkerFailure(std::exception_ptr error) noexcept
    {
        bool expected = false;
        if (!_worker_failure_recorded.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel))
        {
            return;
        }

        {
            std::lock_guard lock{_error_mutex};
            _worker_error = std::move(error);
        }

        _completion_state.store(CompletionState::Failed, std::memory_order_release);
        cancel();
        _runtime_completion.notifyFailed(RuntimeId::Logic);

        if (_on_worker_failure)
        {
            try
            {
                _on_worker_failure();
            }
            catch (...)
            {
                // Failure notification must never hide the worker exception.
            }
        }
    }

    void ActorRuntime::joinWorkers() noexcept
    {
        for (const auto& worker : _workers)
        {
            if (worker->thread.joinable())
            {
                worker->thread.join();
            }
        }
    }

    void ActorRuntime::updateMaximum(std::atomic<std::uint64_t>& target,
                                     const std::uint64_t candidate) noexcept
    {
        std::uint64_t current = target.load(std::memory_order_relaxed);
        while (current < candidate &&
               !target.compare_exchange_weak(
                   current, candidate, std::memory_order_relaxed, std::memory_order_relaxed))
        {
        }
    }
}
