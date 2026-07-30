#include "snf/runtime/actor_runtime.hpp"

#include "snf/runtime/bounded_queue.hpp"

#include <chrono>
#include <deque>
#include <optional>
#include <stdexcept>
#include <utility>

namespace
{
    constexpr std::size_t INGRESS_BATCH_SIZE = 64;
    constexpr std::size_t ACTOR_TURN_BUDGET = 16;
}

namespace snf::runtime
{
    struct ActorRuntime::QueuedSubmission
    {
        ActorSubmission submission;
        std::chrono::steady_clock::time_point enqueued_at;
    };

    struct ActorRuntime::ActorSlotEntry
    {
        ActorBinding* binding;
        std::unique_ptr<ActorSlot> actor;
        std::deque<QueuedSubmission> mailbox;
        ActorExecutionState state{ActorExecutionState::Idle};
    };

    struct ActorRuntime::WorkerCounters
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

    struct ActorRuntime::Worker
    {
        explicit Worker(const std::size_t queue_capacity)
            : ingress(queue_capacity)
            , capacity(queue_capacity)
        {
        }

        BoundedQueue<QueuedSubmission> ingress;
        const std::size_t capacity;
        std::atomic<std::size_t> outstanding{0};
        WorkerCounters counters;
        mutable std::mutex scheduling_mutex;
        std::unordered_map<ActorKey, ActorSlotEntry, ActorKeyHash> actors;
        std::deque<ActorKey> ready_actors;
        std::jthread thread;
    };

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
        , _runtime_completion(runtime_completion)
        , _on_worker_start(config.on_worker_start)
        , _on_before_dispatch(config.on_before_dispatch)
        , _on_worker_failure(config.on_worker_failure)
    {
        if (_worker_count == 0 || config.queue_capacity_per_worker == 0)
        {
            throw std::invalid_argument{"Invalid ActorRuntime configuration"};
        }

        _workers.reserve(_worker_count);
        for (std::size_t worker_index = 0; worker_index < _worker_count; ++worker_index)
        {
            _workers.push_back(std::make_unique<Worker>(config.queue_capacity_per_worker));
        }
    }

    ActorRuntime::~ActorRuntime()
    {
        cancel();
        joinWorkers();
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
        std::lock_guard lock{_state_mutex};
        const auto binding_iterator = _bindings.find(submission.target().kind);
        if (binding_iterator == _bindings.end() || binding_iterator->second != submission._binding)
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
            // The state mutex makes a close race impossible here. A failed push
            // is consequently the conservative capacity fallback.
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
        }
    }

    void ActorRuntime::cancel() noexcept
    {
        std::lock_guard lock{_state_mutex};
        if (_input_state == InputState::Cancelled)
        {
            return;
        }

        _input_state = InputState::Cancelled;
        CompletionState completion = _completion_state.load(std::memory_order_acquire);
        while (completion != CompletionState::Cancelled &&
               completion != CompletionState::DrainWon && completion != CompletionState::Failed &&
               !_completion_state.compare_exchange_weak(completion,
                                                        CompletionState::Cancelled,
                                                        std::memory_order_acq_rel,
                                                        std::memory_order_acquire))
        {
        }

        for (const auto& worker : _workers)
        {
            discardWorkerSubmissions(*worker);
        }
        // A binding may be waiting on an external backpressure point. Its token
        // is runtime-local, so cancelling it never cancels a shared outbound queue.
        _dispatch_stop_source.request_stop();
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
            const std::uint64_t queue_wait_samples =
                worker->counters.queue_wait_samples.load(std::memory_order_relaxed);
            const std::uint64_t total_wait =
                worker->counters.total_queue_wait_nanoseconds.load(std::memory_order_relaxed);

            std::size_t actor_count = 0;
            std::size_t ready_actor_count = 0;
            {
                std::lock_guard lock{worker->scheduling_mutex};
                actor_count = worker->actors.size();
                ready_actor_count = worker->ready_actors.size();
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
                .average_queue_wait =
                    queue_wait_samples == 0
                        ? std::chrono::nanoseconds{0}
                        : std::chrono::nanoseconds{total_wait / queue_wait_samples},
                .max_queue_wait =
                    std::chrono::nanoseconds{worker->counters.max_queue_wait_nanoseconds.load(
                        std::memory_order_relaxed)},
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

            while (fillIngress(worker))
            {
                if (!hasReadyActor(worker))
                {
                    continue;
                }

                if (!runReadyActorTurn(worker, worker_index))
                {
                    if (worker.ingress.isCancelled())
                    {
                        break;
                    }

                    throw std::runtime_error{"ActorRuntime binding stopped without cancellation"};
                }
            }

            destroyWorkerActors(worker);
            workerFinished();
        }
        catch (...)
        {
            recordWorkerFailure(std::current_exception());
            destroyWorkerActors(worker);
        }
    }

    bool ActorRuntime::fillIngress(Worker& worker)
    {
        const auto route = [this, &worker](QueuedSubmission submission)
        { return routeToMailbox(worker, std::move(submission)); };

        std::size_t routed = 0;
        if (!hasReadyActor(worker))
        {
            auto submission = worker.ingress.pop();
            if (!submission)
            {
                return false;
            }

            if (!route(std::move(*submission)))
            {
                return false;
            }
            ++routed;
        }

        while (routed < INGRESS_BATCH_SIZE)
        {
            auto submission = worker.ingress.tryPop();
            if (!submission)
            {
                break;
            }

            if (!route(std::move(*submission)))
            {
                return false;
            }
            ++routed;
        }

        return !worker.ingress.isCancelled();
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
        try
        {
            if (activation_required)
            {
                activated_actor = binding->activate(key.entity);
                if (!activated_actor)
                {
                    throw std::logic_error{"ActorBinding::activate returned no slot"};
                }
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

                    actor_iterator = worker.actors
                                         .emplace(key,
                                                  ActorSlotEntry{
                                                      .binding = binding,
                                                      .actor = std::move(activated_actor),
                                                      .mailbox = {},
                                                      .state = ActorExecutionState::Idle,
                                                  })
                                         .first;
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
                actor_iterator->second.mailbox.empty())
            {
                throw std::logic_error{"ActorRuntime ready queue invariant violated"};
            }

            slot = &actor_iterator->second;
            slot->state = ActorExecutionState::Running;
        }

        std::size_t handled_submissions = 0;
        while (handled_submissions < ACTOR_TURN_BUDGET)
        {
            std::optional<QueuedSubmission> queued;
            {
                std::lock_guard lock{worker.scheduling_mutex};
                if (worker.ingress.isCancelled())
                {
                    slot->state = ActorExecutionState::Idle;
                    return false;
                }

                if (slot->mailbox.empty())
                {
                    slot->state = ActorExecutionState::Idle;
                    return true;
                }

                queued.emplace(std::move(slot->mailbox.front()));
                slot->mailbox.pop_front();
                worker.counters.mailbox_depth.fetch_sub(1, std::memory_order_relaxed);
            }

            QueuedSubmission& event = *queued;
            const bool is_command = event.submission.accounting() == ActorAccounting::Command;
            if (is_command)
            {
                const auto waited = std::chrono::steady_clock::now() - event.enqueued_at;
                const auto wait_nanoseconds = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(waited).count());
                worker.counters.total_queue_wait_nanoseconds.fetch_add(wait_nanoseconds,
                                                                       std::memory_order_relaxed);
                worker.counters.queue_wait_samples.fetch_add(1, std::memory_order_relaxed);
                updateMaximum(worker.counters.max_queue_wait_nanoseconds, wait_nanoseconds);
            }

            ActorDispatchResult result;
            try
            {
                if (_on_before_dispatch)
                {
                    _on_before_dispatch(worker_index, key, event.submission);
                }
                result = slot->binding->dispatch(
                    *slot->actor, event.submission, _dispatch_stop_source.get_token());
            }
            catch (...)
            {
                releaseOutstanding(worker);
                throw;
            }

            if (result == ActorDispatchResult::Stopped)
            {
                releaseOutstanding(worker);
                if (!worker.ingress.isCancelled())
                {
                    throw std::logic_error{"ActorBinding returned Stopped before cancellation"};
                }
                return false;
            }

            releaseOutstanding(worker);
            ++handled_submissions;
            if (is_command)
            {
                worker.counters.processed.fetch_add(1, std::memory_order_relaxed);
            }

            if (result == ActorDispatchResult::Evict)
            {
                std::size_t discarded_mailbox_submissions = 0;
                std::unique_ptr<ActorSlot> actor_to_destroy;
                {
                    std::lock_guard lock{worker.scheduling_mutex};
                    discarded_mailbox_submissions = slot->mailbox.size();
                    slot->mailbox.clear();
                    if (discarded_mailbox_submissions != 0)
                    {
                        worker.counters.mailbox_depth.fetch_sub(discarded_mailbox_submissions,
                                                                std::memory_order_relaxed);
                        releaseOutstanding(worker, discarded_mailbox_submissions);
                    }

                    const auto actor_iterator = worker.actors.find(key);
                    if (actor_iterator == worker.actors.end() || &actor_iterator->second != slot)
                    {
                        throw std::logic_error{"ActorRuntime actor eviction invariant violated"};
                    }
                    actor_to_destroy = std::move(actor_iterator->second.actor);
                    worker.actors.erase(actor_iterator);
                    worker.counters.evicted_actors.fetch_add(1, std::memory_order_relaxed);
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

    void ActorRuntime::discardWorkerSubmissions(Worker& worker) noexcept
    {
        releaseOutstanding(worker, worker.ingress.cancel());

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
