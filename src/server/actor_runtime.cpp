#include "snf/server/actor_runtime.hpp"

#include <chrono>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace
{
    constexpr std::size_t INGRESS_BATCH_SIZE = 64;
    constexpr std::size_t ACTOR_TURN_BUDGET = 16;
}

namespace snf::server
{
    ActorRuntime::ActorRuntime(const ActorRuntimeConfig& config,
                               PlayerEffectSink& player_effects,
                               RuntimeCompletionSink& runtime_completion)
        : _worker_count(config.worker_count)
        , _runtime_id(config.runtime_id)
        , _player_effects(player_effects)
        , _runtime_completion(runtime_completion)
        , _on_worker_start(config.on_worker_start)
        , _on_before_command(config.on_before_command)
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

    PostResult ActorRuntime::tryPost(InboundCommand command)
    {
        return postEvent(
            QueuedEvent{
                .payload = std::move(command),
                .enqueued_at = std::chrono::steady_clock::now(),
            },
            EventKind::Command);
    }

    PostResult ActorRuntime::tryPostConnectionClosed(const ProvisionalActorId actor,
                                                     ConnectionClosed closed)
    {
        return postEvent(
            QueuedEvent{
                .payload =
                    ConnectionClosedEvent{
                        .actor = actor,
                        .closed = closed,
                    },
                .enqueued_at = std::chrono::steady_clock::now(),
            },
            EventKind::ConnectionClosed);
    }

    PostResult ActorRuntime::postEvent(QueuedEvent event, const EventKind kind)
    {
        std::lock_guard lock{_state_mutex};
        if (_input_state != InputState::Running)
        {
            return PostResult::Closed;
        }

        const ProvisionalActorId actor =
            std::visit([](const auto& payload) { return payload.actor; }, event.payload);
        Worker& worker = *_workers[workerIndexFor(actor)];
        if (!reserveOutstanding(worker))
        {
            if (kind == EventKind::Command)
            {
                worker.counters.rejected_full.fetch_add(1, std::memory_order_relaxed);
            }
            return PostResult::Full;
        }

        bool pushed = false;
        try
        {
            pushed = worker.ingress.tryPush(std::move(event));
        }
        catch (...)
        {
            releaseOutstanding(worker);
            throw;
        }

        if (!pushed)
        {
            releaseOutstanding(worker);
            // close() cannot race the state check above, so a failed ingress push
            // here is capacity accounting's conservative fallback.
            if (kind == EventKind::Command)
            {
                worker.counters.rejected_full.fetch_add(1, std::memory_order_relaxed);
            }
            return PostResult::Full;
        }

        if (kind == EventKind::Command)
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
            discardWorkerCommands(*worker);
        }
        // Publish waits are released only after every ingress reports cancellation,
        // so a worker cannot mistake an explicit cancel for an outbound failure.
        _effect_stop_source.request_stop();
    }

    std::size_t ActorRuntime::workerIndexFor(const ProvisionalActorId actor) const noexcept
    {
        return static_cast<std::size_t>(actor.value % static_cast<std::uint64_t>(_worker_count));
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
            const std::uint64_t accepted =
                worker->counters.accepted.load(std::memory_order_relaxed);
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
                .accepted = accepted,
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
        try
        {
            Worker& worker = *_workers[worker_index];
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

                    throw std::runtime_error{"ActorRuntime failed to publish a network action"};
                }
            }

            workerFinished();
        }
        catch (...)
        {
            recordWorkerFailure(std::current_exception());
        }
    }

    bool ActorRuntime::fillIngress(Worker& worker)
    {
        const auto route = [this, &worker](QueuedEvent event)
        { return routeToMailbox(worker, std::move(event)); };

        std::size_t routed = 0;
        if (!hasReadyActor(worker))
        {
            auto command = worker.ingress.pop();
            if (!command)
            {
                return false;
            }

            if (!route(std::move(*command)))
            {
                return false;
            }
            ++routed;
        }

        while (routed < INGRESS_BATCH_SIZE)
        {
            auto command = worker.ingress.tryPop();
            if (!command)
            {
                break;
            }

            if (!route(std::move(*command)))
            {
                return false;
            }
            ++routed;
        }

        return !worker.ingress.isCancelled();
    }

    bool ActorRuntime::routeToMailbox(Worker& worker, QueuedEvent event)
    {
        if (worker.ingress.isCancelled())
        {
            releaseOutstanding(worker);
            return false;
        }

        const ProvisionalActorId actor_id =
            std::visit([](const auto& payload) { return payload.actor; }, event.payload);
        const bool is_close = std::holds_alternative<ConnectionClosedEvent>(event.payload);
        bool discarded = false;
        bool consumed_without_slot = false;
        try
        {
            std::lock_guard lock{worker.scheduling_mutex};
            if (worker.ingress.isCancelled())
            {
                discarded = true;
            }
            else
            {
                auto actor_iterator = worker.actors.find(actor_id);
                if (actor_iterator == worker.actors.end())
                {
                    if (is_close)
                    {
                        consumed_without_slot = true;
                    }
                    else
                    {
                        actor_iterator = worker.actors.try_emplace(actor_id).first;
                    }
                }

                if (!consumed_without_slot)
                {
                    ActorSlot& slot = actor_iterator->second;
                    slot.mailbox.push_back(std::move(event));
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
        }
        catch (...)
        {
            releaseOutstanding(worker);
            throw;
        }

        if (discarded || consumed_without_slot)
        {
            releaseOutstanding(worker);
            return !discarded;
        }

        return true;
    }

    bool ActorRuntime::runReadyActorTurn(Worker& worker, const std::size_t worker_index)
    {
        ProvisionalActorId actor_id;
        ActorSlot* slot = nullptr;
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

            actor_id = worker.ready_actors.front();
            worker.ready_actors.pop_front();
            auto actor_iterator = worker.actors.find(actor_id);
            if (actor_iterator == worker.actors.end() ||
                actor_iterator->second.state != ActorExecutionState::Ready ||
                actor_iterator->second.mailbox.empty())
            {
                throw std::logic_error{"ActorRuntime ready queue invariant violated"};
            }

            slot = &actor_iterator->second;
            slot->state = ActorExecutionState::Running;
        }

        std::size_t handled_this_turn = 0;
        while (handled_this_turn < ACTOR_TURN_BUDGET)
        {
            QueuedEvent event;
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

                event = std::move(slot->mailbox.front());
                slot->mailbox.pop_front();
                worker.counters.mailbox_depth.fetch_sub(1, std::memory_order_relaxed);
            }

            if (auto* command = std::get_if<InboundCommand>(&event.payload))
            {
                const auto waited = std::chrono::steady_clock::now() - event.enqueued_at;
                const auto wait_nanoseconds = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(waited).count());
                worker.counters.total_queue_wait_nanoseconds.fetch_add(wait_nanoseconds,
                                                                       std::memory_order_relaxed);
                worker.counters.queue_wait_samples.fetch_add(1, std::memory_order_relaxed);
                updateMaximum(worker.counters.max_queue_wait_nanoseconds, wait_nanoseconds);

                try
                {
                    if (_on_before_command)
                    {
                        _on_before_command(worker_index, actor_id, command->command);
                    }

                    PlayerResult result = slot->actor.handle(command->command);

                    if (!_player_effects.apply(command->connection,
                                               std::move(result),
                                               _effect_stop_source.get_token()))
                    {
                        worker.counters.processed.fetch_add(1, std::memory_order_relaxed);
                        releaseOutstanding(worker);
                        return false;
                    }
                }
                catch (...)
                {
                    releaseOutstanding(worker);
                    throw;
                }

                worker.counters.processed.fetch_add(1, std::memory_order_relaxed);
                releaseOutstanding(worker);
                ++handled_this_turn;
                continue;
            }

            // Close events are intentionally excluded from command wait and
            // processing metrics. The owning worker is the only eraser, and this
            // cached slot pointer is no longer touched after the erase below.
            static_cast<void>(std::get<ConnectionClosedEvent>(event.payload));
            releaseOutstanding(worker);

            std::size_t discarded_mailbox_events = 0;
            {
                std::lock_guard lock{worker.scheduling_mutex};
                discarded_mailbox_events = slot->mailbox.size();
                slot->mailbox.clear();
                if (discarded_mailbox_events != 0)
                {
                    worker.counters.mailbox_depth.fetch_sub(discarded_mailbox_events,
                                                            std::memory_order_relaxed);
                    releaseOutstanding(worker, discarded_mailbox_events);
                }

                const auto actor_iterator = worker.actors.find(actor_id);
                if (actor_iterator == worker.actors.end() || &actor_iterator->second != slot)
                {
                    throw std::logic_error{"ActorRuntime actor eviction invariant violated"};
                }
                worker.actors.erase(actor_iterator);
                worker.counters.evicted_actors.fetch_add(1, std::memory_order_relaxed);
            }

            return true;
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
            worker.ready_actors.push_back(actor_id);
            worker.counters.budget_yield_turns.fetch_add(1, std::memory_order_relaxed);
        }

        return true;
    }

    bool ActorRuntime::hasReadyActor(const Worker& worker) const
    {
        std::lock_guard lock{worker.scheduling_mutex};
        return !worker.ready_actors.empty();
    }

    void ActorRuntime::discardWorkerCommands(Worker& worker) noexcept
    {
        releaseOutstanding(worker, worker.ingress.cancel());

        std::size_t discarded_mailbox_commands = 0;
        {
            std::lock_guard lock{worker.scheduling_mutex};
            for (auto& [actor_id, slot] : worker.actors)
            {
                static_cast<void>(actor_id);
                discarded_mailbox_commands += slot.mailbox.size();
                slot.mailbox.clear();
                if (slot.state == ActorExecutionState::Ready)
                {
                    slot.state = ActorExecutionState::Idle;
                }
            }
            worker.ready_actors.clear();
        }

        if (discarded_mailbox_commands != 0)
        {
            worker.counters.mailbox_depth.fetch_sub(discarded_mailbox_commands,
                                                    std::memory_order_relaxed);
            releaseOutstanding(worker, discarded_mailbox_commands);
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
            _runtime_completion.notifyDrained(_runtime_id);
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

        _runtime_completion.notifyFailed(_runtime_id);

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
