#include "snf/server/actor_runtime.hpp"

#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <sys/eventfd.h>
#include <unistd.h>
#include <utility>

namespace snf::server
{
    ActorRuntime::ActorRuntime(const ActorRuntimeConfig& config,
                               snf::runtime::BoundedQueue<NetworkAction>& network_actions,
                               const int outbound_event_descriptor)
        : _worker_count(config.worker_count)
        , _network_actions(network_actions)
        , _outbound_event_descriptor(outbound_event_descriptor)
        , _message_dispatcher_factory(config.message_dispatcher_factory)
        , _on_worker_failure(config.on_worker_failure)
    {
        if (_worker_count == 0 || config.queue_capacity_per_worker == 0 ||
            _outbound_event_descriptor < 0)
        {
            throw std::invalid_argument{"Invalid ActorRuntime configuration"};
        }

        if (!_message_dispatcher_factory)
        {
            _message_dispatcher_factory =
                [](std::size_t) { return MessageDispatcher{}; };
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
                    _workers[worker_index]->thread = std::jthread{
                        [this, worker_index] { runWorker(worker_index); }};
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
        std::lock_guard lock{_state_mutex};
        if (_input_state != InputState::Running)
        {
            return PostResult::Closed;
        }

        const std::size_t worker_index = workerIndexFor(command.actor);
        Worker& worker = *_workers[worker_index];
        if (!worker.commands.tryPush(QueuedCommand{
                .command = std::move(command),
                .enqueued_at = std::chrono::steady_clock::now(),
            }))
        {
            worker.counters.rejected_full.fetch_add(1, std::memory_order_relaxed);
            return PostResult::Full;
        }

        worker.counters.accepted.fetch_add(1, std::memory_order_relaxed);
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
        _normal_drain_requested.store(true, std::memory_order_release);
        for (const auto& worker : _workers)
        {
            worker->commands.close();
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
        _cancelled.store(true, std::memory_order_release);
        for (const auto& worker : _workers)
        {
            worker->commands.cancel();
        }
    }

    std::size_t ActorRuntime::workerIndexFor(const ActorId actor) const noexcept
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

            stats.workers.push_back(ActorRuntimeWorkerStats{
                .accepted = accepted,
                .processed = worker->counters.processed.load(std::memory_order_relaxed),
                .rejected_full = worker->counters.rejected_full.load(std::memory_order_relaxed),
                .queue_depth = worker->commands.size(),
                .queue_high_water_mark = worker->commands.highWaterMark(),
                .average_queue_wait = queue_wait_samples == 0
                                          ? std::chrono::nanoseconds{0}
                                          : std::chrono::nanoseconds{total_wait / queue_wait_samples},
                .max_queue_wait = std::chrono::nanoseconds{
                    worker->counters.max_queue_wait_nanoseconds.load(std::memory_order_relaxed)},
            });
        }

        return stats;
    }

    void ActorRuntime::runWorker(const std::size_t worker_index)
    {
        try
        {
            MessageDispatcher dispatcher = _message_dispatcher_factory(worker_index);
            Worker& worker = *_workers[worker_index];

            while (const auto queued_command = worker.commands.pop())
            {
                const auto waited = std::chrono::steady_clock::now() - queued_command->enqueued_at;
                const auto wait_nanoseconds = static_cast<std::uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(waited).count());
                worker.counters.total_queue_wait_nanoseconds.fetch_add(wait_nanoseconds,
                                                                        std::memory_order_relaxed);
                worker.counters.queue_wait_samples.fetch_add(1, std::memory_order_relaxed);
                updateMaximum(worker.counters.max_queue_wait_nanoseconds, wait_nanoseconds);

                const auto dispatch_result = dispatcher.dispatch(queued_command->command.frame);
                if (!dispatch_result.handled())
                {
                    if (!publish(CloseConnection{
                            .connection = queued_command->command.connection,
                            .reason = CloseReason::ProtocolError,
                        }))
                    {
                        break;
                    }
                }
                else
                {
                    bool published_all = true;
                    for (const auto& response : dispatch_result.responses)
                    {
                        if (!publish(SendFrame{
                                .connection = queued_command->command.connection,
                                .frame = response,
                            }))
                        {
                            published_all = false;
                            break;
                        }
                    }

                    if (!published_all)
                    {
                        break;
                    }
                }

                worker.counters.processed.fetch_add(1, std::memory_order_relaxed);
            }

            workerFinished();
        }
        catch (...)
        {
            recordWorkerFailure(std::current_exception());
        }
    }

    void ActorRuntime::workerFinished() noexcept
    {
        const std::size_t completed =
            _finished_workers.fetch_add(1, std::memory_order_acq_rel) + 1;
        if (completed != _worker_count ||
            !_normal_drain_requested.load(std::memory_order_acquire) ||
            _cancelled.load(std::memory_order_acquire))
        {
            return;
        }

        bool expected = false;
        if (_drained_published.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        {
            static_cast<void>(publish(GameRuntimeDrained{}));
        }
    }

    void ActorRuntime::recordWorkerFailure(std::exception_ptr error) noexcept
    {
        bool expected = false;
        if (!_worker_failure_recorded.compare_exchange_strong(expected,
                                                              true,
                                                              std::memory_order_acq_rel))
        {
            return;
        }

        {
            std::lock_guard lock{_error_mutex};
            _worker_error = std::move(error);
        }

        cancel();
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

    bool ActorRuntime::publish(NetworkAction action) const
    {
        if (!_network_actions.push(std::move(action)))
        {
            return false;
        }

        signalNetwork();
        return true;
    }

    void ActorRuntime::signalNetwork() const noexcept
    {
        constexpr std::uint64_t wakeup_value = 1;

        while (::write(_outbound_event_descriptor, &wakeup_value, sizeof(wakeup_value)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return;
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
               !target.compare_exchange_weak(current,
                                             candidate,
                                             std::memory_order_relaxed,
                                             std::memory_order_relaxed))
        {
        }
    }
}
