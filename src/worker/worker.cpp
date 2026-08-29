#include "snf/worker/worker.hpp"

#include <algorithm>
#include <cassert>

namespace snf::worker
{
    Worker::Worker(const WorkerId id, const std::uint16_t worker_count, const WorkerBudgets budgets, const WorkerInboxConfig inbox_config)
        : _id(id)
        , _budgets(budgets)
        , _poller(budgets.poll.max_events)
        , _inbox(worker_count, _wakeup, inbox_config)
    {
        assert(worker_count > 0);
        _poller.add(_wakeup.descriptor(), PollToken{PollTargetKind::Wakeup, 0, 0}, PollInterest{.read = true, .write = false});
    }

    void Worker::run()
    {
        bindOwnerThread();

        while (!_stop_requested.load(std::memory_order_acquire))
        {
            ++_metrics.loop_iterations;
            const auto timeout = hasRunnableWork() ? std::chrono::milliseconds(0) : pollTimeout();
            const auto events = _poller.wait(timeout);

            processPollEvents(events, _budgets.poll);
            drainInbox(_budgets.inbox);
            expireTimers(std::chrono::steady_clock::now(), _budgets.timers);
            runReadyActors(_budgets.actors);
            flushWrites(_budgets.writes);
        }

        // stop 이후: inbox.close() 뒤 남은 accepted event와 만료된 timer를
        // hard deadline(초기값 2s)까지 정리하고 반환
        _inbox.close();
        const auto shutdown_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);

        while (std::chrono::steady_clock::now() < shutdown_deadline)
        {
            const auto drain_res = _inbox.drain(
                _budgets.inbox,
                [this](WorkerEvent&& ev)
                {
                    onEvent(std::move(ev));
                }
            );
            _metrics.shutdown_inbox_events += drain_res.processed;

            const auto expire_res = _timers.expire(
                std::chrono::steady_clock::now(),
                _budgets.timers,
                [this](TimerPayload&& payload)
                {
                    onTimer(std::move(payload));
                }
            );
            _metrics.shutdown_timers_fired += expire_res.expired;

            if (!drain_res.has_more && !expire_res.due_items_remain)
            {
                break;
            }
        }
    }

    void Worker::requestStop() noexcept
    {
        _stop_requested.store(true, std::memory_order_release);
        _wakeup.notify();
    }

    WorkerInboxPort Worker::bindInboxSource(const WorkerId source) noexcept
    {
        return _inbox.bindSource(source);
    }

    void Worker::setEventHandler(EventHandler handler)
    {
        _event_handler = std::move(handler);
    }

    void Worker::setTimerHandler(TimerHandler handler)
    {
        _timer_handler = std::move(handler);
    }

    bool Worker::trySchedule(const TimePoint deadline, TimerPayload payload)
    {
        assertOwnerThread();
        return _timers.trySchedule(deadline, std::move(payload));
    }

    const WorkerMetrics& Worker::metrics() const noexcept
    {
        return _metrics;
    }

    WorkerId Worker::id() const noexcept
    {
        return _id;
    }

    void Worker::bindOwnerThread() noexcept
    {
        _owner_thread = std::this_thread::get_id();
    }

    void Worker::assertOwnerThread() const noexcept
    {
#ifndef NDEBUG
        assert(_owner_thread == std::thread::id{} || _owner_thread == std::this_thread::get_id());
#endif
    }

    bool Worker::hasRunnableWork() const noexcept
    {
        return _inbox_has_more || _timers_have_due;
    }

    std::optional<std::chrono::milliseconds> Worker::pollTimeout() const
    {
        const auto next_deadline = _timers.nextDeadline();
        if (!next_deadline.has_value())
        {
            return _budgets.max_poll_timeout;
        }

        const auto now = std::chrono::steady_clock::now();
        if (*next_deadline <= now)
        {
            return std::chrono::milliseconds(0);
        }

        // duration_cast 는 0 으로 절단되어 1ms 미만 deadline 에서 timeout 0 인 바쁜 대기를 만든다.
        const auto diff = std::chrono::ceil<std::chrono::milliseconds>(*next_deadline - now);
        return std::min(diff, _budgets.max_poll_timeout);
    }

    void Worker::processPollEvents(const std::span<const PollEvent> events, const IoBudget&)
    {
        assertOwnerThread();
        _metrics.poll_events += events.size();
        for (const auto& ev : events)
        {
            if (ev.token.kind == PollTargetKind::Wakeup)
            {
                _wakeup.consume();
                ++_metrics.wakeups_consumed;
            }
        }
    }

    void Worker::drainInbox(const InboxBudget& budget)
    {
        assertOwnerThread();
        const auto res = _inbox.drain(
            budget,
            [this](WorkerEvent&& ev)
            {
                onEvent(std::move(ev));
            }
        );
        _metrics.inbox_events += res.processed;
        _inbox_has_more = res.has_more;
        if (res.budget_exhausted)
        {
            ++_metrics.inbox_budget_stops;
        }
    }

    void Worker::expireTimers(const TimePoint now, const CountTimeBudget& budget)
    {
        assertOwnerThread();
        const auto res = _timers.expire(
            now,
            budget,
            [this](TimerPayload&& payload)
            {
                onTimer(std::move(payload));
            }
        );
        _metrics.timers_fired += res.expired;
        _timers_have_due = res.due_items_remain;
        if (res.budget_exhausted)
        {
            ++_metrics.timer_budget_stops;
        }
    }

    void Worker::runReadyActors(const CountTimeBudget&)
    {
        // 4단계에서 구현
    }

    void Worker::flushWrites(const ByteTimeBudget&)
    {
        // 3단계에서 구현
    }

    void Worker::onEvent(WorkerEvent&& event)
    {
        assertOwnerThread();
        if (_event_handler)
        {
            _event_handler(std::move(event));
        }
    }

    void Worker::onTimer(TimerPayload&& payload)
    {
        assertOwnerThread();
        if (_timer_handler)
        {
            _timer_handler(std::move(payload));
        }
    }
}
