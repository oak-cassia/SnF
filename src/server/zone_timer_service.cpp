#include "snf/server/zone_timer_service.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace snf::server
{
    TimerTimePoint SteadyTimerClock::now() const noexcept
    {
        return std::chrono::steady_clock::now();
    }

    ZoneTimerService::ZoneTimerService(ZoneTimerSink& sink,
                                       TimerClock& clock,
                                       ZoneTimerServiceConfig config)
        : _sink(sink)
        , _clock(clock)
        , _tick_interval(config.tick_interval)
        , _cancellation_retry_interval(config.cancellation_retry_interval)
        , _max_timers(config.max_timers)
        , _on_failure(std::move(config.on_failure))
    {
        if (_tick_interval < std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Zone tick interval cannot be negative"};
        }
        if (_cancellation_retry_interval <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Timer cancellation retry interval must be positive"};
        }
        if (_max_timers == 0)
        {
            throw std::invalid_argument{"Timer capacity must be positive"};
        }
    }

    ZoneTimerService::~ZoneTimerService()
    {
        try
        {
            stop();
        }
        catch (...)
        {
            // std::jthread owns the final no-throw join fallback.
        }
    }

    void ZoneTimerService::start()
    {
        std::lock_guard lock{_mutex};
        if (_started)
        {
            throw std::logic_error{"ZoneTimerService may only be started once"};
        }
        if (_closed)
        {
            throw std::logic_error{"Closed ZoneTimerService cannot be started"};
        }

        _started = true;
        _thread = std::jthread{[this](const std::stop_token stop_token) { run(stop_token); }};
    }

    void ZoneTimerService::stop()
    {
        {
            std::lock_guard lock{_mutex};
            closeLocked();
        }
        _thread.request_stop();
        _wake.notify_all();
        if (_thread.joinable())
        {
            _thread.join();
        }
    }

    ZoneTimerRegistration ZoneTimerService::tryEnsureTimer(const ZoneId zone)
    {
        if (zone.value == 0)
        {
            throw std::invalid_argument{"Zone timer target must be non-zero"};
        }

        std::lock_guard lock{_mutex};
        if (_closed)
        {
            return {
                .result = snf::runtime::PostResult::Closed,
                .timer = std::nullopt,
                .created = false,
            };
        }
        if (_tick_interval == std::chrono::milliseconds::zero())
        {
            return {
                .result = snf::runtime::PostResult::Accepted,
                .timer = std::nullopt,
                .created = false,
            };
        }

        auto existing = _timers.find(zone);
        if (existing != _timers.end() && existing->second.state == TimerState::CancelPending)
        {
            const snf::runtime::PostResult cancellation =
                tryPublishCancellationLocked(existing, true);
            if (cancellation != snf::runtime::PostResult::Accepted)
            {
                return {
                    .result = cancellation,
                    .timer = std::nullopt,
                    .created = false,
                };
            }
            existing = _timers.end();
        }
        if (existing != _timers.end())
        {
            return {
                .result = snf::runtime::PostResult::Accepted,
                .timer = existing->second.id,
                .created = false,
            };
        }
        if (_timers.size() == _max_timers)
        {
            return {
                .result = snf::runtime::PostResult::Full,
                .timer = std::nullopt,
                .created = false,
            };
        }
        if (_next_timer_id == std::numeric_limits<std::uint64_t>::max())
        {
            throw std::overflow_error{"Timer identity exhausted"};
        }

        const TimerId id{.value = ++_next_timer_id};
        const TimerTimePoint now = _clock.now();
        if (TimerTimePoint::max() - now < _tick_interval)
        {
            throw std::overflow_error{"Zone timer deadline overflow"};
        }

        auto [timer, inserted] = _timers.emplace(zone,
                                                 TimerRecord{
                                                     .id = id,
                                                     .state = TimerState::Active,
                                                     .next_deadline = now + _tick_interval,
                                                     .tick = 0,
                                                 });
        static_cast<void>(inserted);

        snf::runtime::PostResult result;
        try
        {
            result = _sink.tryPostTimerCommand(zone, ArmZoneSimulationTimer{.timer = id});
        }
        catch (...)
        {
            _timers.erase(timer);
            throw;
        }
        if (result != snf::runtime::PostResult::Accepted)
        {
            _timers.erase(timer);
            if (result == snf::runtime::PostResult::Closed)
            {
                closeLocked();
            }
            return {
                .result = result,
                .timer = std::nullopt,
                .created = false,
            };
        }

        ++_scheduled;
        ++_revision;
        _wake.notify_all();
        return {
            .result = snf::runtime::PostResult::Accepted,
            .timer = id,
            .created = true,
        };
    }

    snf::runtime::PostResult ZoneTimerService::tryCancelTimer(const ZoneId zone)
    {
        std::lock_guard lock{_mutex};
        auto timer = _timers.find(zone);
        if (timer == _timers.end())
        {
            return _closed ? snf::runtime::PostResult::Closed : snf::runtime::PostResult::Accepted;
        }

        timer->second.state = TimerState::CancelPending;
        ++_revision;
        const snf::runtime::PostResult result = tryPublishCancellationLocked(timer, false);
        _wake.notify_all();
        return result;
    }

    void ZoneTimerService::dispatchDue()
    {
        bool notify_failure = false;
        {
            std::lock_guard lock{_mutex};
            if (!_closed)
            {
                try
                {
                    dispatchDueLocked(_clock.now());
                }
                catch (...)
                {
                    notify_failure = recordFailureLocked(std::current_exception());
                }
            }
        }
        if (notify_failure)
        {
            notifyFailure();
        }
    }

    ZoneTimerServiceStats ZoneTimerService::stats() const
    {
        std::lock_guard lock{_mutex};
        ZoneTimerServiceStats result{
            .scheduled = _scheduled,
            .cancelled = _cancelled,
            .fired = _fired,
            .dropped_full = _dropped_full,
            .skipped_intervals = _skipped_intervals,
            .cancellation_retries = _cancellation_retries,
            .failures = _failures,
        };
        for (const auto& [zone, timer] : _timers)
        {
            static_cast<void>(zone);
            if (timer.state == TimerState::Active)
            {
                ++result.active_timers;
            }
            else
            {
                ++result.pending_cancellations;
            }
        }
        return result;
    }

    void ZoneTimerService::rethrowIfFailed() const
    {
        std::exception_ptr error;
        {
            std::lock_guard lock{_mutex};
            error = _error;
        }
        if (error)
        {
            std::rethrow_exception(error);
        }
    }

    void ZoneTimerService::run(const std::stop_token stop_token) noexcept
    {
        bool failed = false;
        try
        {
            std::unique_lock lock{_mutex};
            while (!stop_token.stop_requested() && !_closed)
            {
                const TimerTimePoint now = _clock.now();
                dispatchDueLocked(now);
                if (_closed)
                {
                    break;
                }

                const std::uint64_t revision = _revision;
                const auto wake_at = nextWakeLocked(now);
                if (wake_at)
                {
                    _wake.wait_until(lock,
                                     *wake_at,
                                     [this, revision, &stop_token] {
                                         return stop_token.stop_requested() || _closed ||
                                                _revision != revision;
                                     });
                }
                else
                {
                    _wake.wait(lock,
                               [this, revision, &stop_token] {
                                   return stop_token.stop_requested() || _closed ||
                                          _revision != revision;
                               });
                }
            }
        }
        catch (...)
        {
            std::lock_guard lock{_mutex};
            failed = recordFailureLocked(std::current_exception());
        }

        if (failed)
        {
            notifyFailure();
        }
    }

    void ZoneTimerService::dispatchDueLocked(const TimerTimePoint now)
    {
        for (auto timer = _timers.begin(); timer != _timers.end();)
        {
            if (timer->second.state == TimerState::CancelPending)
            {
                const snf::runtime::PostResult result = tryPublishCancellationLocked(timer, true);
                if (result == snf::runtime::PostResult::Accepted)
                {
                    timer = _timers.begin();
                    continue;
                }
                if (result == snf::runtime::PostResult::Closed)
                {
                    return;
                }
                ++timer;
                continue;
            }

            if (now < timer->second.next_deadline)
            {
                ++timer;
                continue;
            }

            const auto overdue = now - timer->second.next_deadline;
            _skipped_intervals += static_cast<std::uint64_t>(overdue / _tick_interval);
            timer->second.next_deadline = now + _tick_interval;
            ++timer->second.tick;
            const snf::runtime::PostResult result =
                _sink.tryPostTimerCommand(timer->first,
                                          ZoneSimulationTick{
                                              .timer = timer->second.id,
                                              .tick = timer->second.tick,
                                          });
            if (result == snf::runtime::PostResult::Accepted)
            {
                ++_fired;
            }
            else if (result == snf::runtime::PostResult::Full)
            {
                ++_dropped_full;
            }
            else
            {
                closeLocked();
                return;
            }
            ++timer;
        }
    }

    snf::runtime::PostResult
    ZoneTimerService::tryPublishCancellationLocked(const TimerMap::iterator timer, const bool retry)
    {
        if (retry)
        {
            ++_cancellation_retries;
        }
        const snf::runtime::PostResult result = _sink.tryPostTimerCommand(
            timer->first, CancelZoneSimulationTimer{.timer = timer->second.id});
        if (result == snf::runtime::PostResult::Accepted)
        {
            _timers.erase(timer);
            ++_cancelled;
            ++_revision;
        }
        else if (result == snf::runtime::PostResult::Closed)
        {
            closeLocked();
        }
        return result;
    }

    std::optional<TimerTimePoint> ZoneTimerService::nextWakeLocked(const TimerTimePoint now) const
    {
        std::optional<TimerTimePoint> result;
        for (const auto& [zone, timer] : _timers)
        {
            static_cast<void>(zone);
            const TimerTimePoint candidate = timer.state == TimerState::Active
                                                 ? timer.next_deadline
                                                 : now + _cancellation_retry_interval;
            result = result ? std::min(*result, candidate) : candidate;
        }
        return result;
    }

    bool ZoneTimerService::recordFailureLocked(std::exception_ptr error) noexcept
    {
        if (!_error)
        {
            _error = std::move(error);
            ++_failures;
        }
        closeLocked();
        if (!_failure_notified)
        {
            _failure_notified = true;
            return true;
        }
        return false;
    }

    void ZoneTimerService::notifyFailure() const noexcept
    {
        if (!_on_failure)
        {
            return;
        }
        try
        {
            _on_failure();
        }
        catch (...)
        {
            // The original timer failure remains authoritative.
        }
    }

    void ZoneTimerService::closeLocked() noexcept
    {
        if (!_closed)
        {
            _closed = true;
            _timers.clear();
            ++_revision;
        }
    }
}
