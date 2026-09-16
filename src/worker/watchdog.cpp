#include "snf/worker/watchdog.hpp"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace
{
    [[nodiscard]] bool isShutdownPhase(const snf::worker::WorkerPhase phase) noexcept
    {
        return phase >= snf::worker::WorkerPhase::ShutdownA && phase <= snf::worker::WorkerPhase::ShutdownD;
    }

    void atomicMax(std::atomic<std::uint64_t>& target, const std::uint64_t value) noexcept
    {
        std::uint64_t observed = target.load(std::memory_order_relaxed);
        while (observed < value && !target.compare_exchange_weak(observed, value, std::memory_order_relaxed, std::memory_order_relaxed))
        {
        }
    }
}

namespace snf::worker
{
    bool isValid(const WorkerWatchdogConfig& config) noexcept
    {
        return config.sample_interval > std::chrono::milliseconds::zero() && config.phase_stall_threshold > std::chrono::milliseconds::zero() &&
               config.poll_wait_stall_threshold >= std::chrono::milliseconds::zero();
    }

    WorkerWatchdog::WorkerWatchdog(
        WorkerWatchdogConfig config,
        const WorkerBudgets budgets,
        std::vector<WorkerWatchdogTarget> targets,
        StallHandler handler
    )
        : _config(config)
        , _poll_wait_stall_threshold(
              config.poll_wait_stall_threshold == std::chrono::milliseconds::zero() ? budgets.max_poll_timeout * 4 + config.phase_stall_threshold
                                                                                    : config.poll_wait_stall_threshold
          )
        , _targets(std::move(targets))
        , _handler(std::move(handler))
        , _stalled(_targets.size(), false)
    {
        if (!isValid(_config) || _targets.empty() || _poll_wait_stall_threshold <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Invalid Worker watchdog configuration"};
        }
        if (std::ranges::any_of(
                _targets,
                [](const WorkerWatchdogTarget& target)
                {
                    return target.progress == nullptr;
                }
            ))
        {
            throw std::invalid_argument{"Worker watchdog target has no progress source"};
        }
    }

    WorkerWatchdog::~WorkerWatchdog()
    {
        stop();
    }

    void WorkerWatchdog::start()
    {
        if (_thread.joinable() || _running.load(std::memory_order_acquire))
        {
            throw std::logic_error{"Worker watchdog is already running"};
        }
        _stop_requested.store(false, std::memory_order_release);
        std::fill(_stalled.begin(), _stalled.end(), false);
        _thread = std::thread(
            [this]
            {
                run();
            }
        );
    }

    void WorkerWatchdog::stop() noexcept
    {
        _stop_requested.store(true, std::memory_order_release);
        _wait_cv.notify_all();
        if (_thread.joinable())
        {
            _thread.join();
        }
    }

    bool WorkerWatchdog::running() const noexcept
    {
        return _running.load(std::memory_order_acquire);
    }

    const WorkerWatchdogMetrics& WorkerWatchdog::metrics() const noexcept
    {
        return _metrics;
    }

    std::chrono::milliseconds WorkerWatchdog::pollWaitStallThreshold() const noexcept
    {
        return _poll_wait_stall_threshold;
    }

    void WorkerWatchdog::notifyHandler(const WorkerStallReport& report) const noexcept
    {
        if (!_handler)
        {
            return;
        }
        try
        {
            _handler(report);
        }
        catch (...)
        {
            // Diagnostic callbacks cannot kill the Worker runtime.
        }
    }

    void WorkerWatchdog::run() noexcept
    {
        _running.store(true, std::memory_order_release);
        while (!_stop_requested.load(std::memory_order_acquire))
        {
            sampleOnce(std::chrono::steady_clock::now());
            std::unique_lock lock{_wait_mutex};
            _wait_cv.wait_for(
                lock,
                _config.sample_interval,
                [this]
                {
                    return _stop_requested.load(std::memory_order_acquire);
                }
            );
        }
        _running.store(false, std::memory_order_release);
    }

    void WorkerWatchdog::sampleOnce(const std::chrono::steady_clock::time_point now) noexcept
    {
        for (std::size_t index = 0; index < _targets.size(); ++index)
        {
            const WorkerProgress::Sample sample = _targets[index].progress->sample();
            _metrics.samples_taken.fetch_add(1, std::memory_order_relaxed);

            if (sample.phase == WorkerPhase::Starting || sample.phase == WorkerPhase::Stopped || sample.entered_at == WorkerProgress::TimePoint{})
            {
                _stalled[index] = false;
                continue;
            }

            const auto stuck_for = std::chrono::duration_cast<std::chrono::nanoseconds>(now - sample.entered_at);
            const auto threshold = sample.phase == WorkerPhase::PollWait
                                       ? std::chrono::duration_cast<std::chrono::nanoseconds>(_poll_wait_stall_threshold)
                                       : std::chrono::duration_cast<std::chrono::nanoseconds>(_config.phase_stall_threshold);
            if (stuck_for < threshold)
            {
                _stalled[index] = false;
                continue;
            }

            const std::uint64_t stuck_ns = stuck_for.count() <= 0 ? 0 : static_cast<std::uint64_t>(stuck_for.count());
            atomicMax(_metrics.longest_stall_ns, stuck_ns);
            _metrics.last_stall_phase.store(static_cast<std::uint32_t>(sample.phase), std::memory_order_relaxed);

            if (_stalled[index])
            {
                continue;
            }
            _stalled[index] = true;
            if (isShutdownPhase(sample.phase))
            {
                _metrics.shutdown_stall_episodes.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                _metrics.active_stall_episodes.fetch_add(1, std::memory_order_relaxed);
            }

            notifyHandler(WorkerStallReport{.worker = _targets[index].worker, .phase = sample.phase, .stuck_for = stuck_for});
        }
    }
}
