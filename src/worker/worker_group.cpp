#include "snf/worker/worker_group.hpp"

#include "snf/net/system_error.hpp"
#include "snf/net/tcp_listener.hpp"
#include "snf/net/unique_file_descriptor.hpp"

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <utility>

namespace
{
    [[nodiscard]] std::uint16_t listenerPort(const int descriptor)
    {
        sockaddr_in address{};
        auto address_size = static_cast<socklen_t>(sizeof(address));
        if (::getsockname(descriptor, reinterpret_cast<sockaddr*>(&address), &address_size) == -1)
        {
            snf::net::throw_system_error("getsockname(worker listener)");
        }
        return ntohs(address.sin_port);
    }
}

namespace snf::worker
{
    WorkerGroup::WorkerGroup(
        const WorkerGroupConfig& config,
        RequestSinkFactory request_sink_factory,
        ActorFactoryFactory actor_factory_factory,
        WorkerWatchdog::StallHandler watchdog_handler
    )
        : _config(config)
        , _barrier(config.worker_count)
        , _thread_exited(std::make_unique<std::atomic<bool>[]>(config.worker_count))
    {
        if (!isValid(config))
        {
            throw std::invalid_argument{"Invalid WorkerGroup configuration"};
        }
        if (config.actor.has_value() != static_cast<bool>(actor_factory_factory))
        {
            throw std::invalid_argument{"WorkerGroup actor configuration and factory must be provided together"};
        }

        for (std::uint16_t index = 0; index < config.worker_count; ++index)
        {
            _thread_exited[index].store(false, std::memory_order_relaxed);
        }
        _join_overruns.reserve(config.worker_count);

        std::vector<snf::net::UniqueFileDescriptor> listeners;
        listeners.reserve(config.worker_count);
        listeners.push_back(snf::net::create_tcp_listener(config.port, true));
        _port = listenerPort(listeners.front().getDescriptor());
        for (std::uint16_t index = 1; index < config.worker_count; ++index)
        {
            // A port-0 listener gets its concrete port from the first socket;
            // subsequent workers bind that same port with SO_REUSEPORT.
            listeners.push_back(snf::net::create_tcp_listener(_port, true));
        }

        _sinks.reserve(config.worker_count);
        if (actor_factory_factory)
        {
            _actor_factories.reserve(config.worker_count);
        }
        _workers.reserve(config.worker_count);
        for (std::uint16_t index = 0; index < config.worker_count; ++index)
        {
            std::unique_ptr<RequestSink> sink;
            if (request_sink_factory)
            {
                sink = request_sink_factory(WorkerId{index});
            }
            else
            {
                sink = std::make_unique<NullRequestSink>();
            }
            if (!sink)
            {
                throw std::invalid_argument{"WorkerGroup request sink factory returned nothing"};
            }

            auto worker = std::make_unique<Worker>(WorkerId{index}, config.worker_count, config.budgets, config.inbox, config.network, *sink);
            worker->attachListener(std::move(listeners[index]));
            worker->attachBarrier(&_barrier);

            if (actor_factory_factory && config.actor)
            {
                auto factory = actor_factory_factory(WorkerId{index});
                if (!factory)
                {
                    throw std::invalid_argument{"WorkerGroup actor factory returned nothing"};
                }
                worker->configureActors(*config.actor, *factory);
                _actor_factories.push_back(std::move(factory));
            }

            _sinks.push_back(std::move(sink));
            _workers.push_back(std::move(worker));
        }

        // Every target has one SPSC lane per source. The returned ports are
        // stored by the source worker, so only that worker can produce on the
        // lane during normal operation.
        for (std::uint16_t target = 0; target < config.worker_count; ++target)
        {
            for (std::uint16_t source = 0; source < config.worker_count; ++source)
            {
                auto port = _workers[target]->bindInboxSource(WorkerId{source});
                _workers[source]->bindRemoteTarget(WorkerId{target}, std::move(port));
            }
        }

        if (config.watchdog)
        {
            std::vector<WorkerWatchdogTarget> targets;
            targets.reserve(_workers.size());
            for (const auto& worker : _workers)
            {
                targets.push_back(WorkerWatchdogTarget{.worker = worker->id(), .progress = &worker->progress()});
            }
            _watchdog = std::make_unique<WorkerWatchdog>(*config.watchdog, config.budgets, std::move(targets), std::move(watchdog_handler));
        }

        for (const auto& worker : _workers)
        {
            _group_shutdown_budget = std::max(_group_shutdown_budget, worker->configuredShutdownTimeout());
        }
        _group_shutdown_budget += std::chrono::duration_cast<std::chrono::nanoseconds>(config.group_shutdown_grace);
    }

    WorkerGroup::~WorkerGroup()
    {
        stopAndJoinStartedThreads();
    }

    void WorkerGroup::start()
    {
        if (_started || _started_once)
        {
            throw std::logic_error{"WorkerGroup::start may only be called once"};
        }

        _started_once = true;
        _started = true;
        try
        {
            _threads.reserve(_workers.size());
            for (std::size_t index = 0; index < _workers.size(); ++index)
            {
                _threads.emplace_back(
                    [this, index]
                    {
                        workerMain(index);
                    }
                );
            }
            if (_watchdog)
            {
                _watchdog->start();
            }
        }
        catch (...)
        {
            _barrier.abort();
            stopAndJoinStartedThreads();
            throw;
        }
    }

    void WorkerGroup::join()
    {
        if (!_started)
        {
            return;
        }

        observeThreadExitDeadline();
        for (std::thread& thread : _threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
        if (_watchdog)
        {
            _watchdog->stop();
        }
        _threads.clear();
        _started = false;

        std::exception_ptr failure;
        {
            std::lock_guard lock{_failure_mutex};
            failure = _failure;
            _failure = nullptr;
        }
        if (failure)
        {
            std::rethrow_exception(failure);
        }
    }

    void WorkerGroup::run()
    {
        start();
        join();
    }

    void WorkerGroup::requestStop() noexcept
    {
        const auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
        std::int64_t unset = 0;
        static_cast<void>(_stop_requested_at_ns.compare_exchange_strong(unset, now_ns, std::memory_order_acq_rel, std::memory_order_acquire));
        _barrier.arm();
        for (const auto& worker : _workers)
        {
            worker->requestStop();
        }
    }

    std::uint16_t WorkerGroup::port() const noexcept
    {
        return _port;
    }

    std::uint16_t WorkerGroup::workerCount() const noexcept
    {
        return _config.worker_count;
    }

    const Worker& WorkerGroup::worker(const std::size_t index) const noexcept
    {
        return *_workers[index];
    }

    const WorkerWatchdogMetrics* WorkerGroup::watchdogMetrics() const noexcept
    {
        return _watchdog == nullptr ? nullptr : &_watchdog->metrics();
    }

    std::span<const WorkerJoinOverrun> WorkerGroup::joinOverruns() const noexcept
    {
        return _join_overruns;
    }

    bool WorkerGroup::isRunning() const noexcept
    {
        return _started;
    }

    void WorkerGroup::workerMain(const std::size_t index) noexcept
    {
        try
        {
            _workers[index]->run();
        }
        catch (...)
        {
            _barrier.abort();
            {
                std::lock_guard lock{_failure_mutex};
                if (!_failure)
                {
                    _failure = std::current_exception();
                }
            }
            requestStop();
        }
        _thread_exited[index].store(true, std::memory_order_release);
        _exit_cv.notify_all();
    }

    bool WorkerGroup::allStartedThreadsExited() const noexcept
    {
        for (std::size_t index = 0; index < _threads.size(); ++index)
        {
            if (!_thread_exited[index].load(std::memory_order_acquire))
            {
                return false;
            }
        }
        return true;
    }

    std::chrono::steady_clock::time_point WorkerGroup::stopRequestedAt() const noexcept
    {
        const std::int64_t value = _stop_requested_at_ns.load(std::memory_order_acquire);
        return std::chrono::steady_clock::time_point{std::chrono::nanoseconds{value}};
    }

    void WorkerGroup::observeThreadExitDeadline() noexcept
    {
        if (_stop_requested_at_ns.load(std::memory_order_acquire) == 0)
        {
            requestStop();
        }

        const auto deadline = stopRequestedAt() + _group_shutdown_budget;
        {
            std::unique_lock lock{_exit_mutex};
            static_cast<void>(_exit_cv.wait_until(
                lock,
                deadline,
                [this]
                {
                    return allStartedThreadsExited();
                }
            ));
        }

        if (allStartedThreadsExited())
        {
            return;
        }

        const auto observed_at = std::chrono::steady_clock::now();
        for (std::size_t index = 0; index < _threads.size(); ++index)
        {
            if (_thread_exited[index].load(std::memory_order_acquire))
            {
                continue;
            }

            const WorkerProgress::Sample sample = _workers[index]->progress().sample();
            const auto stuck_for = sample.entered_at == WorkerProgress::TimePoint{} || observed_at <= sample.entered_at
                                       ? std::chrono::nanoseconds::zero()
                                       : std::chrono::duration_cast<std::chrono::nanoseconds>(observed_at - sample.entered_at);
            _join_overruns.push_back(WorkerJoinOverrun{
                .worker = _workers[index]->id(),
                .phase = sample.phase,
                .stuck_for = stuck_for,
            });
            if (_watchdog)
            {
                _watchdog->notifyHandler(WorkerStallReport{
                    .worker = _workers[index]->id(),
                    .phase = sample.phase,
                    .stuck_for = stuck_for,
                });
            }
        }
    }

    void WorkerGroup::stopAndJoinStartedThreads() noexcept
    {
        if (!_started)
        {
            return;
        }

        requestStop();
        observeThreadExitDeadline();
        for (std::thread& thread : _threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
        if (_watchdog)
        {
            _watchdog->stop();
        }
        _threads.clear();
        _started = false;
    }
}
