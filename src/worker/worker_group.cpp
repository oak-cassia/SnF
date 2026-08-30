#include "snf/worker/worker_group.hpp"

#include "snf/net/system_error.hpp"
#include "snf/net/tcp_listener.hpp"
#include "snf/net/unique_file_descriptor.hpp"

#include <arpa/inet.h>
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
        ActorFactoryFactory actor_factory_factory
    )
        : _config(config)
    {
        if (!isValid(config))
        {
            throw std::invalid_argument{"Invalid WorkerGroup configuration"};
        }
        if (config.actor.has_value() != static_cast<bool>(actor_factory_factory))
        {
            throw std::invalid_argument{"WorkerGroup actor configuration and factory must be provided together"};
        }

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
        }
        catch (...)
        {
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

        for (std::thread& thread : _threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
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
            {
                std::lock_guard lock{_failure_mutex};
                if (!_failure)
                {
                    _failure = std::current_exception();
                }
            }
            requestStop();
        }
    }

    void WorkerGroup::stopAndJoinStartedThreads() noexcept
    {
        if (!_started)
        {
            return;
        }

        requestStop();
        for (std::thread& thread : _threads)
        {
            if (thread.joinable())
            {
                thread.join();
            }
        }
        _threads.clear();
        _started = false;
    }
}
