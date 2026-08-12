#include "snf/server/game_server.hpp"

#include "snf/net/system_error.hpp"

#include <algorithm>
#include <stdexcept>
#include <sys/eventfd.h>
#include <utility>

namespace
{
    snf::net::UniqueFileDescriptor create_outbound_event()
    {
        const int descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (descriptor == -1)
        {
            snf::net::throw_system_error("eventfd");
        }

        return snf::net::UniqueFileDescriptor{descriptor};
    }
}

namespace snf::server
{
    GameServer::GameServer(const GameServerConfig& config)
        : _metrics_reporter(config.metrics_reporter)
        , _outbound_event(create_outbound_event())
        , _outbound_channel(
              [&config]
              {
                  return OutboundChannelConfig{
                      .capacity = config.outbound_queue_capacity,
                      // A per-connection cap above the shared capacity is unreachable
                      // anyway, so it is capped rather than rejected: shrinking the
                      // channel must not make the server refuse to start.
                      .max_slots_per_connection = std::min(config.max_outbound_slots_per_connection,
                                                           config.outbound_queue_capacity),
                      .max_grants_per_turn = config.outbound_grants_per_turn,
                      // A Worker reserves an in-flight slot before it registers as a
                      // waiter, so the registry is sized above the sum of those
                      // budgets and can never be the tighter of the two bounds.
                      .max_waiters = config.actor_worker_count *
                                     config.actor_max_in_flight_operations_per_worker,
                  };
              }(),
              _outbound_event.getDescriptor())
        , _outbound_sink(_outbound_channel)
        , _player_effects(_outbound_sink)
        , _runtime_completion(snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic),
                              _outbound_event.getDescriptor())
        , _player_actor_binding(_player_effects, _outbound_sink)
        , _logic_runtime(
              [config]
              {
                  snf::runtime::ActorRuntimeConfig runtime_config;
                  runtime_config.worker_count = config.actor_worker_count;
                  runtime_config.queue_capacity_per_worker = config.actor_queue_capacity_per_worker;
                  runtime_config.max_in_flight_operations_per_worker =
                      config.actor_max_in_flight_operations_per_worker;
                  return runtime_config;
              }(),
              _runtime_completion)
        , _player_actor_ingress(_logic_runtime, _player_actor_binding)
        , _command_router(_player_actor_ingress)
        , _protocol_gateway(_command_router)
        , _tcp_server(
              TcpServerConfig{
                  .port = config.port,
                  .shutdown_grace_period = config.shutdown_grace_period,
                  .max_pending_send_bytes = config.max_pending_send_bytes,
                  .client_send_buffer_size = config.client_send_buffer_size,
                  .connection_lifecycle_capacity = config.connection_lifecycle_capacity,
                  .metrics_report_interval = config.metrics_report_interval,
                  .on_metrics_interval = [this] { publishMetrics(); },
              },
              _protocol_gateway,
              _outbound_channel,
              _runtime_completion,
              _outbound_event.getDescriptor())
    {
        _logic_runtime.registerBinding(_player_actor_binding);
    }

    GameServer::GameServer(const std::uint16_t port,
                           const std::chrono::milliseconds shutdown_grace_period)
        : GameServer(GameServerConfig{
              .port = port,
              .shutdown_grace_period = shutdown_grace_period,
              .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
              .client_send_buffer_size = std::nullopt,
              .actor_worker_count = 2,
              .actor_queue_capacity_per_worker = 4096,
              .outbound_queue_capacity = 4096,
              .connection_lifecycle_capacity = 4096,
          })
    {
    }

    GameServer::~GameServer()
    {
        requestStop();
        cancelActorRuntime();
        try
        {
            joinActorRuntime();
        }
        catch (...)
        {
            // A destructor cannot propagate a worker failure. run() reports it instead.
        }
    }

    std::uint16_t GameServer::getPort() const noexcept
    {
        return _tcp_server.getPort();
    }

    const GameServerStats& GameServer::getStats() const noexcept
    {
        return _tcp_server.getStats();
    }

    snf::runtime::ActorRuntimeStats GameServer::getActorRuntimeStats() const
    {
        return _logic_runtime.getStats();
    }

    ServerMetricsSnapshot GameServer::getMetricsSnapshot() const
    {
        return ServerMetricsSnapshot{
            .counters = _tcp_server.getStats(),
            .network = _tcp_server.getMetrics(),
            .actor_runtime = _logic_runtime.getStats(),
        };
    }

    void GameServer::run(const int termination_signal_descriptor)
    {
        startActorRuntime();

        try
        {
            _tcp_server.run(termination_signal_descriptor);
        }
        catch (...)
        {
            cancelActorRuntime();
            try
            {
                joinActorRuntime();
            }
            catch (...)
            {
                // Preserve the reactor failure that entered this path.
            }
            throw;
        }

        joinActorRuntime();
    }

    void GameServer::requestStop() const noexcept
    {
        _tcp_server.requestStop();
    }

    void GameServer::startActorRuntime()
    {
        if (_has_run)
        {
            throw std::logic_error{"GameServer::run may only be called once"};
        }

        _has_run = true;

        _logic_runtime.start();
    }

    void GameServer::joinActorRuntime()
    {
        _logic_runtime.join();
    }

    void GameServer::cancelActorRuntime() noexcept
    {
        _protocol_gateway.cancel();
        // Also the path a reactor failure takes, and the reason a Worker suspended on
        // outbound capacity still reaches a terminal outcome when the reactor is gone.
        static_cast<void>(_outbound_channel.cancel());
    }

    void GameServer::publishMetrics() const
    {
        if (!_metrics_reporter)
        {
            return;
        }

        _metrics_reporter(getMetricsSnapshot());
    }
}
