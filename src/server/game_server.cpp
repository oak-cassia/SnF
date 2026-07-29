#include "snf/server/game_server.hpp"

#include "snf/net/system_error.hpp"

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
        : _outbound_actions(config.outbound_queue_capacity)
        , _outbound_event(create_outbound_event())
        , _outbound_sink(_outbound_actions, _outbound_event.getDescriptor())
        , _player_effects(_outbound_sink)
        , _runtime_completion(runtimeMask(RuntimeId::Player), _outbound_event.getDescriptor())
        , _actor_runtime(
              [config]
              {
                  ActorRuntimeConfig runtime_config{RuntimeId::Player};
                  runtime_config.worker_count = config.actor_worker_count;
                  runtime_config.queue_capacity_per_worker = config.actor_queue_capacity_per_worker;
                  return runtime_config;
              }(),
              _player_effects,
              _runtime_completion)
        , _command_router(_actor_runtime)
        , _protocol_gateway(_command_router)
        , _tcp_server(
              TcpServerConfig{
                  .port = config.port,
                  .shutdown_grace_period = config.shutdown_grace_period,
                  .max_pending_send_bytes = config.max_pending_send_bytes,
                  .client_send_buffer_size = config.client_send_buffer_size,
                  .connection_lifecycle_capacity = config.connection_lifecycle_capacity,
              },
              _protocol_gateway,
              _outbound_actions,
              _runtime_completion,
              _outbound_event.getDescriptor())
    {
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

    ActorRuntimeStats GameServer::getActorRuntimeStats() const
    {
        return _actor_runtime.getStats();
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

        _actor_runtime.start();
    }

    void GameServer::joinActorRuntime()
    {
        _actor_runtime.join();
    }

    void GameServer::cancelActorRuntime() noexcept
    {
        _protocol_gateway.cancel();
        _outbound_actions.cancel();
    }
}
