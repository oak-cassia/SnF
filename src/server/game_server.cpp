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
        : _inbound_commands(config.inbound_queue_capacity)
        , _network_actions(config.outbound_queue_capacity)
        , _outbound_event(create_outbound_event())
        , _tcp_server(TcpServerConfig{
              .port = config.port,
              .shutdown_grace_period = config.shutdown_grace_period,
              .max_pending_send_bytes = config.max_pending_send_bytes,
              .client_send_buffer_size = config.client_send_buffer_size,
          },
          _inbound_commands,
          _network_actions,
          _outbound_event.getDescriptor())
        , _game_runtime(_inbound_commands,
                        _network_actions,
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
              .inbound_queue_capacity = 4096,
              .outbound_queue_capacity = 4096,
          })
    {
    }

    GameServer::~GameServer()
    {
        requestStop();
        cancelGameRuntime();
        joinGameRuntime();
    }

    std::uint16_t GameServer::getPort() const noexcept
    {
        return _tcp_server.getPort();
    }

    const GameServerStats& GameServer::getStats() const noexcept
    {
        return _tcp_server.getStats();
    }

    void GameServer::run(const int termination_signal_descriptor)
    {
        startGameRuntime();

        try
        {
            _tcp_server.run(termination_signal_descriptor);
        }
        catch (...)
        {
            cancelGameRuntime();
            joinGameRuntime();
            throw;
        }

        joinGameRuntime();

        if (_game_runtime_error)
        {
            std::rethrow_exception(_game_runtime_error);
        }
    }

    void GameServer::requestStop() const noexcept
    {
        _tcp_server.requestStop();
    }

    void GameServer::startGameRuntime()
    {
        if (_has_run)
        {
            throw std::logic_error{"GameServer::run may only be called once"};
        }

        _has_run = true;

        _game_worker = std::jthread{
            [this]
            {
                try
                {
                    _game_runtime.run();
                }
                catch (...)
                {
                    _game_runtime_error = std::current_exception();
                    _inbound_commands.cancel();
                    _network_actions.cancel();
                    requestStop();
                }
            }};
    }

    void GameServer::joinGameRuntime()
    {
        if (_game_worker.joinable())
        {
            _game_worker.join();
        }
    }

    void GameServer::cancelGameRuntime() noexcept
    {
        _inbound_commands.cancel();
        _network_actions.cancel();
    }
}
