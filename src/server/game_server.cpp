#include "snf/server/game_server.hpp"

#include "snf/net/system_error.hpp"

#include <algorithm>
#include <exception>
#include <limits>
#include <stdexcept>
#include <sys/eventfd.h>
#include <utility>
#include <variant>

namespace
{
    std::unique_ptr<snf::server::PlayerRepository>
    make_player_repository(const snf::server::GameServerConfig& config)
    {
        if (config.mysql_player_repository)
        {
            return std::make_unique<snf::server::MySqlPlayerRepository>(
                *config.mysql_player_repository);
        }
        return std::make_unique<snf::server::ThreadedPlayerRepository>(
            snf::server::ThreadedPlayerRepositoryConfig{
                .worker_count = config.player_repository_worker_count,
                .queue_capacity = config.player_repository_queue_capacity,
                .max_idempotency_records_per_player =
                    config.max_purchase_idempotency_records_per_player,
                .max_ranking_events = config.max_player_domain_events,
            });
    }

    const snf::server::PlayerRepositoryDiagnostics&
    repository_diagnostics(const snf::server::PlayerRepository& repository)
    {
        const auto* diagnostics =
            dynamic_cast<const snf::server::PlayerRepositoryDiagnostics*>(&repository);
        if (diagnostics == nullptr)
        {
            throw std::logic_error{"Configured Player repository has no diagnostics"};
        }
        return *diagnostics;
    }

    snf::server::RankingStore& ranking_store(snf::server::PlayerRepository& repository)
    {
        auto* store = dynamic_cast<snf::server::RankingStore*>(&repository);
        if (store == nullptr)
        {
            throw std::logic_error{"Configured Player repository has no ranking store"};
        }
        return *store;
    }

    snf::net::UniqueFileDescriptor create_outbound_event()
    {
        const int descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (descriptor == -1)
        {
            snf::net::throw_system_error("eventfd");
        }

        return snf::net::UniqueFileDescriptor{descriptor};
    }

    std::size_t
    checked_product(const std::size_t left, const std::size_t right, const char* const message)
    {
        if (left != 0 && right > std::numeric_limits<std::size_t>::max() / left)
        {
            throw std::invalid_argument{message};
        }

        return left * right;
    }

    std::size_t
    checked_sum(const std::size_t left, const std::size_t right, const char* const message)
    {
        if (right > std::numeric_limits<std::size_t>::max() - left)
        {
            throw std::invalid_argument{message};
        }

        return left + right;
    }

    std::size_t checked_party_members(const std::size_t max_members)
    {
        constexpr std::size_t FIXED_PARTY_RESPONSE_PAYLOAD_SIZE = 1 + 8 + 8 + 2;
        constexpr std::size_t MAX_MEMBERS_BY_PAYLOAD =
            (snf::protocol::MAX_PAYLOAD_SIZE - FIXED_PARTY_RESPONSE_PAYLOAD_SIZE) / 8;
        constexpr std::size_t MAX_WIRE_MEMBERS =
            std::min(MAX_MEMBERS_BY_PAYLOAD,
                     static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max()));
        if (max_members == 0 || max_members > MAX_WIRE_MEMBERS)
        {
            throw std::invalid_argument{"Party member capacity exceeds its wire response"};
        }
        return max_members;
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
                  const std::size_t total_in_flight_budget =
                      checked_product(config.actor_worker_count,
                                      config.actor_max_in_flight_operations_per_worker,
                                      "Actor in-flight operation budget exceeds size_t");
                  const std::size_t total_actor_outstanding_budget =
                      checked_product(config.actor_worker_count,
                                      config.actor_queue_capacity_per_worker,
                                      "Actor outstanding command budget exceeds size_t");

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
                      .max_waiters = total_in_flight_budget,
                      // A pending record can belong either to a current session or to a
                      // command that was already admitted when its session disappeared.
                      // Cover both populations; the channel still has a no-throw
                      // close-all fail-safe if this accounting invariant is ever broken.
                      .max_pending_admission_failures =
                          checked_sum(config.connection_lifecycle_capacity,
                                      total_actor_outstanding_budget,
                                      "Outbound admission failure budget exceeds size_t"),
                  };
              }(),
              _outbound_event.getDescriptor())
        , _player_effects(_outbound_channel)
        , _zone_results(_outbound_channel)
        , _party_results(_outbound_channel)
        , _party_coordinator(checked_party_members(config.max_party_members))
        , _player_repository(make_player_repository(config))
        , _ranking_projector(ranking_store(*_player_repository),
                             RepositoryRankingProjectorConfig{
                                 .batch_size = config.ranking_projector_batch_size,
                                 .checkpoint_every_events = config.ranking_checkpoint_every_events,
                                 .poll_interval = config.ranking_projector_poll_interval,
                             })
        , _runtime_completion(snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic),
                              _outbound_event.getDescriptor())
        , _player_actor_binding(_player_effects, _outbound_channel, _command_lifecycle)
        , _persistent_player_actor_binding(
              _player_effects,
              _outbound_channel,
              _command_lifecycle,
              PlayerActorBindingConfig{
                  .actor_kind = snf::runtime::ActorKind::Player,
                  .repository = _player_repository.get(),
                  .on_before_command = {},
                  .on_actor_deactivated =
                      [this](const PlayerActorId actor)
                  {
                      if (const auto player = actor.playerId())
                      {
                          _player_sessions.completePassivation(*player);
                      }
                  },
                  .on_record_loaded = [this](const snf::net::ConnectionId connection,
                                             std::optional<PlayerLocation> location)
                  { _player_sessions.noteLocation(connection, std::move(location)); },
              })
        , _zone_actor_binding(
              ZoneActorBindingConfig{
                  .actor = ZoneActorConfig{.aoi_radius = config.zone_aoi_radius},
                  .tick_budget = config.zone_tick_budget,
                  .on_result = [this](const ZoneInboundCommand& command, const ZoneResult& result)
                  { _zone_results.accept(command, result); },
              },
              _command_lifecycle)
        , _party_actor_binding(
              PartyActorBindingConfig{
                  .actor = PartyActorConfig{.max_members =
                                                checked_party_members(config.max_party_members)},
                  .on_result =
                      [this](const PartyInboundCommand& command, const PartyResult& result)
                  {
                      if (const auto* leave = std::get_if<LeavePartyCommand>(&command.command))
                      {
                          _party_coordinator.completeLeave(PartyRoute{
                              .connection = command.connection,
                              .player = leave->player,
                              .party = command.party,
                              .membership_epoch = leave->membership_epoch,
                              .leaving = true,
                          });
                      }
                      else if (result.status != PartyCommandStatus::Applied &&
                               result.status != PartyCommandStatus::AlreadyMember)
                      {
                          const auto& join = std::get<JoinPartyCommand>(command.command);
                          _party_coordinator.completeLeave(PartyRoute{
                              .connection = command.connection,
                              .player = join.player,
                              .party = command.party,
                              .membership_epoch = join.membership_epoch,
                              .leaving = false,
                          });
                      }
                      _party_results.accept(command, result);
                  },
              },
              _command_lifecycle)
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
        , _player_actor_ingress(_logic_runtime,
                                _player_actor_binding,
                                _persistent_player_actor_binding,
                                _command_lifecycle)
        , _zone_actor_ingress(_logic_runtime, _zone_actor_binding, _command_lifecycle)
        , _party_actor_ingress(_logic_runtime, _party_actor_binding, _command_lifecycle)
        , _zone_timer_clock()
        , _zone_timers(_zone_actor_ingress,
                       _zone_timer_clock,
                       ZoneTimerServiceConfig{
                           .tick_interval = config.zone_tick_interval,
                           .cancellation_retry_interval = std::chrono::milliseconds{1},
                           .max_timers = config.max_zone_timers,
                           .on_failure = [this]
                           { _runtime_completion.notifyFailed(snf::runtime::RuntimeId::Logic); },
                       })
        , _command_router(_player_actor_ingress, _zone_actor_ingress, _party_actor_ingress)
        , _protocol_gateway(_command_router,
                            _player_sessions,
                            _route_coordinator,
                            _zone_timers,
                            _party_coordinator)
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
        _logic_runtime.registerBinding(_persistent_player_actor_binding);
        _logic_runtime.registerBinding(_zone_actor_binding);
        _logic_runtime.registerBinding(_party_actor_binding);
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
        try
        {
            _zone_timers.stop();
        }
        catch (...)
        {
        }
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

    std::optional<PlayerRecord> GameServer::getPlayerRecord(const PlayerId player) const
    {
        return repository_diagnostics(*_player_repository).find(player);
    }

    ZoneTimerServiceStats GameServer::getZoneTimerStats() const
    {
        return _zone_timers.stats();
    }

    ZoneActorBindingStats GameServer::getZoneActorStats() const noexcept
    {
        return _zone_actor_binding.stats();
    }

    PartyActorBindingStats GameServer::getPartyActorStats() const noexcept
    {
        return _party_actor_binding.stats();
    }

    RankingPipelineStats GameServer::getRankingStats() const
    {
        return _ranking_projector.stats();
    }

    std::vector<RankingEntry> GameServer::getRankingStandings() const
    {
        return _ranking_projector.standings();
    }

    ServerMetricsSnapshot GameServer::getMetricsSnapshot() const
    {
        return ServerMetricsSnapshot{
            .counters = _tcp_server.getStats(),
            .network = _tcp_server.getMetrics(),
            .actor_runtime = _logic_runtime.getStats(),
            .player_repository = repository_diagnostics(*_player_repository).stats(),
            .zone_timers = _zone_timers.stats(),
            .zone_actors = _zone_actor_binding.stats(),
            .party_actors = _party_actor_binding.stats(),
            .ranking_projection = _ranking_projector.stats(),
            .command_terminals = _command_lifecycle.terminalCount(),
            .command_admission_rejections = _command_lifecycle.admissionRejectionCount(),
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
        _zone_timers.start();
    }

    void GameServer::joinActorRuntime()
    {
        _zone_timers.stop();
        std::exception_ptr actor_failure;
        try
        {
            _logic_runtime.join();
        }
        catch (...)
        {
            actor_failure = std::current_exception();
        }

        // No transaction can commit after the Actor runtime drains. Stop then
        // performs one final durable-tail replay and checkpoint before run()
        // returns, so post-run metrics and standings are authoritative.
        _ranking_projector.stop();
        if (actor_failure)
        {
            std::rethrow_exception(actor_failure);
        }
        _zone_timers.rethrowIfFailed();
    }

    void GameServer::cancelActorRuntime() noexcept
    {
        try
        {
            _zone_timers.stop();
        }
        catch (...)
        {
        }
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
