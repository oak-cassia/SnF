#pragma once

#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/command_router.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/outbound_channel.hpp"
#include "snf/server/party_actor_binding.hpp"
#include "snf/server/party_actor_ingress.hpp"
#include "snf/server/party_coordinator.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_actor_ingress.hpp"
#include "snf/server/player_persistence_service.hpp"
#include "snf/server/player_repository.hpp"
#include "snf/server/player_session_directory.hpp"
#include "snf/server/protocol_gateway.hpp"
#include "snf/server/protocol_party_result_sink.hpp"
#include "snf/server/protocol_player_response_sink.hpp"
#include "snf/server/protocol_room_result_sink.hpp"
#include "snf/server/protocol_zone_result_sink.hpp"
#include "snf/server/room_actor_binding.hpp"
#include "snf/server/room_actor_ingress.hpp"
#include "snf/server/room_entry_service.hpp"
#include "snf/server/room_transition_channel.hpp"
#include "snf/server/route_coordinator.hpp"
#include "snf/server/tcp_server.hpp"
#include "snf/server/zone_actor_binding.hpp"
#include "snf/server/zone_actor_ingress.hpp"
#include "snf/server/zone_handoff_service.hpp"
#include "snf/server/zone_transition_channel.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <vector>

namespace snf::server
{
    using GameServerStats = TcpServerStats;

    // The baseline observation surface for the backpressure contract: reactor
    // saturation, per-connection send buildup and Logic Worker queue wait in one
    // consistent read.
    struct ServerMetricsSnapshot
    {
        GameServerStats counters;
        TcpServerMetrics network;
        snf::runtime::ActorRuntimeStats actor_runtime;
        PlayerRepositoryStats player_repository;
        ZoneActorBindingStats zone_actors;
        RoomActorBindingStats room_actors;
        ProtocolRoomResultSinkStats room_protocol;
        RouteCoordinatorStats zone_handoffs;
        ZoneHandoffStats zone_handoffs_saga;
        ZoneTransitionChannelStats zone_transition_channel;
        RoomEntryStats room_entries;
        RoomTransitionChannelStats room_transition_channel;
        PartyActorBindingStats party_actors;
        // Commands that were admitted and reached a final result, counted once each
        // whether or not they answered. If playable slow-command measurements justify
        // per-connection credit, its owner consumes this same terminal signal.
        std::uint64_t command_terminals{0};
        // Posts the runtime refused. A different fact with a different cause, so it is
        // never folded into the count above.
        std::uint64_t command_admission_rejections{0};
        PlayerPersistenceServiceStats player_persistence;
    };

    struct GameServerConfig
    {
        std::uint16_t port{7777};
        std::chrono::milliseconds shutdown_grace_period{5000};
        std::size_t max_pending_send_bytes{snf::net::MAX_PENDING_SEND_BYTES};
        std::optional<int> client_send_buffer_size{};
        std::size_t actor_worker_count{2};
        std::size_t actor_queue_capacity_per_worker{4096};
        // Also the continuation queue capacity per Worker, and the bound the outbound
        // waiter registry is sized against: a Worker holds one of these while it waits
        // for capacity, so registration must never be the tighter limit.
        std::size_t actor_max_in_flight_operations_per_worker{1024};
        std::size_t max_purchase_idempotency_records_per_player{1024};
        // Empty keeps the deterministic in-memory adapter. A value selects the
        // durable MySQL adapter and its own bounded Worker/queue configuration.
        // Supplies the repository when the caller wants one other than the default
        // in-memory adapter. It is a factory rather than a config so that choosing a
        // backend -- and linking it -- stays with whoever runs the server.
        std::function<std::unique_ptr<PlayerRepository>()> player_repository_factory{};
        std::size_t max_party_members{8};
        std::int32_t zone_aoi_radius{1000};
        std::chrono::milliseconds zone_tick_interval{50};
        std::chrono::nanoseconds zone_tick_budget{std::chrono::milliseconds{5}};
        std::size_t max_zone_handoffs{4096};
        std::chrono::milliseconds room_battle_duration{90000};
        std::size_t max_room_participants{4};
        std::uint64_t room_clear_experience{300};
        std::uint64_t room_boss_health{1000};
        std::chrono::milliseconds room_tick_interval{100};
        std::chrono::nanoseconds room_tick_budget{std::chrono::milliseconds{5}};
        std::chrono::milliseconds room_wave_interval{20000};
        std::size_t room_wave_count{2};
        std::size_t room_minions_per_wave{10};
        std::uint64_t room_minion_health{30};
        std::chrono::milliseconds room_boss_spawn_after{40000};
        std::size_t max_room_spawned_enemies{64};
        std::size_t room_digest_flush_threshold{512};
        std::uint32_t room_arena_width{100};
        std::uint32_t room_arena_height{100};
        std::uint32_t room_player_move_speed{4};
        std::uint32_t room_participant_spawn_spacing{4};
        std::uint32_t room_minion_spawn_radius{25};
        std::uint32_t room_minion_move_speed{2};
        std::uint32_t room_boss_move_speed{1};
        std::uint64_t room_minion_attack_damage{3};
        std::uint64_t room_boss_attack_damage{10};
        std::uint32_t room_minion_attack_range{3};
        std::uint32_t room_boss_attack_range{5};
        std::chrono::milliseconds room_minion_attack_cooldown{1000};
        std::chrono::milliseconds room_boss_attack_cooldown{2000};
        std::size_t max_room_entries{4096};
        std::size_t max_room_entry_completions_per_turn{64};
        std::size_t max_zone_handoff_completions_per_turn{64};
        std::size_t outbound_queue_capacity{4096};
        // Bounds the shared outbound capacity one connection may hold at once.
        std::size_t max_outbound_slots_per_connection{64};
        // Bounds the waiters a single reactor turn examines and grants.
        std::size_t outbound_grants_per_turn{64};
        std::size_t connection_lifecycle_capacity{4096};
        // Periodic exposure while the server runs. Zero reports nothing, leaving
        // only the caller's own snapshot after run() returns.
        std::chrono::milliseconds metrics_report_interval{0};
        // Called on the reactor thread, so it must not block. Anything slower than
        // a local formatting pass belongs on a separate bounded logger queue that
        // the reporter only posts to. Its cost is not part of
        // TcpServerMetrics::reactor_turn_nanoseconds.
        std::function<void(const ServerMetricsSnapshot&)> metrics_reporter{};
        std::size_t player_persistence_queue_capacity{4096};
        std::chrono::milliseconds player_persistence_flush_interval{100};
    };

    // Composes the reactor, Logic ActorRuntime, Player binding, outbound hand-off
    // queue and eventfd wake-up.
    class GameServer
    {
    public:
        explicit GameServer(const GameServerConfig& config);
        explicit GameServer(std::uint16_t port, std::chrono::milliseconds shutdown_grace_period = std::chrono::milliseconds{5000});
        ~GameServer();

        GameServer(const GameServer&) = delete;
        GameServer& operator=(const GameServer&) = delete;

        GameServer(GameServer&&) = delete;
        GameServer& operator=(GameServer&&) = delete;

        [[nodiscard]] std::uint16_t getPort() const noexcept;
        [[nodiscard]] const GameServerStats& getStats() const noexcept;
        [[nodiscard]] snf::runtime::ActorRuntimeStats getActorRuntimeStats() const;
        [[nodiscard]] std::optional<PlayerRecord> getPlayerRecord(PlayerId player) const;
        [[nodiscard]] ZoneActorBindingStats getZoneActorStats() const noexcept;
        [[nodiscard]] RoomActorBindingStats getRoomActorStats() const noexcept;
        [[nodiscard]] PartyActorBindingStats getPartyActorStats() const noexcept;
        // Reads reactor state, so it belongs to the reactor thread: call it from
        // metrics_reporter or after run() has returned.
        [[nodiscard]] ServerMetricsSnapshot getMetricsSnapshot() const;

        void run(int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD);
        void requestStop() const noexcept;

    private:
        void startActorRuntime();
        void joinActorRuntime();
        void cancelActorRuntime() noexcept;
        void publishMetrics() const;

        std::function<void(const ServerMetricsSnapshot&)> _metrics_reporter;
        // The channel signals this descriptor, so it has to outlive the channel.
        snf::net::UniqueFileDescriptor _outbound_event;
        // The domain side takes it as an OutboundSink&, so the binding and the follow-up
        // sink still cannot reach the reactor-only half.
        OutboundChannel _outbound_channel;
        ZoneTransitionChannel _zone_transition_channel;
        RoomTransitionChannel _room_transition_channel;
        ProtocolPlayerResponseSink _player_responses;
        ProtocolZoneResultSink _zone_results;
        ProtocolPartyResultSink _party_results;
        CountingCommandLifecycleSink _command_lifecycle;
        PlayerSessionDirectory _player_sessions;
        RouteCoordinator _route_coordinator;
        PartyCoordinator _party_coordinator;
        std::unique_ptr<PlayerRepository> _player_repository;
        PlayerPersistenceService _player_persistence_service;
        snf::runtime::RuntimeCompletionCoordinator _runtime_completion;
        // Bindings must outlive the generic runtime: the worker owns the
        // wrapper destruction, while this object owns Player dependencies.
        PlayerActorBinding _player_actor_binding;
        PlayerActorBinding _persistent_player_actor_binding;
        ZoneActorBinding _zone_actor_binding;
        PartyActorBinding _party_actor_binding;
        // Declared here rather than beside the other sinks: it reads the session
        // directory, so it has to be constructed after one.
        ProtocolRoomResultSink _room_result_sink;
        RoomActorBinding _room_actor_binding;
        snf::runtime::ActorRuntime _logic_runtime;
        PlayerActorIngress _player_actor_ingress;
        ZoneActorIngress _zone_actor_ingress;
        PartyActorIngress _party_actor_ingress;
        RoomActorIngress _room_actor_ingress;
        CommandRouter _command_router;
        ZoneHandoffService _zone_handoff_service;
        RoomEntryService _room_entry_service;
        ProtocolGateway _protocol_gateway;
        TcpServer _tcp_server;
        bool _has_run{false};
    };
}
