#pragma once

#include "snf/game/player.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/actor_task.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/connection_lifecycle.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_actor_id.hpp"
#include "snf/server/player_inbound_command.hpp"
#include "snf/server/player_persistence_service.hpp"
#include "snf/server/player_repository.hpp"
#include "snf/server/player_response_sink.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

namespace snf::server
{
    struct PlayerActorBindingConfig
    {
        snf::runtime::ActorKind actor_kind{snf::runtime::ActorKind::ProvisionalPlayer};
        PlayerRepository* repository{nullptr};
        std::function<void(PlayerActorId, const PlayerCommand&)> on_before_command{};
        std::function<void(PlayerActorId, std::optional<snf::net::ConnectionId>)> on_actor_deactivated{};
        std::function<void(snf::net::ConnectionId, std::optional<PlayerLocation>)> on_record_loaded{};
        std::function<void(const RoomEntryContext&, RoomId)> on_room_join_undelivered{};
        PlayerPersistenceService* persistence_service{nullptr};
        std::size_t max_purchase_idempotency_records_per_player{DEFAULT_PURCHASE_IDEMPOTENCY_CAPACITY};
        std::chrono::milliseconds snapshot_retry_delay{1000};
        int snapshot_retry_limit{5};
    };

    struct PlayerActorBindingStats
    {
        std::uint64_t reward_snapshot_admission_rejections{0};
        std::uint64_t reward_snapshot_retry_giveups{0};
        std::uint64_t grant_load_failures{0};
    };

    class PlayerActorBinding final : public snf::runtime::ActorBinding
    {
    public:
        PlayerActorBinding(PlayerResponseSink& response_sink, OutboundSink& outbound, CommandLifecycleSink& lifecycle, PlayerActorBindingConfig config = {});

        [[nodiscard]] snf::runtime::ActorKind kind() const noexcept override;
        [[nodiscard]] snf::runtime::ActorSubmission makeCommand(PlayerInboundCommand command) const;
        [[nodiscard]] snf::runtime::ActorSubmission makeConnectionClosed(PlayerActorId actor, ConnectionClosed closed) const;
        [[nodiscard]] PlayerActorBindingStats stats() const noexcept;

    protected:
        [[nodiscard]] std::unique_ptr<snf::runtime::ActorState> activate(snf::runtime::EntityId entity) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult
        dispatch(snf::runtime::ActorState& state, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, std::stop_token stop_token) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult resume(snf::runtime::ActorState& state, snf::runtime::ActorContext& context, std::stop_token stop_token) override;
        [[nodiscard]] std::optional<snf::runtime::ActorSubmission> makeTell(snf::runtime::ActorKey target, snf::runtime::TellPayload payload) override;

    private:
        struct PlayerActorState;
        struct CommandPayload;
        struct ConnectionClosedPayload;
        struct StreetExperienceGrantPayload;
        struct SnapshotRetryPayload;

        enum class SnapshotPublishOutcome
        {
            CleanOrAccepted,
            RetryScheduled,
            GaveUp,
        };

        [[nodiscard]] snf::runtime::ActorDispatchResult advance(PlayerActorState& state, snf::runtime::ActorContext& context, std::stop_token stop_token);
        [[nodiscard]] snf::runtime::ActorDispatchResult applyResponses(PlayerActorState& state, OutboundReservation& reservation, std::stop_token stop_token);
        void runHandler(PlayerActorState& state, const PlayerCommand& command, snf::runtime::ActorContext& context);
        [[nodiscard]] SnapshotPublishOutcome
        publishDirtySnapshot(PlayerActorState& state, snf::runtime::ActorContext& context, bool retry_attempt) noexcept;
        [[nodiscard]] bool tryPublishDirtySnapshot(PlayerActorState& state) noexcept;
        [[nodiscard]] bool tryScheduleSnapshotRetry(PlayerActorState& state, snf::runtime::ActorContext& context) noexcept;
        [[nodiscard]] static snf::runtime::ActorDispatchResult snapshotTerminalResult(const PlayerActorState& state) noexcept;
        [[nodiscard]] snf::runtime::ActorDispatchResult abandonResponses(PlayerActorState& state) noexcept;
        static void resetPendingCommand(PlayerActorState& state) noexcept;

        PlayerResponseSink& _response_sink;
        OutboundSink& _outbound;
        CommandLifecycleSink& _lifecycle;
        snf::runtime::ActorKind _kind;
        PlayerRepository* _repository;
        std::function<void(PlayerActorId, const PlayerCommand&)> _on_before_command;
        std::function<void(PlayerActorId, std::optional<snf::net::ConnectionId>)> _on_actor_deactivated;
        std::function<void(snf::net::ConnectionId, std::optional<PlayerLocation>)> _on_record_loaded;
        std::function<void(const RoomEntryContext&, RoomId)> _on_room_join_undelivered;
        std::unique_ptr<PlayerPersistenceService> _owned_persistence_service;
        PlayerPersistenceService* _persistence_service;
        std::size_t _max_purchase_idempotency_records_per_player;
        std::chrono::milliseconds _snapshot_retry_delay;
        int _snapshot_retry_limit;
        std::atomic<std::uint64_t> _reward_snapshot_admission_rejections{0};
        std::atomic<std::uint64_t> _reward_snapshot_retry_giveups{0};
        std::atomic<std::uint64_t> _grant_load_failures{0};
    };
}
