#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/connection_lifecycle.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_actor.hpp"
#include "snf/server/player_actor_id.hpp"
#include "snf/server/player_inbound_command.hpp"
#include "snf/server/player_persistence_service.hpp"
#include "snf/server/player_repository.hpp"
#include "snf/server/player_response_sink.hpp"

#include <functional>
#include <memory>
#include <optional>

namespace snf::server
{
    struct PlayerActorBindingConfig
    {
        snf::runtime::ActorKind actor_kind{snf::runtime::ActorKind::ProvisionalPlayer};
        PlayerRepository* repository{nullptr};
        // Test-only diagnostic hook. It executes on the Player actor's owner
        // Worker immediately before PlayerActor::handle().
        std::function<void(PlayerActorId, const PlayerCommand&)> on_before_command;
        // Runs after the scheduler has removed and destroyed a persistent Player
        // slot. GameServer uses it to finish the Closing -> detached transition.
        std::function<void(PlayerActorId)> on_actor_deactivated;
        // Runs on the owning Worker after a persistent record has loaded and before
        // its first response is emitted. Session routing uses the immutable location
        // value to restore a Zone entry without reading Actor state cross-thread.
        std::function<void(snf::net::ConnectionId, std::optional<PlayerLocation>)> on_record_loaded;
        // Production persistent bindings route every snapshot save through this
        // service. When omitted, a standalone persistent binding creates an owned
        // service so the repository is never called directly by the Actor binding.
        PlayerPersistenceService* persistence_service{nullptr};
        std::size_t max_purchase_idempotency_records_per_player{DEFAULT_PURCHASE_IDEMPOTENCY_CAPACITY};
    };

    // Owns Player-specific type erasure at the edge of the generic scheduler.
    // Its factories are the only way Player inputs become ActorSubmission values.
    //
    // It also owns the outbound reservation, in two stages: the handler decides, and
    // only then does the binding acquire the capacity to emit. That keeps PlayerActor
    // unaware that outbound capacity is finite while still applying follow-ups strictly
    // after the handler has returned.
    class PlayerActorBinding final : public snf::runtime::ActorBinding
    {
    public:
        PlayerActorBinding(PlayerResponseSink& response_sink, OutboundSink& outbound, CommandLifecycleSink& lifecycle, PlayerActorBindingConfig config = {});

        [[nodiscard]] snf::runtime::ActorKind kind() const noexcept override;
        [[nodiscard]] snf::runtime::ActorSubmission makeCommand(PlayerInboundCommand command) const;
        [[nodiscard]] snf::runtime::ActorSubmission makeConnectionClosed(PlayerActorId actor, ConnectionClosed closed) const;

    protected:
        [[nodiscard]] std::unique_ptr<snf::runtime::ActorSlot> activate(snf::runtime::EntityId entity) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult
        dispatch(snf::runtime::ActorSlot& slot, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, std::stop_token stop_token) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult resume(snf::runtime::ActorSlot& slot, snf::runtime::ActorContext& context, std::stop_token stop_token) override;
        // Called on the sending actor's Worker, so this stays a read-only transform:
        // no cache, no counter, nothing another Worker could race with.
        [[nodiscard]] std::optional<snf::runtime::ActorSubmission> makeTell(snf::runtime::ActorKey target, snf::runtime::TellPayload payload) override;

    private:
        struct PlayerActorSlot;
        struct CommandPayload;
        struct ConnectionClosedPayload;
        struct StreetExperienceGrantPayload;

        // Shared tail of dispatch and resume: drive whichever stage the command is in,
        // and apply its follow-ups once the capacity is in hand.
        [[nodiscard]] snf::runtime::ActorDispatchResult advance(PlayerActorSlot& slot, snf::runtime::ActorContext& context, std::stop_token stop_token);
        [[nodiscard]] snf::runtime::ActorDispatchResult applyResponses(PlayerActorSlot& slot, OutboundReservation& reservation, std::stop_token stop_token);
        void publishDirtySnapshot(PlayerActorSlot& slot) noexcept;
        // Ends a command that could not acquire capacity at all, either because none
        // could be awaited or because the result asks for more than one connection may
        // ever hold. The connection is closed by the backend, so the command itself ends
        // normally.
        [[nodiscard]] snf::runtime::ActorDispatchResult abandonResponses(PlayerActorSlot& slot) noexcept;
        static void resetPendingCommand(PlayerActorSlot& slot) noexcept;

        PlayerResponseSink& _response_sink;
        OutboundSink& _outbound;
        CommandLifecycleSink& _lifecycle;
        snf::runtime::ActorKind _kind;
        PlayerRepository* _repository;
        std::function<void(PlayerActorId, const PlayerCommand&)> _on_before_command;
        std::function<void(PlayerActorId)> _on_actor_deactivated;
        std::function<void(snf::net::ConnectionId, std::optional<PlayerLocation>)> _on_record_loaded;
        std::unique_ptr<PlayerPersistenceService> _owned_persistence_service;
        PlayerPersistenceService* _persistence_service;
        std::size_t _max_purchase_idempotency_records_per_player;
    };
}
