#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/connection_lifecycle.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_effect_sink.hpp"
#include "snf/server/player_inbound_command.hpp"

#include <functional>

namespace snf::server
{
    struct PlayerActorBindingConfig
    {
        // Test-only diagnostic hook. It executes on the Player actor's owner
        // Worker immediately before PlayerActor::handle().
        std::function<void(ProvisionalActorId, const PlayerCommand&)> on_before_command;
    };

    // Owns Player-specific type erasure at the edge of the generic scheduler.
    // Its factories are the only way Player inputs become ActorSubmission values.
    //
    // It also owns the outbound reservation, in two stages: the handler decides, and
    // only then does the binding acquire the capacity to emit. That keeps PlayerActor
    // unaware that outbound capacity is finite while still emitting effects strictly
    // after the handler has returned.
    class PlayerActorBinding final : public snf::runtime::ActorBinding
    {
    public:
        PlayerActorBinding(PlayerEffectSink& effects,
                           OutboundSink& outbound,
                           CommandTerminalSink& terminals,
                           PlayerActorBindingConfig config = {});

        [[nodiscard]] snf::runtime::ActorKind kind() const noexcept override;
        [[nodiscard]] snf::runtime::ActorSubmission makeCommand(PlayerInboundCommand command) const;
        [[nodiscard]] snf::runtime::ActorSubmission
        makeConnectionClosed(ProvisionalActorId actor, ConnectionClosed closed) const;

    protected:
        [[nodiscard]] std::unique_ptr<snf::runtime::ActorSlot>
        activate(snf::runtime::EntityId entity) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult
        dispatch(snf::runtime::ActorSlot& slot,
                 const snf::runtime::ActorSubmission& submission,
                 snf::runtime::ActorContext& context,
                 std::stop_token stop_token) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult resume(snf::runtime::ActorSlot& slot,
                                                               snf::runtime::ActorContext& context,
                                                               std::stop_token stop_token) override;

    private:
        struct PlayerActorSlot;
        struct CommandPayload;
        struct ConnectionClosedPayload;

        // Shared tail of dispatch and resume: drive whichever stage the command is in,
        // and emit once its capacity is in hand.
        [[nodiscard]] snf::runtime::ActorDispatchResult advance(PlayerActorSlot& slot,
                                                                snf::runtime::ActorContext& context,
                                                                std::stop_token stop_token);
        [[nodiscard]] snf::runtime::ActorDispatchResult
        emit(PlayerActorSlot& slot, OutboundReservation& reservation, std::stop_token stop_token);
        // Ends a command that could not acquire capacity at all. The connection is
        // closed by the backend, so the command itself ends normally.
        [[nodiscard]] snf::runtime::ActorDispatchResult
        abandonEmission(PlayerActorSlot& slot) noexcept;
        static void resetPendingCommand(PlayerActorSlot& slot) noexcept;

        PlayerEffectSink& _effects;
        OutboundSink& _outbound;
        CommandTerminalSink& _terminals;
        std::function<void(ProvisionalActorId, const PlayerCommand&)> _on_before_command;
    };
}
