#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/connection_lifecycle.hpp"
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
    class PlayerActorBinding final : public snf::runtime::ActorBinding
    {
    public:
        explicit PlayerActorBinding(PlayerEffectSink& effects,
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

        // Shared tail of dispatch and resume: drive the handler's task and, once
        // it completes, apply its effects in order.
        [[nodiscard]] snf::runtime::ActorDispatchResult advance(PlayerActorSlot& slot,
                                                                std::stop_token stop_token);

        PlayerEffectSink& _effects;
        std::function<void(ProvisionalActorId, const PlayerCommand&)> _on_before_command;
    };
}
