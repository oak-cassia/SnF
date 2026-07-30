#include "snf/server/player_actor_binding.hpp"

#include "snf/server/player_actor.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    struct PlayerActorBinding::PlayerActorSlot final : snf::runtime::ActorSlot
    {
        PlayerActor actor;
    };

    struct PlayerActorBinding::CommandPayload
    {
        PlayerInboundCommand command;
    };

    struct PlayerActorBinding::ConnectionClosedPayload
    {
        ConnectionClosed closed;
    };

    PlayerActorBinding::PlayerActorBinding(PlayerEffectSink& effects,
                                           PlayerActorBindingConfig config)
        : _effects(effects)
        , _on_before_command(std::move(config.on_before_command))
    {
    }

    snf::runtime::ActorKind PlayerActorBinding::kind() const noexcept
    {
        return snf::runtime::ActorKind::ProvisionalPlayer;
    }

    snf::runtime::ActorSubmission
    PlayerActorBinding::makeCommand(PlayerInboundCommand command) const
    {
        const ProvisionalActorId actor = command.actor;
        return makeSubmission(
            snf::runtime::ActorKey{
                .kind = kind(),
                .entity = actor.value,
            },
            snf::runtime::ActorActivation::ActivateIfMissing,
            snf::runtime::ActorAccounting::Command,
            CommandPayload{
                .command = std::move(command),
            });
    }

    snf::runtime::ActorSubmission
    PlayerActorBinding::makeConnectionClosed(const ProvisionalActorId actor,
                                             ConnectionClosed closed) const
    {
        return makeSubmission(
            snf::runtime::ActorKey{
                .kind = kind(),
                .entity = actor.value,
            },
            snf::runtime::ActorActivation::ExistingOnly,
            snf::runtime::ActorAccounting::Control,
            ConnectionClosedPayload{
                .closed = std::move(closed),
            });
    }

    std::unique_ptr<snf::runtime::ActorSlot>
    PlayerActorBinding::activate(const snf::runtime::EntityId entity)
    {
        static_cast<void>(entity);
        return std::make_unique<PlayerActorSlot>();
    }

    snf::runtime::ActorDispatchResult
    PlayerActorBinding::dispatch(snf::runtime::ActorSlot& slot,
                                 const snf::runtime::ActorSubmission& submission,
                                 const std::stop_token stop_token)
    {
        auto& player_slot = dynamic_cast<PlayerActorSlot&>(slot);
        if (submission.accounting() == snf::runtime::ActorAccounting::Control)
        {
            static_cast<void>(payloadAs<ConnectionClosedPayload>(submission));
            return snf::runtime::ActorDispatchResult::Evict;
        }

        const CommandPayload& payload = payloadAs<CommandPayload>(submission);
        if (_on_before_command)
        {
            _on_before_command(payload.command.actor, payload.command.command);
        }

        PlayerResult result = player_slot.actor.handle(payload.command.command);
        if (_effects.apply(payload.command.connection, std::move(result), stop_token))
        {
            return snf::runtime::ActorDispatchResult::KeepActive;
        }

        if (stop_token.stop_requested())
        {
            return snf::runtime::ActorDispatchResult::Stopped;
        }

        throw std::runtime_error{"Player effect application failed while logic runtime was active"};
    }
}
