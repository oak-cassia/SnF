#include "snf/server/player_actor_binding.hpp"

#include "snf/server/player_actor.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    struct PlayerActorBinding::PlayerActorSlot final : snf::runtime::ActorSlot
    {
        PlayerActor actor;
        // Holds the handler's task while it runs, including across a suspension.
        // Keeping the frame in the slot is what confines resume and destruction to
        // the owning Worker.
        snf::runtime::ActorTask<PlayerResult> task;
        // Captured at dispatch because resume() does not carry the submission, and
        // effects still have to reach the connection that issued the command.
        snf::net::ConnectionId connection{};
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
                                 snf::runtime::ActorContext& context,
                                 const std::stop_token stop_token)
    {
        // PlayerActor awaits nothing yet, so it needs no context. The parameter is
        // the seam a persistence await will use without touching the scheduler.
        static_cast<void>(context);

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

        player_slot.connection = payload.command.connection;
        player_slot.task = player_slot.actor.handle(payload.command.command);
        return advance(player_slot, stop_token);
    }

    snf::runtime::ActorDispatchResult
    PlayerActorBinding::resume(snf::runtime::ActorSlot& slot,
                               snf::runtime::ActorContext& context,
                               const std::stop_token stop_token)
    {
        static_cast<void>(context);

        return advance(dynamic_cast<PlayerActorSlot&>(slot), stop_token);
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::advance(PlayerActorSlot& slot,
                                                                  const std::stop_token stop_token)
    {
        if (slot.task.resume() == snf::runtime::ActorTaskStatus::Suspended)
        {
            return snf::runtime::ActorDispatchResult::Suspended;
        }

        // Effects are applied only after the handler has returned normally, which
        // is the same ordering the synchronous handler had. takeResult rethrows a
        // handler exception, and the frame is then destroyed with the slot on this
        // same Worker.
        PlayerResult result = slot.task.takeResult();
        slot.task = {};

        if (_effects.apply(slot.connection, std::move(result), stop_token))
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
