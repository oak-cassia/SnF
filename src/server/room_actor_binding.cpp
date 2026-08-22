#include "snf/server/room_actor_binding.hpp"

#include "snf/runtime/tell_payload.hpp"

#include <chrono>
#include <stdexcept>
#include <utility>

namespace snf::server
{
    struct RoomActorBinding::RoomActorState final : snf::runtime::ActorState
    {
        RoomActorState(const RoomId room, const RoomConfig config)
            : room(room, config)
        {
        }

        Room room;
    };

    struct RoomActorBinding::CommandPayload
    {
        RoomInboundCommand command;
        CommandReleaseToken release;
    };

    struct RoomActorBinding::PassivatePayload
    {
    };

    RoomActorBinding::RoomActorBinding(RoomActorBindingConfig config)
        : _actor_config(config.actor)
        , _on_result(std::move(config.on_result))
    {
    }

    RoomActorBinding::RoomActorBinding(RoomActorBindingConfig config, CommandLifecycleSink& lifecycle)
        : _actor_config(config.actor)
        , _on_result(std::move(config.on_result))
        , _lifecycle(&lifecycle)
    {
    }

    snf::runtime::ActorKind RoomActorBinding::kind() const noexcept
    {
        return snf::runtime::ActorKind::Room;
    }

    RoomActorBindingStats RoomActorBinding::stats() const noexcept
    {
        return RoomActorBindingStats{
            .command_execution_nanoseconds = _command_execution_nanoseconds.snapshot(),
        };
    }

    snf::runtime::ActorSubmission RoomActorBinding::makeCommand(RoomInboundCommand command) const
    {
        if (command.room.value == 0)
        {
            throw std::invalid_argument{"Room command target must be non-zero"};
        }

        const RoomId room = command.room;
        CommandReleaseToken release;
        if (command.reply)
        {
            if (_lifecycle == nullptr)
            {
                throw std::logic_error{"Replying Room command requires a lifecycle sink"};
            }
            release = CommandReleaseToken{*_lifecycle, command.reply->connection};
        }
        return makeSubmission(
            snf::runtime::ActorKey{
                .kind = kind(),
                .entity = room.value,
            },
            snf::runtime::ActorActivation::ActivateIfMissing,
            snf::runtime::ActorAccounting::Command,
            CommandPayload{
                .command = std::move(command),
                .release = std::move(release),
            }
        );
    }

    snf::runtime::ActorSubmission RoomActorBinding::makePassivate(const RoomId room) const
    {
        if (room.value == 0)
        {
            throw std::invalid_argument{"Room passivation target must be non-zero"};
        }

        return makeSubmission(
            snf::runtime::ActorKey{
                .kind = kind(),
                .entity = room.value,
            },
            snf::runtime::ActorActivation::ExistingOnly,
            snf::runtime::ActorAccounting::Control,
            PassivatePayload{}
        );
    }

    std::optional<snf::runtime::ActorSubmission> RoomActorBinding::makeTell(const snf::runtime::ActorKey target, snf::runtime::TellPayload payload)
    {
        auto join = payload.take<RoomJoinTell>();
        if (!join)
        {
            // A refused take leaves the carrier intact, and the runtime reports the
            // mismatch as the wiring bug it is rather than inventing a command.
            return std::nullopt;
        }
        if (join->request.room.value != target.entity)
        {
            // Only a routing bug puts these out of step, and entering the wrong Room
            // is worse than failing loudly.
            return std::nullopt;
        }
        CommandReleaseToken release;
        if (!join->entry)
        {
            if (_lifecycle == nullptr)
            {
                throw std::logic_error{"Replying Room join requires a lifecycle sink"};
            }
            release = CommandReleaseToken{*_lifecycle, join->reply.connection};
        }

        return makeSubmission(
            target,
            snf::runtime::ActorActivation::ActivateIfMissing,
            snf::runtime::ActorAccounting::Command,
            CommandPayload{
                .command =
                    RoomInboundCommand{
                        .room = join->request.room,
                        .command =
                            JoinRoom{
                                .player = join->player,
                                .stats = join->request.stats,
                            },
                        .reply = join->entry ? std::nullopt : std::optional{join->reply},
                        .entry = join->entry,
                    },
                .release = std::move(release),
            }
        );
    }

    std::unique_ptr<snf::runtime::ActorState> RoomActorBinding::activate(const snf::runtime::EntityId entity)
    {
        return std::make_unique<RoomActorState>(RoomId{.value = entity}, _actor_config);
    }

    snf::runtime::ActorDispatchResult RoomActorBinding::dispatch(
        snf::runtime::ActorState& state,
        const snf::runtime::ActorSubmission& submission,
        snf::runtime::ActorContext& context,
        const std::stop_token stop_token
    )
    {
        static_cast<void>(stop_token);
        if (submission.accounting() == snf::runtime::ActorAccounting::Control)
        {
            static_cast<void>(payloadAs<PassivatePayload>(submission));
            return snf::runtime::ActorDispatchResult::Evict;
        }

        auto& room_state = dynamic_cast<RoomActorState&>(state);
        const CommandPayload& payload = payloadAs<CommandPayload>(submission);
        const auto started_at = std::chrono::steady_clock::now();
        // observedAt, not the reading above: the Room derives cooldowns from when its
        // turn began, while the reading here exists to measure how long the turn took.
        RoomResult result = room_state.room.handle(payload.command.command, context.observedAt());
        _command_execution_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_at));
        if (_on_result)
        {
            _on_result(payload.command, result);
        }

        // The Room decided in game terms; naming a mailbox and a timer is this
        // binding's job.
        if (result.deadline_after)
        {
            RoomInboundCommand deadline_command{
                .room = payload.command.room,
                .command = BattleDeadline{},
                .reply = std::nullopt,
            };
            // ExistingOnly: if the Room is gone there is nobody left to fail the
            // battle for, so the deadline must not resurrect it. A Running Room stays
            // resident, which is what keeps this deliverable.
            auto timer_submission = makeSubmission(
                submission.target(),
                snf::runtime::ActorActivation::ExistingOnly,
                snf::runtime::ActorAccounting::Command,
                CommandPayload{
                    .command = std::move(deadline_command),
                    .release = {},
                }
            );
            static_cast<void>(context.trySchedule(*result.deadline_after, std::move(timer_submission)));
        }

        for (const StreetExperienceGrant& grant : result.grants)
        {
            // Best effort by design. A full target mailbox drops the reward rather
            // than blocking the Room, and there is no reply channel to report it on.
            static_cast<void>(context.tryTell(
                snf::runtime::ActorKey{
                    .kind = snf::runtime::ActorKind::Player,
                    .entity = grant.player.value,
                },
                snf::runtime::TellPayload::of(grant)
            ));
        }

        // A decided Room has emitted whatever it owed and has nothing left to do, and
        // a Room nobody joined was activated by a stray command. A Running Room must
        // stay resident so its deadline timer can land.
        const RoomPhase phase = room_state.room.phase();
        if (phase == RoomPhase::Cleared || phase == RoomPhase::Failed || room_state.room.participantCount() == 0)
        {
            return snf::runtime::ActorDispatchResult::PassivateIfIdle;
        }
        return snf::runtime::ActorDispatchResult::KeepActive;
    }

    snf::runtime::ActorDispatchResult
    RoomActorBinding::resume(snf::runtime::ActorState& state, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        static_cast<void>(state);
        static_cast<void>(context);
        static_cast<void>(stop_token);
        throw std::logic_error{"RoomActorBinding has no suspension point"};
    }
}
