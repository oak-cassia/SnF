#include "snf/server/room_actor_binding.hpp"

#include <chrono>
#include <stdexcept>
#include <type_traits>
#include <utility>
#include <variant>

namespace
{
    template <typename> inline constexpr bool always_false_v = false;
}

namespace snf::server
{
    struct RoomActorBinding::RoomActorSlot final : snf::runtime::ActorSlot
    {
        RoomActorSlot(const RoomId room, const RoomActorConfig config)
            : actor(room, config)
        {
        }

        RoomActor actor;
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
            });
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
            PassivatePayload{});
    }

    std::unique_ptr<snf::runtime::ActorSlot> RoomActorBinding::activate(const snf::runtime::EntityId entity)
    {
        return std::make_unique<RoomActorSlot>(RoomId{.value = entity}, _actor_config);
    }

    snf::runtime::ActorDispatchResult
    RoomActorBinding::dispatch(snf::runtime::ActorSlot& slot, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        static_cast<void>(stop_token);
        if (submission.accounting() == snf::runtime::ActorAccounting::Control)
        {
            static_cast<void>(payloadAs<PassivatePayload>(submission));
            return snf::runtime::ActorDispatchResult::Evict;
        }

        auto& room_slot = dynamic_cast<RoomActorSlot&>(slot);
        const CommandPayload& payload = payloadAs<CommandPayload>(submission);
        const auto started_at = std::chrono::steady_clock::now();
        RoomResult result = room_slot.actor.handle(payload.command.command);
        _command_execution_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_at));
        if (_on_result)
        {
            _on_result(payload.command, result);
        }

        for (FollowUpAction& action : result.follow_ups)
        {
            std::visit(
                [&](auto& follow_up)
                {
                    using Action = std::decay_t<decltype(follow_up)>;
                    if constexpr (std::is_same_v<Action, ScheduleTimer>)
                    {
                        RoomInboundCommand completion_command{
                            .room = payload.command.room,
                            .command = BattleCompleted{},
                            .reply = std::nullopt,
                        };
                        // ExistingOnly: if the Room is gone there is nobody left to
                        // reward, so a completion must not resurrect it. A Running
                        // Room stays resident, which is what keeps this deliverable.
                        auto timer_submission = makeSubmission(submission.target(),
                                                               snf::runtime::ActorActivation::ExistingOnly,
                                                               snf::runtime::ActorAccounting::Command,
                                                               CommandPayload{
                                                                   .command = std::move(completion_command),
                                                                   .release = {},
                                                               });
                        static_cast<void>(context.trySchedule(follow_up.delay, std::move(timer_submission)));
                    }
                    else if constexpr (std::is_same_v<Action, TellActor>)
                    {
                        // Best effort by design. A full target mailbox drops the
                        // reward rather than blocking the Room, and there is no reply
                        // channel to report the loss on.
                        static_cast<void>(context.tryTell(follow_up.target, std::move(follow_up.payload)));
                    }
                    else
                    {
                        static_assert(always_false_v<Action>, "Unhandled FollowUpAction alternative");
                    }
                },
                action);
        }

        // A cleared Room has emitted its rewards and has nothing left to do, and a
        // Room nobody joined was activated by a stray command. A Running Room must
        // stay resident so its completion timer can land.
        if (room_slot.actor.phase() == RoomPhase::Cleared || room_slot.actor.participantCount() == 0)
        {
            return snf::runtime::ActorDispatchResult::PassivateIfIdle;
        }
        return snf::runtime::ActorDispatchResult::KeepActive;
    }

    snf::runtime::ActorDispatchResult RoomActorBinding::resume(snf::runtime::ActorSlot& slot, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        static_cast<void>(slot);
        static_cast<void>(context);
        static_cast<void>(stop_token);
        throw std::logic_error{"RoomActorBinding has no suspension point"};
    }
}
