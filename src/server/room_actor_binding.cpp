#include "snf/server/room_actor_binding.hpp"

#include "snf/runtime/tell_payload.hpp"

#include <chrono>
#include <stdexcept>
#include <utility>
#include <variant>

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
        , _tick_budget(config.tick_budget)
        , _on_result(std::move(config.on_result))
    {
        if (_tick_budget < std::chrono::nanoseconds::zero())
        {
            throw std::invalid_argument{"Room tick budget cannot be negative"};
        }
    }

    RoomActorBinding::RoomActorBinding(RoomActorBindingConfig config, CommandLifecycleSink& lifecycle)
        : _actor_config(config.actor)
        , _tick_budget(config.tick_budget)
        , _on_result(std::move(config.on_result))
        , _lifecycle(&lifecycle)
    {
        if (_tick_budget < std::chrono::nanoseconds::zero())
        {
            throw std::invalid_argument{"Room tick budget cannot be negative"};
        }
    }

    snf::runtime::ActorKind RoomActorBinding::kind() const noexcept
    {
        return snf::runtime::ActorKind::Room;
    }

    RoomActorBindingStats RoomActorBinding::stats() const noexcept
    {
        return RoomActorBindingStats{
            .command_execution_nanoseconds = _command_execution_nanoseconds.snapshot(),
            .tick_execution_nanoseconds = _tick_execution_nanoseconds.snapshot(),
            .tick_publish_nanoseconds = _tick_publish_nanoseconds.snapshot(),
            .tick_turn_nanoseconds = _tick_turn_nanoseconds.snapshot(),
            .tick_overruns = _tick_overruns.load(std::memory_order_relaxed),
            .tick_schedule_rejections = _tick_schedule_rejections.load(std::memory_order_relaxed),
            .deadline_schedule_rejections = _deadline_schedule_rejections.load(std::memory_order_relaxed),
            .grant_tell_rejections = _grant_tell_rejections.load(std::memory_order_relaxed),
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
        const bool is_tick = std::holds_alternative<RoomSimulationTick>(payload.command.command);
        const auto started_at = std::chrono::steady_clock::now();

        // StartBattle is a two-resource commit: the Room may enter Running only
        // after its mandatory terminal backstop owns capacity. The Actor is the
        // single writer, so canStartBattle cannot become stale before handle().
        std::optional<snf::runtime::TimerHandle> deadline_timer;
        if (std::holds_alternative<StartBattle>(payload.command.command) && room_state.room.canStartBattle())
        {
            RoomInboundCommand deadline_command{
                .room = payload.command.room,
                .command = BattleDeadline{},
                .reply = std::nullopt,
            };
            auto timer_submission = makeSubmission(
                submission.target(),
                snf::runtime::ActorActivation::ExistingOnly,
                snf::runtime::ActorAccounting::Command,
                CommandPayload{
                    .command = std::move(deadline_command),
                    .release = {},
                }
            );
            deadline_timer = context.trySchedule(_actor_config.battle_duration, std::move(timer_submission));
            if (!deadline_timer)
            {
                _deadline_schedule_rejections.fetch_add(1, std::memory_order_relaxed);
                const RoomResult rejected{
                    .status = RoomCommandStatus::RuntimeOverloaded,
                    .phase = room_state.room.phase(),
                    .boss_health = room_state.room.bossHealth(),
                    .boss_spawned = room_state.room.bossSpawned(),
                };
                const auto handled_at = std::chrono::steady_clock::now();
                _command_execution_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(handled_at - started_at));
                if (_on_result)
                {
                    _on_result(payload.command, rejected);
                }
                return snf::runtime::ActorDispatchResult::KeepActive;
            }
        }

        // observedAt, not the reading above: the Room derives cooldowns from when its
        // turn began, while the reading here exists to measure how long the turn took.
        RoomResult result = room_state.room.handle(payload.command.command, context.observedAt());
        const auto handled_at = std::chrono::steady_clock::now();
        const auto handle_elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(handled_at - started_at);
        _command_execution_nanoseconds.record(handle_elapsed);
        if (is_tick)
        {
            _tick_execution_nanoseconds.record(handle_elapsed);
        }

        // If the preflight and state-machine predicate ever diverge, release a timer
        // that has no Running Room. A result that requests a deadline without the
        // pre-reservation is an internal contract violation, not overload.
        if (deadline_timer && !result.deadline_after)
        {
            context.cancelTimer(*deadline_timer);
        }
        if (result.deadline_after && !deadline_timer)
        {
            throw std::logic_error{"Running Room did not pre-reserve its deadline timer"};
        }

        if (result.tick_after)
        {
            RoomInboundCommand tick_command{
                .room = payload.command.room,
                .command = RoomSimulationTick{},
                .reply = std::nullopt,
            };
            auto timer_submission = makeSubmission(
                submission.target(),
                snf::runtime::ActorActivation::ExistingOnly,
                snf::runtime::ActorAccounting::Command,
                CommandPayload{
                    .command = std::move(tick_command),
                    .release = {},
                }
            );
            if (!context.trySchedule(*result.tick_after, std::move(timer_submission)))
            {
                _tick_schedule_rejections.fetch_add(1, std::memory_order_relaxed);
            }
        }

        const auto publish_started_at = std::chrono::steady_clock::now();
        if (_on_result)
        {
            _on_result(payload.command, result);
        }
        const auto published_at = std::chrono::steady_clock::now();
        if (is_tick)
        {
            _tick_publish_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(published_at - publish_started_at));
            const auto turn_elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(published_at - started_at);
            _tick_turn_nanoseconds.record(turn_elapsed);
            if (turn_elapsed >= _tick_budget)
            {
                _tick_overruns.fetch_add(1, std::memory_order_relaxed);
            }
        }

        for (const StreetExperienceGrant& grant : result.grants)
        {
            // Best effort by design. A full target mailbox drops the reward rather
            // than blocking the Room, and there is no reply channel to report it on.
            const snf::runtime::PostResult posted = context.tryTell(
                snf::runtime::ActorKey{
                    .kind = snf::runtime::ActorKind::Player,
                    .entity = grant.player.value,
                },
                snf::runtime::TellPayload::of(grant)
            );
            if (posted != snf::runtime::PostResult::Accepted)
            {
                _grant_tell_rejections.fetch_add(1, std::memory_order_relaxed);
            }
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
