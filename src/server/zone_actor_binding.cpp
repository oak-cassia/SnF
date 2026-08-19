#include "snf/server/zone_actor_binding.hpp"

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
    struct ZoneActorBinding::ZoneActorSlot final : snf::runtime::ActorSlot
    {
        ZoneActorSlot(const ZoneId zone, const ZoneActorConfig config)
            : actor(zone, config)
        {
        }

        ZoneActor actor;
    };

    struct ZoneActorBinding::CommandPayload
    {
        ZoneInboundCommand command;
        CommandReleaseToken release;
    };

    struct ZoneActorBinding::PassivatePayload
    {
    };

    ZoneActorBinding::ZoneActorBinding(ZoneActorBindingConfig config)
        : _actor_config(config.actor)
        , _tick_budget(config.tick_budget)
        , _on_result(std::move(config.on_result))
    {
        if (_tick_budget < std::chrono::nanoseconds::zero())
        {
            throw std::invalid_argument{"Zone tick budget cannot be negative"};
        }
    }

    ZoneActorBinding::ZoneActorBinding(ZoneActorBindingConfig config, CommandLifecycleSink& lifecycle)
        : _actor_config(config.actor)
        , _tick_budget(config.tick_budget)
        , _on_result(std::move(config.on_result))
        , _lifecycle(&lifecycle)
    {
        if (_tick_budget < std::chrono::nanoseconds::zero())
        {
            throw std::invalid_argument{"Zone tick budget cannot be negative"};
        }
    }

    snf::runtime::ActorKind ZoneActorBinding::kind() const noexcept
    {
        return snf::runtime::ActorKind::Zone;
    }

    ZoneActorBindingStats ZoneActorBinding::stats() const noexcept
    {
        return ZoneActorBindingStats{
            .command_execution_nanoseconds = _command_execution_nanoseconds.snapshot(),
            .tick_execution_nanoseconds = _tick_execution_nanoseconds.snapshot(),
            .tick_overruns = _tick_overruns.load(std::memory_order_relaxed),
        };
    }

    snf::runtime::ActorSubmission ZoneActorBinding::makeCommand(ZoneInboundCommand command) const
    {
        if (command.zone.value == 0)
        {
            throw std::invalid_argument{"Zone command target must be non-zero"};
        }

        const ZoneId zone = command.zone;
        CommandReleaseToken release;
        if (command.reply)
        {
            if (_lifecycle == nullptr)
            {
                throw std::logic_error{"Replying Zone command requires a lifecycle sink"};
            }
            release = CommandReleaseToken{*_lifecycle, command.reply->connection};
        }
        return makeSubmission(
            snf::runtime::ActorKey{
                .kind = kind(),
                .entity = zone.value,
            },
            snf::runtime::ActorActivation::ActivateIfMissing,
            snf::runtime::ActorAccounting::Command,
            CommandPayload{
                .command = std::move(command),
                .release = std::move(release),
            });
    }

    snf::runtime::ActorSubmission ZoneActorBinding::makePassivate(const ZoneId zone) const
    {
        if (zone.value == 0)
        {
            throw std::invalid_argument{"Zone passivation target must be non-zero"};
        }

        return makeSubmission(
            snf::runtime::ActorKey{
                .kind = kind(),
                .entity = zone.value,
            },
            snf::runtime::ActorActivation::ExistingOnly,
            snf::runtime::ActorAccounting::Control,
            PassivatePayload{});
    }

    std::unique_ptr<snf::runtime::ActorSlot> ZoneActorBinding::activate(const snf::runtime::EntityId entity)
    {
        return std::make_unique<ZoneActorSlot>(ZoneId{.value = entity}, _actor_config);
    }

    snf::runtime::ActorDispatchResult
    ZoneActorBinding::dispatch(snf::runtime::ActorSlot& slot, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        static_cast<void>(stop_token);
        if (submission.accounting() == snf::runtime::ActorAccounting::Control)
        {
            static_cast<void>(payloadAs<PassivatePayload>(submission));
            return snf::runtime::ActorDispatchResult::Evict;
        }

        auto& zone_slot = dynamic_cast<ZoneActorSlot&>(slot);
        const CommandPayload& payload = payloadAs<CommandPayload>(submission);
        const auto started_at = std::chrono::steady_clock::now();
        ZoneResult result = zone_slot.actor.handle(payload.command.command);
        const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_at);
        _command_execution_nanoseconds.record(elapsed);
        if (std::holds_alternative<ZoneSimulationTick>(payload.command.command))
        {
            _tick_execution_nanoseconds.record(elapsed);
            if (elapsed >= _tick_budget)
            {
                _tick_overruns.fetch_add(1, std::memory_order_relaxed);
            }
        }
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
                        ZoneInboundCommand tick_command{
                            .zone = payload.command.zone,
                            .command = ZoneSimulationTick{},
                            .reply = std::nullopt,
                            .handoff = std::nullopt,
                        };
                        // ExistingOnly: a tick must not resurrect a Zone that was
                        // evicted between the request and the deadline.
                        auto timer_submission = makeSubmission(submission.target(),
                                                               snf::runtime::ActorActivation::ExistingOnly,
                                                               snf::runtime::ActorAccounting::Command,
                                                               CommandPayload{
                                                                   .command = std::move(tick_command),
                                                                   .release = {},
                                                               });
                        static_cast<void>(context.trySchedule(follow_up.delay, std::move(timer_submission)));
                    }
                    else if constexpr (std::is_same_v<Action, TellActor>)
                    {
                        static_cast<void>(context.tryTell(follow_up.target, std::move(follow_up.payload)));
                    }
                    else
                    {
                        static_assert(always_false_v<Action>, "Unhandled FollowUpAction alternative");
                    }
                },
                action);
        }

        if (zone_slot.actor.playerCount() == 0)
        {
            return snf::runtime::ActorDispatchResult::PassivateIfIdle;
        }
        return snf::runtime::ActorDispatchResult::KeepActive;
    }

    snf::runtime::ActorDispatchResult ZoneActorBinding::resume(snf::runtime::ActorSlot& slot, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        static_cast<void>(slot);
        static_cast<void>(context);
        static_cast<void>(stop_token);
        throw std::logic_error{"ZoneActorBinding has no suspension point"};
    }
}
