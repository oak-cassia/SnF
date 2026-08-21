#include "snf/server/party_actor_binding.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    struct PartyActorBinding::PartyActorState final : snf::runtime::ActorState
    {
        PartyActorState(const PartyId party, const PartyConfig config)
            : party(party, config)
        {
        }

        Party party;
    };

    struct PartyActorBinding::CommandPayload
    {
        PartyInboundCommand command;
        CommandReleaseToken release;
    };

    PartyActorBinding::PartyActorBinding(PartyActorBindingConfig config, CommandLifecycleSink& lifecycle)
        : _actor_config(config.actor)
        , _on_result(std::move(config.on_result))
        , _lifecycle(lifecycle)
    {
        if (_actor_config.max_members == 0)
        {
            throw std::invalid_argument{"Party member capacity must be positive"};
        }
    }

    snf::runtime::ActorKind PartyActorBinding::kind() const noexcept
    {
        return snf::runtime::ActorKind::Party;
    }

    PartyActorBindingStats PartyActorBinding::stats() const noexcept
    {
        return PartyActorBindingStats{
            .commands = _commands.load(std::memory_order_relaxed),
            .rejected = _rejected.load(std::memory_order_relaxed),
            .passivation_requests = _passivation_requests.load(std::memory_order_relaxed),
        };
    }

    snf::runtime::ActorSubmission PartyActorBinding::makeCommand(PartyInboundCommand command) const
    {
        if (command.party.value == 0)
        {
            throw std::invalid_argument{"Party command target must be non-zero"};
        }

        CommandReleaseToken release;
        if (command.reply)
        {
            if (command.reply->connection != command.connection)
            {
                throw std::invalid_argument{"Party reply connection does not match its command"};
            }
            release = CommandReleaseToken{_lifecycle, command.reply->connection};
        }
        const PartyId party = command.party;
        return makeSubmission(
            snf::runtime::ActorKey{
                .kind = kind(),
                .entity = party.value,
            },
            snf::runtime::ActorActivation::ActivateIfMissing,
            snf::runtime::ActorAccounting::Command,
            CommandPayload{
                .command = std::move(command),
                .release = std::move(release),
            });
    }

    std::unique_ptr<snf::runtime::ActorState> PartyActorBinding::activate(const snf::runtime::EntityId entity)
    {
        return std::make_unique<PartyActorState>(PartyId{.value = entity}, _actor_config);
    }

    snf::runtime::ActorDispatchResult
    PartyActorBinding::dispatch(snf::runtime::ActorState& state, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        static_cast<void>(context);
        static_cast<void>(stop_token);
        auto& party_state = dynamic_cast<PartyActorState&>(state);
        const CommandPayload& payload = payloadAs<CommandPayload>(submission);
        const PartyResult result = party_state.party.handle(payload.command.command);
        _commands.fetch_add(1, std::memory_order_relaxed);
        if (result.status != PartyCommandStatus::Applied && result.status != PartyCommandStatus::AlreadyMember)
        {
            _rejected.fetch_add(1, std::memory_order_relaxed);
        }
        if (_on_result)
        {
            _on_result(payload.command, result);
        }

        if (party_state.party.memberCount() == 0)
        {
            _passivation_requests.fetch_add(1, std::memory_order_relaxed);
            return snf::runtime::ActorDispatchResult::PassivateIfIdle;
        }
        return snf::runtime::ActorDispatchResult::KeepActive;
    }

    snf::runtime::ActorDispatchResult PartyActorBinding::resume(snf::runtime::ActorState& state, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        static_cast<void>(state);
        static_cast<void>(context);
        static_cast<void>(stop_token);
        throw std::logic_error{"PartyActorBinding has no suspension point"};
    }
}
