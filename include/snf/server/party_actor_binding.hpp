#pragma once

#include "snf/game/party.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/party_inbound_command.hpp"

#include <atomic>
#include <cstdint>
#include <functional>

namespace snf::server
{
    struct PartyActorBindingConfig
    {
        PartyConfig actor;
        std::function<void(const PartyInboundCommand&, const PartyResult&)> on_result;
    };

    struct PartyActorBindingStats
    {
        std::uint64_t commands{0};
        std::uint64_t rejected{0};
        std::uint64_t passivation_requests{0};
    };

    class PartyActorBinding final : public snf::runtime::ActorBinding
    {
    public:
        PartyActorBinding(PartyActorBindingConfig config, CommandLifecycleSink& lifecycle);

        [[nodiscard]] snf::runtime::ActorKind kind() const noexcept override;
        [[nodiscard]] snf::runtime::ActorSubmission makeCommand(PartyInboundCommand command) const;
        [[nodiscard]] PartyActorBindingStats stats() const noexcept;

    protected:
        [[nodiscard]] std::unique_ptr<snf::runtime::ActorSlot> activate(snf::runtime::EntityId entity) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult
        dispatch(snf::runtime::ActorSlot& slot, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, std::stop_token stop_token) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult resume(snf::runtime::ActorSlot& slot, snf::runtime::ActorContext& context, std::stop_token stop_token) override;

    private:
        struct PartyActorSlot;
        struct CommandPayload;

        PartyConfig _actor_config;
        std::function<void(const PartyInboundCommand&, const PartyResult&)> _on_result;
        CommandLifecycleSink& _lifecycle;
        std::atomic<std::uint64_t> _commands{0};
        std::atomic<std::uint64_t> _rejected{0};
        std::atomic<std::uint64_t> _passivation_requests{0};
    };
}
