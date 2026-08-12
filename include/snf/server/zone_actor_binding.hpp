#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/distribution.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/zone_actor.hpp"
#include "snf/server/zone_inbound_command.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>

namespace snf::server
{
    struct ZoneActorBindingConfig
    {
        ZoneActorConfig actor;
        std::chrono::nanoseconds tick_budget{std::chrono::milliseconds{5}};
        // Result delivery is intentionally a binding concern. Production routing
        // can replace this callback with a typed sink without putting connection
        // or protocol state inside ZoneActor.
        std::function<void(const ZoneInboundCommand&, const ZoneResult&)> on_result;
    };

    struct ZoneActorBindingStats
    {
        snf::runtime::DistributionSnapshot command_execution_nanoseconds;
        snf::runtime::DistributionSnapshot tick_execution_nanoseconds;
        std::uint64_t tick_overruns{0};
    };

    class ZoneActorBinding final : public snf::runtime::ActorBinding
    {
    public:
        explicit ZoneActorBinding(ZoneActorBindingConfig config = {});
        ZoneActorBinding(ZoneActorBindingConfig config, CommandLifecycleSink& lifecycle);

        [[nodiscard]] snf::runtime::ActorKind kind() const noexcept override;
        [[nodiscard]] snf::runtime::ActorSubmission makeCommand(ZoneInboundCommand command) const;
        [[nodiscard]] snf::runtime::ActorSubmission makePassivate(ZoneId zone) const;
        [[nodiscard]] ZoneActorBindingStats stats() const noexcept;

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
        struct ZoneActorSlot;
        struct CommandPayload;
        struct PassivatePayload;

        ZoneActorConfig _actor_config;
        std::chrono::nanoseconds _tick_budget;
        std::function<void(const ZoneInboundCommand&, const ZoneResult&)> _on_result;
        CommandLifecycleSink* _lifecycle{nullptr};
        snf::runtime::Distribution _command_execution_nanoseconds;
        snf::runtime::Distribution _tick_execution_nanoseconds;
        std::atomic<std::uint64_t> _tick_overruns{0};
    };
}
