#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/zone_actor.hpp"
#include "snf/server/zone_inbound_command.hpp"

#include <functional>

namespace snf::server
{
    struct ZoneActorBindingConfig
    {
        ZoneActorConfig actor;
        // Result delivery is intentionally a binding concern. Production routing
        // can replace this callback with a typed sink without putting connection
        // or protocol state inside ZoneActor.
        std::function<void(const ZoneInboundCommand&, const ZoneResult&)> on_result;
    };

    class ZoneActorBinding final : public snf::runtime::ActorBinding
    {
    public:
        explicit ZoneActorBinding(ZoneActorBindingConfig config = {});
        ZoneActorBinding(ZoneActorBindingConfig config, CommandLifecycleSink& lifecycle);

        [[nodiscard]] snf::runtime::ActorKind kind() const noexcept override;
        [[nodiscard]] snf::runtime::ActorSubmission makeCommand(ZoneInboundCommand command) const;
        [[nodiscard]] snf::runtime::ActorSubmission makePassivate(ZoneId zone) const;

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
        std::function<void(const ZoneInboundCommand&, const ZoneResult&)> _on_result;
        CommandLifecycleSink* _lifecycle{nullptr};
    };
}
