#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/zone_actor_binding.hpp"
#include "snf/server/zone_timer_service.hpp"

namespace snf::server
{
    class ZoneActorIngress final : public ZoneTimerSink
    {
    public:
        ZoneActorIngress(snf::runtime::ActorRuntime& runtime, ZoneActorBinding& binding) noexcept;
        ZoneActorIngress(snf::runtime::ActorRuntime& runtime,
                         ZoneActorBinding& binding,
                         CommandLifecycleSink& lifecycle) noexcept;

        [[nodiscard]] snf::runtime::PostResult tryPost(ZoneInboundCommand command);
        [[nodiscard]] snf::runtime::PostResult tryPassivate(ZoneId zone);
        [[nodiscard]] snf::runtime::PostResult tryPostTimerCommand(ZoneId zone,
                                                                   ZoneCommand command) override;

    private:
        snf::runtime::ActorRuntime& _runtime;
        ZoneActorBinding& _binding;
        CommandLifecycleSink* _lifecycle{nullptr};
    };
}
