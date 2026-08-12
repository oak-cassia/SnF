#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/zone_actor_binding.hpp"

namespace snf::server
{
    class ZoneActorIngress
    {
    public:
        ZoneActorIngress(snf::runtime::ActorRuntime& runtime, ZoneActorBinding& binding) noexcept;

        [[nodiscard]] snf::runtime::PostResult tryPost(ZoneInboundCommand command);
        [[nodiscard]] snf::runtime::PostResult tryPassivate(ZoneId zone);

    private:
        snf::runtime::ActorRuntime& _runtime;
        ZoneActorBinding& _binding;
    };
}
