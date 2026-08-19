#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/party_actor_binding.hpp"

namespace snf::server
{
    class PartyActorIngress final
    {
    public:
        PartyActorIngress(snf::runtime::ActorRuntime& runtime, PartyActorBinding& binding, CommandLifecycleSink& lifecycle) noexcept;

        [[nodiscard]] snf::runtime::PostResult tryPost(PartyInboundCommand command);

    private:
        snf::runtime::ActorRuntime& _runtime;
        PartyActorBinding& _binding;
        CommandLifecycleSink& _lifecycle;
    };
}
