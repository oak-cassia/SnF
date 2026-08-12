#include "snf/server/zone_actor_ingress.hpp"

#include <utility>

namespace snf::server
{
    ZoneActorIngress::ZoneActorIngress(snf::runtime::ActorRuntime& runtime,
                                       ZoneActorBinding& binding) noexcept
        : _runtime(runtime)
        , _binding(binding)
    {
    }

    snf::runtime::PostResult ZoneActorIngress::tryPost(ZoneInboundCommand command)
    {
        return _runtime.tryPost(_binding.makeCommand(std::move(command)));
    }

    snf::runtime::PostResult ZoneActorIngress::tryPassivate(const ZoneId zone)
    {
        return _runtime.tryPost(_binding.makePassivate(zone));
    }
}
