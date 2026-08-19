#include "snf/server/zone_actor_ingress.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    ZoneActorIngress::ZoneActorIngress(snf::runtime::ActorRuntime& runtime,
                                       ZoneActorBinding& binding) noexcept
        : _runtime(runtime)
        , _binding(binding)
    {
    }

    ZoneActorIngress::ZoneActorIngress(snf::runtime::ActorRuntime& runtime,
                                       ZoneActorBinding& binding,
                                       CommandLifecycleSink& lifecycle) noexcept
        : _runtime(runtime)
        , _binding(binding)
        , _lifecycle(&lifecycle)
    {
    }

    snf::runtime::PostResult ZoneActorIngress::tryPost(ZoneInboundCommand command)
    {
        const std::optional<snf::net::ConnectionId> connection =
            command.reply ? std::optional{command.reply->connection} : std::nullopt;
        const snf::runtime::PostResult result =
            _runtime.tryPost(_binding.makeCommand(std::move(command)));
        if (result != snf::runtime::PostResult::Accepted && connection)
        {
            if (_lifecycle == nullptr)
            {
                throw std::logic_error{"Replying Zone command requires a lifecycle sink"};
            }
            _lifecycle->onCommandAdmissionRejected(*connection);
        }
        return result;
    }

    snf::runtime::PostResult ZoneActorIngress::tryPassivate(const ZoneId zone)
    {
        return _runtime.tryPost(_binding.makePassivate(zone));
    }
}
