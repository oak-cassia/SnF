#include "snf/server/party_actor_ingress.hpp"

#include <utility>

namespace snf::server
{
    PartyActorIngress::PartyActorIngress(snf::runtime::ActorRuntime& runtime,
                                         PartyActorBinding& binding,
                                         CommandLifecycleSink& lifecycle) noexcept
        : _runtime(runtime)
        , _binding(binding)
        , _lifecycle(lifecycle)
    {
    }

    snf::runtime::PostResult PartyActorIngress::tryPost(PartyInboundCommand command)
    {
        const std::optional<snf::net::ConnectionId> connection =
            command.reply ? std::optional{command.reply->connection} : std::nullopt;
        const snf::runtime::PostResult result =
            _runtime.tryPost(_binding.makeCommand(std::move(command)));
        if (result != snf::runtime::PostResult::Accepted && connection)
        {
            _lifecycle.onCommandAdmissionRejected(*connection);
        }
        return result;
    }
}
