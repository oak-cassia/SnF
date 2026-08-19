#include "snf/server/room_actor_ingress.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    RoomActorIngress::RoomActorIngress(snf::runtime::ActorRuntime& runtime, RoomActorBinding& binding) noexcept
        : _runtime(runtime)
        , _binding(binding)
    {
    }

    RoomActorIngress::RoomActorIngress(snf::runtime::ActorRuntime& runtime, RoomActorBinding& binding, CommandLifecycleSink& lifecycle) noexcept
        : _runtime(runtime)
        , _binding(binding)
        , _lifecycle(&lifecycle)
    {
    }

    snf::runtime::PostResult RoomActorIngress::tryPost(RoomInboundCommand command)
    {
        const std::optional<snf::net::ConnectionId> connection = command.reply ? std::optional{command.reply->connection} : std::nullopt;
        const snf::runtime::PostResult result = _runtime.tryPost(_binding.makeCommand(std::move(command)));
        if (result != snf::runtime::PostResult::Accepted && connection)
        {
            if (_lifecycle == nullptr)
            {
                throw std::logic_error{"Replying Room command requires a lifecycle sink"};
            }
            _lifecycle->onCommandAdmissionRejected(*connection);
        }
        return result;
    }

    snf::runtime::PostResult RoomActorIngress::tryPassivate(const RoomId room)
    {
        return _runtime.tryPost(_binding.makePassivate(room));
    }
}
