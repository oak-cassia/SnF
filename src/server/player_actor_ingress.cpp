#include "snf/server/player_actor_ingress.hpp"

#include <utility>

namespace snf::server
{
    PlayerActorIngress::PlayerActorIngress(snf::runtime::ActorRuntime& runtime,
                                           PlayerActorBinding& binding) noexcept
        : _runtime(runtime)
        , _binding(binding)
    {
    }

    snf::runtime::PostResult PlayerActorIngress::tryPost(PlayerInboundCommand command)
    {
        return _runtime.tryPost(_binding.makeCommand(std::move(command)));
    }

    snf::runtime::PostResult
    PlayerActorIngress::tryPostConnectionClosed(const ProvisionalActorId actor,
                                                ConnectionClosed closed)
    {
        return _runtime.tryPost(_binding.makeConnectionClosed(actor, std::move(closed)));
    }

    void PlayerActorIngress::close() noexcept
    {
        _runtime.close();
    }

    void PlayerActorIngress::cancel() noexcept
    {
        _runtime.cancel();
    }
}
