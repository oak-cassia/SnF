#include "snf/server/player_actor_ingress.hpp"

#include <utility>

namespace snf::server
{
    PlayerActorIngress::PlayerActorIngress(snf::runtime::ActorRuntime& runtime,
                                           PlayerActorBinding& binding,
                                           CommandLifecycleSink& lifecycle) noexcept
        : _runtime(runtime)
        , _binding(binding)
        , _lifecycle(lifecycle)
    {
    }

    snf::runtime::PostResult PlayerActorIngress::tryPost(PlayerInboundCommand command)
    {
        const snf::net::ConnectionId connection = command.connection;
        const snf::runtime::PostResult result =
            _runtime.tryPost(_binding.makeCommand(std::move(command)));

        if (result != snf::runtime::PostResult::Accepted)
        {
            // The refused submission has already released the credit it took, inside
            // tryPost. Recording the refusal here is what keeps that release out of the
            // count of commands that reached a result.
            _lifecycle.onCommandAdmissionRejected(connection);
        }

        return result;
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
