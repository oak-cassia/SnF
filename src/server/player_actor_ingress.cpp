#include "snf/server/player_actor_ingress.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    PlayerActorIngress::PlayerActorIngress(snf::runtime::ActorRuntime& runtime, PlayerActorBinding& binding, CommandLifecycleSink& lifecycle) noexcept
        : _runtime(runtime)
        , _primary_binding(binding)
        , _lifecycle(lifecycle)
    {
    }

    PlayerActorIngress::PlayerActorIngress(snf::runtime::ActorRuntime& runtime,
                                           PlayerActorBinding& provisional_binding,
                                           PlayerActorBinding& persistent_binding,
                                           CommandLifecycleSink& lifecycle) noexcept
        : _runtime(runtime)
        , _primary_binding(provisional_binding)
        , _secondary_binding(&persistent_binding)
        , _lifecycle(lifecycle)
    {
    }

    snf::runtime::PostResult PlayerActorIngress::tryPost(PlayerInboundCommand command)
    {
        const snf::net::ConnectionId connection = command.connection;
        PlayerActorBinding& binding = bindingFor(command.actor);
        const snf::runtime::PostResult result = _runtime.tryPost(binding.makeCommand(std::move(command)));

        if (result != snf::runtime::PostResult::Accepted)
        {
            // The refused submission has already released the credit it took, inside
            // tryPost. Recording the refusal here is what keeps that release out of the
            // count of commands that reached a result.
            _lifecycle.onCommandAdmissionRejected(connection);
        }

        return result;
    }

    snf::runtime::PostResult PlayerActorIngress::tryPostConnectionClosed(const PlayerActorId actor, ConnectionClosed closed)
    {
        return _runtime.tryPost(bindingFor(actor).makeConnectionClosed(actor, std::move(closed)));
    }

    void PlayerActorIngress::close() noexcept
    {
        _runtime.close();
    }

    void PlayerActorIngress::cancel() noexcept
    {
        _runtime.cancel();
    }

    PlayerActorBinding& PlayerActorIngress::bindingFor(const PlayerActorId actor) const
    {
        if (_primary_binding.kind() == actor.kind())
        {
            return _primary_binding;
        }

        if (_secondary_binding != nullptr && _secondary_binding->kind() == actor.kind())
        {
            return *_secondary_binding;
        }

        throw std::invalid_argument{"No PlayerActorBinding is registered for the target identity"};
    }
}
