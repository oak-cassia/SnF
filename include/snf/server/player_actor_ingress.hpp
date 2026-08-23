#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_command_ingress.hpp"

namespace snf::server
{
    class PlayerActorIngress final : public PlayerCommandIngress
    {
    public:
        PlayerActorIngress(snf::runtime::ActorRuntime& runtime, PlayerActorBinding& binding, CommandLifecycleSink& lifecycle) noexcept;
        PlayerActorIngress(snf::runtime::ActorRuntime& runtime, PlayerActorBinding& provisional_binding, PlayerActorBinding& persistent_binding, CommandLifecycleSink& lifecycle) noexcept;

        [[nodiscard]] snf::runtime::PostResult tryPost(PlayerInboundCommand command) override;
        [[nodiscard]] snf::runtime::PostResult tryPostConnectionClosed(PlayerActorId actor, ConnectionClosed closed) override;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        [[nodiscard]] PlayerActorBinding& bindingFor(PlayerActorId actor) const;

        snf::runtime::ActorRuntime& _runtime;
        PlayerActorBinding& _primary_binding;
        PlayerActorBinding* _secondary_binding{nullptr};
        CommandLifecycleSink& _lifecycle;
    };
}
