#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_command_ingress.hpp"

namespace snf::server
{
    // Adapts the Player command boundary to the registered Player binding.
    class PlayerActorIngress final : public PlayerCommandIngress
    {
    public:
        PlayerActorIngress(snf::runtime::ActorRuntime& runtime,
                           PlayerActorBinding& binding) noexcept;

        [[nodiscard]] snf::runtime::PostResult tryPost(PlayerInboundCommand command) override;
        [[nodiscard]] snf::runtime::PostResult
        tryPostConnectionClosed(ProvisionalActorId actor, ConnectionClosed closed) override;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        snf::runtime::ActorRuntime& _runtime;
        PlayerActorBinding& _binding;
    };
}
