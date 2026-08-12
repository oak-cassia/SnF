#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_command_ingress.hpp"

namespace snf::server
{
    // Adapts the Player command boundary to the registered Player binding.
    //
    // It is also the only place that knows a post was refused. The refusal is reported
    // here rather than inferred from the release the refused submission still performs,
    // so an admission failure never inflates the count of commands that ran.
    class PlayerActorIngress final : public PlayerCommandIngress
    {
    public:
        PlayerActorIngress(snf::runtime::ActorRuntime& runtime,
                           PlayerActorBinding& binding,
                           CommandLifecycleSink& lifecycle) noexcept;

        [[nodiscard]] snf::runtime::PostResult tryPost(PlayerInboundCommand command) override;
        [[nodiscard]] snf::runtime::PostResult
        tryPostConnectionClosed(ProvisionalActorId actor, ConnectionClosed closed) override;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        snf::runtime::ActorRuntime& _runtime;
        PlayerActorBinding& _binding;
        CommandLifecycleSink& _lifecycle;
    };
}
