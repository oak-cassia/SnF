#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/server/room_actor_binding.hpp"

namespace snf::server
{
    class RoomActorIngress final
    {
    public:
        RoomActorIngress(snf::runtime::ActorRuntime& runtime, RoomActorBinding& binding) noexcept;
        RoomActorIngress(snf::runtime::ActorRuntime& runtime, RoomActorBinding& binding, CommandLifecycleSink& lifecycle) noexcept;

        [[nodiscard]] snf::runtime::PostResult tryPost(RoomInboundCommand command);
        [[nodiscard]] snf::runtime::PostResult tryPassivate(RoomId room);

    private:
        snf::runtime::ActorRuntime& _runtime;
        RoomActorBinding& _binding;
        CommandLifecycleSink* _lifecycle{nullptr};
    };
}
