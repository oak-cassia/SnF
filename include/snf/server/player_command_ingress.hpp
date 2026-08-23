#pragma once

#include "snf/runtime/post_result.hpp"
#include "snf/server/connection_lifecycle.hpp"
#include "snf/server/player_inbound_command.hpp"

namespace snf::server
{
    class PlayerCommandIngress
    {
    public:
        virtual ~PlayerCommandIngress() = default;

        [[nodiscard]] virtual snf::runtime::PostResult tryPost(PlayerInboundCommand command) = 0;
        [[nodiscard]] virtual snf::runtime::PostResult tryPostConnectionClosed(PlayerActorId actor, ConnectionClosed closed) = 0;
        virtual void close() noexcept = 0;
        virtual void cancel() noexcept = 0;
    };
}
