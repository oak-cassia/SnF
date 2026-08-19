#pragma once

#include "snf/runtime/post_result.hpp"
#include "snf/server/connection_lifecycle.hpp"
#include "snf/server/player_inbound_command.hpp"

namespace snf::server
{
    // The network route reaches Player logic through this named boundary without
    // knowing anything about runtime shard ownership or typed submissions.
    class PlayerCommandIngress
    {
    public:
        virtual ~PlayerCommandIngress() = default;

        [[nodiscard]] virtual snf::runtime::PostResult tryPost(PlayerInboundCommand command) = 0;
        // A lifecycle fact cannot be silently dropped. Full leaves the caller
        // responsible for retrying without changing command-overload metrics.
        [[nodiscard]] virtual snf::runtime::PostResult tryPostConnectionClosed(PlayerActorId actor, ConnectionClosed closed) = 0;
        virtual void close() noexcept = 0;
        virtual void cancel() noexcept = 0;
    };
}
