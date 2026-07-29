#pragma once

#include "snf/server/connection_lifecycle.hpp"
#include "snf/server/inbound_command.hpp"
#include "snf/server/post_result.hpp"

namespace snf::server
{
    // The network runtime posts protocol-dispatched, typed commands through this
    // boundary without knowing how actors are sharded or which queue owns them.
    class CommandIngress
    {
    public:
        virtual ~CommandIngress() = default;

        [[nodiscard]] virtual PostResult tryPost(InboundCommand command) = 0;
        // A lifecycle fact cannot be silently dropped. Full leaves the caller
        // responsible for retrying without changing command-overload metrics.
        [[nodiscard]] virtual PostResult tryPostConnectionClosed(ProvisionalActorId actor,
                                                                 ConnectionClosed closed) = 0;
        virtual void close() noexcept = 0;
        virtual void cancel() noexcept = 0;
    };
}
