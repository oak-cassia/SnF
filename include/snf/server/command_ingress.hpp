#pragma once

#include "snf/server/runtime_types.hpp"

namespace snf::server
{
    enum class PostResult
    {
        Accepted,
        Full,
        Closed,
    };

    // The network runtime posts decoded commands through this boundary without
    // knowing how actors are sharded or which queue owns the command.
    class CommandIngress
    {
    public:
        virtual ~CommandIngress() = default;

        [[nodiscard]] virtual PostResult tryPost(InboundCommand command) = 0;
        virtual void close() noexcept = 0;
        virtual void cancel() noexcept = 0;
    };
}
