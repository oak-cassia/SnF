#pragma once

#include "snf/net/connection_id.hpp"

#include <cstdint>

namespace snf::server
{
    // Temporary pre-authentication routing identity. It may be derived from a
    // connection generation, but it is never a PlayerId, persistence key, or
    // reconnect key.
    struct ProvisionalActorId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const ProvisionalActorId&) const noexcept = default;
    };

    [[nodiscard]] constexpr ProvisionalActorId provisionalActorIdFor(const snf::net::ConnectionId connection) noexcept
    {
        return ProvisionalActorId{.value = connection.generation};
    }
}
