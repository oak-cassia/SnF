#pragma once

#include <cstdint>

namespace snf::net
{
    // Identifies one concrete network-session incarnation. It is valid only for
    // connection lifetime checks and must never be used as a persistent domain ID.
    struct ConnectionId
    {
        int descriptor{-1};
        std::uint64_t generation{0};

        [[nodiscard]] bool operator==(const ConnectionId&) const noexcept = default;
    };
}
