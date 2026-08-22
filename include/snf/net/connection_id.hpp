#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

namespace snf::net
{
    // Identifies one concrete network-session incarnation. It is valid only for
    // connection lifetime checks and must never be used as a persistent domain ID.
    struct ConnectionId
    {
        // TCP generations are issued from 1, so default initialization is the
        // intentional invalid identity without a second sentinel constant.
        int descriptor{-1};
        std::uint64_t generation{0};

        [[nodiscard]] bool operator==(const ConnectionId&) const noexcept = default;
    };

    // The generation is part of the hash because a reused descriptor is a different
    // incarnation: per-connection accounting for a closed session must never be
    // charged to the session that inherited its descriptor.
    struct ConnectionIdHash
    {
        [[nodiscard]] std::size_t operator()(const ConnectionId& connection) const noexcept
        {
            std::uint64_t value = static_cast<std::uint64_t>(static_cast<std::uint32_t>(connection.descriptor)) + 0x9e3779b97f4a7c15ULL + (connection.generation << 6U) + (connection.generation >> 2U);
            value ^= value >> 30U;
            value *= 0xbf58476d1ce4e5b9ULL;
            value ^= value >> 27U;
            value *= 0x94d049bb133111ebULL;
            value ^= value >> 31U;
            return std::hash<std::uint64_t>{}(value);
        }
    };
}
