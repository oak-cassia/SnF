#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

namespace snf::net
{
    struct ConnectionId
    {
        int descriptor{-1};
        std::uint64_t generation{0};

        [[nodiscard]] bool operator==(const ConnectionId&) const noexcept = default;
    };

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
