#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

namespace snf::server
{
    struct PlayerId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const PlayerId&) const noexcept = default;
    };

    struct PlayerIdHash
    {
        [[nodiscard]] std::size_t operator()(const PlayerId player) const noexcept
        {
            return std::hash<std::uint64_t>{}(player.value);
        }
    };
}
