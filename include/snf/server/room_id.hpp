#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

namespace snf::server
{
    struct RoomId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const RoomId&) const noexcept = default;
    };

    struct RoomIdHash
    {
        [[nodiscard]] std::size_t operator()(const RoomId room) const noexcept
        {
            return std::hash<std::uint64_t>{}(room.value);
        }
    };
}
