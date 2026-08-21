#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>

namespace snf::server
{
    struct ZoneId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const ZoneId&) const noexcept = default;
    };

    struct ZoneIdHash
    {
        [[nodiscard]] std::size_t operator()(const ZoneId zone) const noexcept
        {
            return std::hash<std::uint64_t>{}(zone.value);
        }
    };
}
