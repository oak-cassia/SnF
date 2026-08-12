#pragma once

#include <cstdint>

namespace snf::server
{
    struct ZoneId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const ZoneId&) const noexcept = default;
    };
}
