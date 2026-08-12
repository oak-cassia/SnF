#pragma once

#include <cstdint>

namespace snf::server
{
    struct TimerId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const TimerId&) const noexcept = default;
    };
}
