#pragma once

#include <cstdint>

namespace snf::server
{
    struct SkillId
    {
        std::uint32_t value{0};

        [[nodiscard]] bool operator==(const SkillId&) const noexcept = default;
    };
}
