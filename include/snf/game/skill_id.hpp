#pragma once

#include <cstdint>

namespace snf::server
{
    struct SkillId
    {
        std::uint32_t value{0};

        [[nodiscard]] bool operator==(const SkillId&) const noexcept = default;
    };

    inline constexpr SkillId SLASH_SKILL_ID{.value = 1};
    inline constexpr SkillId ARCANE_BOLT_SKILL_ID{.value = 2};
}
