#pragma once

#include "snf/game/skill_id.hpp"

#include <chrono>
#include <cstdint>
#include <optional>

namespace snf::server
{
    struct SkillDefinition
    {
        SkillId skill{};
        std::uint64_t attack_percent{0};
        std::chrono::milliseconds cooldown{0};
        std::uint32_t range{0};
    };

    inline constexpr SkillId SLASH{.value = 1};
    inline constexpr std::uint64_t SLASH_ATTACK_PERCENT = 100;
    inline constexpr std::chrono::milliseconds SLASH_COOLDOWN{1000};
    inline constexpr std::uint32_t SLASH_RANGE = 12;
    inline constexpr std::uint64_t ATTACK_PERCENT_DENOMINATOR = 100;

    class SkillCatalog final
    {
    public:
        [[nodiscard]] std::optional<SkillDefinition> find(SkillId skill) const noexcept;
    };

    [[nodiscard]] const SkillCatalog& skillCatalog() noexcept;
    [[nodiscard]] std::optional<SkillDefinition> findSkill(SkillId skill) noexcept;

    [[nodiscard]] std::uint64_t skillDamage(const SkillDefinition& skill, std::uint64_t attack) noexcept;
}
