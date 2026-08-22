#include "snf/game/skill_catalog.hpp"

namespace snf::server
{
    std::optional<SkillDefinition> SkillCatalog::find(const SkillId skill) const noexcept
    {
        if (skill == SLASH)
        {
            return SkillDefinition{
                .skill = SLASH,
                .attack_percent = SLASH_ATTACK_PERCENT,
                .cooldown = SLASH_COOLDOWN,
            };
        }
        return std::nullopt;
    }

    const SkillCatalog& skillCatalog() noexcept
    {
        static const SkillCatalog catalog;
        return catalog;
    }

    std::optional<SkillDefinition> findSkill(const SkillId skill) noexcept
    {
        return skillCatalog().find(skill);
    }

    std::uint64_t skillDamage(const SkillDefinition& skill, const std::uint64_t attack) noexcept
    {
        return attack * skill.attack_percent / ATTACK_PERCENT_DENOMINATOR;
    }
}
