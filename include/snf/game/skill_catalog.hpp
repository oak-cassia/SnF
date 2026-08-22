#pragma once

#include "snf/game/skill_id.hpp"

#include <chrono>
#include <cstdint>
#include <optional>

namespace snf::server
{
    // The server owns every number a cast depends on. A client names a skill and
    // nothing else, so damage and cooldown are never sent and never trusted.
    //
    // Keep the arithmetic integral, for the reason street progression does: these
    // values reach results, frames and tests, and a float would make all three
    // depend on rounding.
    struct SkillDefinition
    {
        SkillId skill{};
        // Percent of the caster's attack, so retuning a skill is a constant change
        // rather than a change to how damage is derived.
        std::uint64_t attack_percent{0};
        std::chrono::milliseconds cooldown{0};
    };

    inline constexpr SkillId SLASH{.value = 1};
    inline constexpr std::uint64_t SLASH_ATTACK_PERCENT = 100;
    inline constexpr std::chrono::milliseconds SLASH_COOLDOWN{1000};
    inline constexpr std::uint64_t ATTACK_PERCENT_DENOMINATOR = 100;

    // Slash is the whole catalogue on purpose. A skill that hits several enemies
    // needs several enemies to exist, and a heal needs someone damaged to heal, so
    // both wait for the state they would read instead of being defined ahead of it.
    class SkillCatalog final
    {
    public:
        [[nodiscard]] std::optional<SkillDefinition> find(SkillId skill) const noexcept;
    };

    [[nodiscard]] const SkillCatalog& skillCatalog() noexcept;
    [[nodiscard]] std::optional<SkillDefinition> findSkill(SkillId skill) noexcept;

    // The multiply comes before the divide, and the caster's attack is already
    // bounded by the progression clamp, so this stays exact without overflowing.
    [[nodiscard]] std::uint64_t skillDamage(const SkillDefinition& skill, std::uint64_t attack) noexcept;
}
