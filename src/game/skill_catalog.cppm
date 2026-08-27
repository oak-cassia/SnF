module;

#include "snf/game/skill_id.hpp"

#include <chrono>
#include <optional>
#include <variant>

export module snf.game.skill_catalog;

export namespace snf::server
{
    struct AreaAttackBehavior
    {
        std::uint64_t attack_percent{0};
        std::uint32_t range{0};
    };

    struct HomingProjectileAttackBehavior
    {
        std::uint64_t attack_percent{0};
        std::uint32_t acquisition_range{0};
        std::uint32_t speed_per_tick{0};
        std::uint32_t hit_range{0};
        std::chrono::milliseconds lifetime{0};
    };

    using AttackBehavior = std::variant<AreaAttackBehavior, HomingProjectileAttackBehavior>;

    struct SkillDefinition
    {
        SkillId skill_id{};
        std::chrono::milliseconds cooldown{0};
        AttackBehavior behavior{};
    };

    inline constexpr SkillId SLASH{.value = 1};
    inline constexpr std::uint64_t SLASH_ATTACK_PERCENT = 100;
    inline constexpr std::chrono::milliseconds SLASH_COOLDOWN{1000};
    inline constexpr std::uint32_t SLASH_RANGE = 12;
    inline constexpr SkillId ARCANE_BOLT{.value = 2};
    inline constexpr std::uint64_t ARCANE_BOLT_ATTACK_PERCENT = 160;
    inline constexpr std::chrono::milliseconds ARCANE_BOLT_COOLDOWN{1500};
    inline constexpr std::uint32_t ARCANE_BOLT_ACQUISITION_RANGE = 40;
    inline constexpr std::uint32_t ARCANE_BOLT_SPEED_PER_TICK = 4;
    inline constexpr std::uint32_t ARCANE_BOLT_HIT_RANGE = 1;
    inline constexpr std::chrono::milliseconds ARCANE_BOLT_LIFETIME{3000};
    inline constexpr std::uint64_t ATTACK_PERCENT_DENOMINATOR = 100;

    [[nodiscard]] std::optional<SkillDefinition> findSkill(SkillId skill_id) noexcept;
    [[nodiscard]] std::uint64_t calculateSkillDamage(const SkillDefinition& skill, std::uint64_t attack) noexcept;
}

namespace
{
    using namespace snf::server;

    [[nodiscard]] SkillDefinition makeSlash() noexcept
    {
        return SkillDefinition{
            .skill_id = SLASH,
            .cooldown = SLASH_COOLDOWN,
            .behavior = AreaAttackBehavior{
                .attack_percent = SLASH_ATTACK_PERCENT,
                .range = SLASH_RANGE,
            },
        };
    }

    [[nodiscard]] SkillDefinition makeArcaneBolt() noexcept
    {
        return SkillDefinition{
            .skill_id = ARCANE_BOLT,
            .cooldown = ARCANE_BOLT_COOLDOWN,
            .behavior = HomingProjectileAttackBehavior{
                .attack_percent = ARCANE_BOLT_ATTACK_PERCENT,
                .acquisition_range = ARCANE_BOLT_ACQUISITION_RANGE,
                .speed_per_tick = ARCANE_BOLT_SPEED_PER_TICK,
                .hit_range = ARCANE_BOLT_HIT_RANGE,
                .lifetime = ARCANE_BOLT_LIFETIME,
            },
        };
    }

    [[nodiscard]] std::optional<SkillDefinition> findDefinition(const SkillId skill_id) noexcept
    {
        if (skill_id == SLASH)
        {
            return makeSlash();
        }
        if (skill_id == snf::server::ARCANE_BOLT)
        {
            return makeArcaneBolt();
        }
        return std::nullopt;
    }
}

namespace snf::server
{
    std::optional<SkillDefinition> findSkill(const SkillId skill_id) noexcept
    {
        return findDefinition(skill_id);
    }

    std::uint64_t calculateSkillDamage(const SkillDefinition& skill, const std::uint64_t attack) noexcept
    {
        return std::visit(
            [attack](const auto& behavior)
            {
                return attack * behavior.attack_percent / ATTACK_PERCENT_DENOMINATOR;
            },
            skill.behavior
        );
    }
}
