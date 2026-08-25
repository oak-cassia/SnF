#include "snf/game/skill_id.hpp"

#include <cassert>
#include <chrono>
#include <variant>

import snf.game.skill_catalog;

namespace
{
    using snf::server::ARCANE_BOLT;
    using snf::server::AreaAttackBehavior;
    using snf::server::HomingProjectileAttackBehavior;
    using snf::server::SLASH;

    void test_catalog_defines_slash_as_an_area_attack()
    {
        const auto definition = snf::server::findSkill(SLASH);
        assert(definition);
        assert(definition->skill == SLASH);
        assert(definition->cooldown == snf::server::SLASH_COOLDOWN);

        const auto* area = std::get_if<AreaAttackBehavior>(&definition->behavior);
        assert(area != nullptr);
        assert(area->attack_percent == snf::server::SLASH_ATTACK_PERCENT);
        assert(area->range == snf::server::SLASH_RANGE);
        assert(snf::server::skillDamage(*definition, 37) == 37);
    }

    void test_catalog_defines_arcane_bolt_as_a_projectile_attack()
    {
        const auto definition = snf::server::findSkill(ARCANE_BOLT);
        assert(definition);
        assert(definition->skill == ARCANE_BOLT);
        assert(definition->cooldown == snf::server::ARCANE_BOLT_COOLDOWN);

        const auto* projectile = std::get_if<HomingProjectileAttackBehavior>(&definition->behavior);
        assert(projectile != nullptr);
        assert(projectile->attack_percent == snf::server::ARCANE_BOLT_ATTACK_PERCENT);
        assert(projectile->acquisition_range == snf::server::ARCANE_BOLT_ACQUISITION_RANGE);
        assert(projectile->speed_per_tick == snf::server::ARCANE_BOLT_SPEED_PER_TICK);
        assert(projectile->hit_range == snf::server::ARCANE_BOLT_HIT_RANGE);
        assert(projectile->lifetime == snf::server::ARCANE_BOLT_LIFETIME);
        assert(snf::server::skillDamage(*definition, 37) == 59);
    }

    void test_catalog_rejects_unknown_skills()
    {
        assert(!snf::server::findSkill(snf::server::SkillId{.value = 999}));
    }
}

void run_skill_catalog_tests()
{
    test_catalog_defines_slash_as_an_area_attack();
    test_catalog_defines_arcane_bolt_as_a_projectile_attack();
    test_catalog_rejects_unknown_skills();
}
