#include "snf/game/skill_loadout.hpp"

#include <cassert>
#include <stdexcept>
#include <vector>

namespace
{
    using snf::server::ARCANE_BOLT_SKILL_ID;
    using snf::server::AddOwnedSkillResult;
    using snf::server::EquipSkillResult;
    using snf::server::SLASH_SKILL_ID;
    using snf::server::SkillId;
    using snf::server::SkillLoadout;

    void test_default_loadout_owns_and_equips_slash()
    {
        const SkillLoadout loadout;

        assert(loadout.getOwnedSkillIds().size() == 1);
        assert(loadout.getOwnedSkillIds().front() == SLASH_SKILL_ID);
        assert(loadout.getEquippedSkillId() == SLASH_SKILL_ID);
        assert(loadout.hasOwnedSkillId(SLASH_SKILL_ID));
    }

    void test_owned_skill_ids_stay_sorted_and_unique()
    {
        SkillLoadout loadout;

        assert(loadout.addOwnedSkillId(ARCANE_BOLT_SKILL_ID) == AddOwnedSkillResult::Added);
        assert(loadout.addOwnedSkillId(ARCANE_BOLT_SKILL_ID) == AddOwnedSkillResult::AlreadyOwned);
        assert(loadout.addOwnedSkillId(SkillId{.value = 999}) == AddOwnedSkillResult::UnknownSkill);

        const auto owned_skill_ids = loadout.getOwnedSkillIds();
        assert(owned_skill_ids.size() == 2);
        assert(owned_skill_ids[0] == SLASH_SKILL_ID);
        assert(owned_skill_ids[1] == ARCANE_BOLT_SKILL_ID);
    }

    void test_equipping_requires_a_known_owned_skill_id()
    {
        SkillLoadout loadout;

        assert(loadout.equipSkillId(ARCANE_BOLT_SKILL_ID) == EquipSkillResult::SkillNotOwned);
        assert(loadout.equipSkillId(SkillId{.value = 999}) == EquipSkillResult::UnknownSkill);
        assert(loadout.addOwnedSkillId(ARCANE_BOLT_SKILL_ID) == AddOwnedSkillResult::Added);
        assert(loadout.equipSkillId(ARCANE_BOLT_SKILL_ID) == EquipSkillResult::Equipped);
        assert(loadout.equipSkillId(ARCANE_BOLT_SKILL_ID) == EquipSkillResult::AlreadyEquipped);
        assert(loadout.getEquippedSkillId() == ARCANE_BOLT_SKILL_ID);
    }

    void test_restored_loadout_rejects_broken_invariants()
    {
        bool duplicate_rejected = false;
        try
        {
            static_cast<void>(SkillLoadout{{SLASH_SKILL_ID, SLASH_SKILL_ID}, SLASH_SKILL_ID});
        }
        catch (const std::invalid_argument&)
        {
            duplicate_rejected = true;
        }
        assert(duplicate_rejected);

        bool unowned_equipped_rejected = false;
        try
        {
            static_cast<void>(SkillLoadout{{SLASH_SKILL_ID}, ARCANE_BOLT_SKILL_ID});
        }
        catch (const std::invalid_argument&)
        {
            unowned_equipped_rejected = true;
        }
        assert(unowned_equipped_rejected);
    }
}

void run_skill_loadout_tests()
{
    test_default_loadout_owns_and_equips_slash();
    test_owned_skill_ids_stay_sorted_and_unique();
    test_equipping_requires_a_known_owned_skill_id();
    test_restored_loadout_rejects_broken_invariants();
}
