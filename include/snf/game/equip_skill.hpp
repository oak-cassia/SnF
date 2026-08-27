#pragma once

#include "snf/game/skill_id.hpp"

#include <cstdint>

namespace snf::server
{
    enum class EquipSkillStatus : std::uint8_t
    {
        Equipped = 0,
        AlreadyEquipped = 1,
        SkillNotOwned = 2,
        UnknownSkill = 3,
    };

    struct EquipSkillCommand
    {
        SkillId skill_id{};
    };

    struct EquipSkillResponse
    {
        EquipSkillStatus status{EquipSkillStatus::UnknownSkill};
        SkillId equipped_skill_id{};
    };
}
