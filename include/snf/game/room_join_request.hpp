#pragma once

#include "snf/game/room_id.hpp"
#include "snf/game/skill_id.hpp"
#include "snf/game/street_progression.hpp"

namespace snf::server
{
    struct RoomJoinRequest
    {
        RoomId room{};
        CombatStats stats{};
        SkillId equipped_skill_id{SLASH_SKILL_ID};

        [[nodiscard]] bool operator==(const RoomJoinRequest&) const noexcept = default;
    };
}
