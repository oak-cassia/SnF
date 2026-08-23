#pragma once

#include "snf/game/arena.hpp"
#include "snf/game/player_id.hpp"
#include "snf/game/skill_id.hpp"
#include "snf/game/street_progression.hpp"

#include <cstdint>
#include <variant>

namespace snf::server
{
    struct JoinRoom
    {
        PlayerId player{};
        CombatStats stats{};
    };

    struct LeaveRoom
    {
        PlayerId player{};
    };

    struct StartBattle
    {
    };

    struct UseSkill
    {
        PlayerId player{};
        SkillId skill{};
        std::uint64_t request_sequence{0};
    };

    struct BattleDeadline
    {
    };

    struct RoomSimulationTick
    {
    };

    struct SetMoveIntent
    {
        PlayerId player{};
        MoveDirection direction{MoveDirection::Stop};
        std::uint64_t request_sequence{0};
    };

    using RoomCommand = std::variant<JoinRoom, LeaveRoom, StartBattle, UseSkill, BattleDeadline, RoomSimulationTick, SetMoveIntent>;
}
