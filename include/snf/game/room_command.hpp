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
        // Taken when the Player decided to enter, and fixed for the battle. The Room
        // never asks the Player for it, which is what keeps the two from waiting on
        // each other.
        CombatStats stats{};
    };

    // Leaving before the battle frees the seat; leaving during one forfeits the
    // reward, because a clear pays only the participants the Room still holds. Both
    // are the same removal, which is why an explicit leave and a disconnect do not
    // need separate commands.
    struct LeaveRoom
    {
        PlayerId player{};
    };

    struct StartBattle
    {
    };

    // The client names a skill; the Room decides everything else. request_sequence
    // is the caster's own counter for this battle and has to increase: anything at
    // or below the highest sequence already applied is a resend of a cast that
    // landed, not a new one.
    struct UseSkill
    {
        PlayerId player{};
        SkillId skill{};
        std::uint64_t request_sequence{0};
    };

    // Posted by the Room's own deadline timer, never by a client. It carries no
    // outcome because reaching it *is* the outcome: the boss outlived the battle.
    struct BattleDeadline
    {
    };

    // A one-shot timer command posted back to the same Room by its binding. A
    // Running Room asks for the next one in the result of the current tick.
    struct RoomSimulationTick
    {
    };

    // A separate sequence lets movement and skill input progress independently.
    // The intent changes now; the Room's next simulation Tick changes position.
    struct SetMoveIntent
    {
        PlayerId player{};
        MoveDirection direction{MoveDirection::Stop};
        std::uint64_t request_sequence{0};
    };

    using RoomCommand = std::variant<JoinRoom, LeaveRoom, StartBattle, UseSkill, BattleDeadline, RoomSimulationTick, SetMoveIntent>;
}
