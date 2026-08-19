#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/street_progression.hpp"

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

    struct StartBattle
    {
    };

    // Posted by the Room's own battle timer, never by a client. It carries no
    // outcome yet: combat is a placeholder that always clears.
    struct BattleCompleted
    {
    };

    using RoomCommand = std::variant<JoinRoom, StartBattle, BattleCompleted>;
}
