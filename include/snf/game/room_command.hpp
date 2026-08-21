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

    // Posted by the Room's own battle timer, never by a client. It carries no
    // outcome yet: combat is a placeholder that always clears.
    struct BattleCompleted
    {
    };

    using RoomCommand = std::variant<JoinRoom, LeaveRoom, StartBattle, BattleCompleted>;
}
