#pragma once

#include "snf/game/player_id.hpp"

#include <variant>

namespace snf::server
{
    struct JoinRoom
    {
        PlayerId player;
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
