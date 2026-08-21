#pragma once

#include "snf/game/room_id.hpp"
#include "snf/game/street_progression.hpp"

namespace snf::server
{
    // A Player asking to enter a Room, carrying the combat snapshot it is entering
    // with. The stats come from the Player rather than from the client or a cache:
    // the Player owns the experience they derive from, so reading them anywhere else
    // means reading a copy that can be a battle behind.
    struct RoomJoinRequest
    {
        RoomId room{};
        CombatStats stats{};

        [[nodiscard]] bool operator==(const RoomJoinRequest&) const noexcept = default;
    };
}
