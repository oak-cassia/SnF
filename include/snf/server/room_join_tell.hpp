#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/room_join_request.hpp"
#include "snf/server/room_entry.hpp"
#include "snf/server/room_inbound_command.hpp"

namespace snf::server
{
    // What a Player's binding sends to a Room's binding when the Player has decided
    // to enter. The game half is RoomJoinRequest; the rest is server context the
    // domain has no business naming -- which player is behind the request, and where
    // the answer goes. The Room answers, not the Player, so the reply travels here.
    struct RoomJoinTell
    {
        PlayerId player{};
        RoomJoinRequest request{};
        RoomReplyContext reply{};
        std::optional<RoomEntryContext> entry{};
    };
}
