#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/room_join_request.hpp"
#include "snf/server/room_entry.hpp"
#include "snf/server/room_inbound_command.hpp"

namespace snf::server
{
    struct RoomJoinTell
    {
        PlayerId player{};
        RoomJoinRequest request{};
        RoomReplyContext reply{};
        std::optional<RoomEntryContext> entry{};
    };
}
