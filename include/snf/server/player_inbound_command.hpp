#pragma once

#include "snf/game/player_command.hpp"
#include "snf/net/connection_id.hpp"
#include "snf/server/player_actor_id.hpp"
#include "snf/server/room_entry.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    struct PlayerInboundCommand
    {
        PlayerActorId actor;
        snf::net::ConnectionId connection;
        PlayerCommand command;
        std::uint32_t request_id{0};
        std::optional<RoomEntryContext> room_entry{};
    };
}
