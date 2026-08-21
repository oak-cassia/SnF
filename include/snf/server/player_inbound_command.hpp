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
        // Every Player command answers a client frame, so this is a plain value
        // rather than the optional Zone and Party carry for timer-made commands.
        std::uint32_t request_id{0};
        // The room entry saga's identity, when this command is the snapshot request one
        // started. The binding pairs it with what the handler returns; the Player never
        // sees it.
        std::optional<RoomEntryContext> room_entry{};
    };
}
