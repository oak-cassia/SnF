#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/player_actor_id.hpp"
#include "snf/server/player_command.hpp"

#include <cstdint>

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
    };
}
