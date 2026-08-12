#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/player_actor_id.hpp"
#include "snf/server/player_command.hpp"

namespace snf::server
{
    struct PlayerInboundCommand
    {
        PlayerActorId actor;
        snf::net::ConnectionId connection;
        PlayerCommand command;
    };
}
