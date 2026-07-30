#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/player_command.hpp"
#include "snf/server/provisional_actor_id.hpp"

namespace snf::server
{
    struct PlayerInboundCommand
    {
        ProvisionalActorId actor;
        snf::net::ConnectionId connection;
        PlayerCommand command;
    };
}
