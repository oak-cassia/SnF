#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/connection_lifecycle.hpp"
#include "snf/server/player_actor_id.hpp"
#include "snf/server/player_command.hpp"

#include <variant>

namespace snf::server
{
    // Target and command live in the same variant alternative, preventing an
    // invalid pairing such as a PlayerCommand addressed to a future Zone target.
    struct PlayerCommandRoute
    {
        PlayerActorId actor;
        PlayerCommand command;
    };

    struct ConnectionClosedRoute
    {
        PlayerActorId actor;
        ConnectionCloseCause cause;
    };

    using CommandRoute = std::variant<PlayerCommandRoute, ConnectionClosedRoute>;

    struct RoutedCommand
    {
        snf::net::ConnectionId connection;
        CommandRoute route;
    };
}
