#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/room_command.hpp"
#include "snf/server/room_id.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    enum class RoomReplyKind
    {
        Joined,
        BattleStarted,
    };

    struct RoomReplyContext
    {
        snf::net::ConnectionId connection;
        std::uint32_t request_id{0};
        RoomReplyKind kind{RoomReplyKind::Joined};
    };

    struct RoomInboundCommand
    {
        RoomId room;
        RoomCommand command;
        // Absent when nothing asked for this command. BattleCompleted arrives from
        // the Room's own timer, so there is no request to answer -- the same reason
        // ZoneSimulationTick rides with no reply context.
        std::optional<RoomReplyContext> reply;
    };
}
