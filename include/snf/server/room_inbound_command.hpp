#pragma once

#include "snf/game/room_command.hpp"
#include "snf/game/room_id.hpp"
#include "snf/net/connection_id.hpp"

#include "snf/server/room_entry.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    enum class RoomReplyKind
    {
        Joined,
        BattleStarted,
        SkillAcknowledged,
        MoveAcknowledged,
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
        std::optional<RoomReplyContext> reply;
        std::optional<RoomEntryContext> entry{};
    };
}
