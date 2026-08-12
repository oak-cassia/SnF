#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/zone_command.hpp"
#include "snf/server/zone_id.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    enum class ZoneReplyKind
    {
        Entered,
        Moved,
        Left,
    };

    struct ZoneReplyContext
    {
        snf::net::ConnectionId connection;
        std::uint32_t request_id{0};
        ZoneReplyKind kind{ZoneReplyKind::Moved};
    };

    struct ZoneInboundCommand
    {
        ZoneId zone;
        ZoneCommand command;
        std::optional<ZoneReplyContext> reply;
    };
}
