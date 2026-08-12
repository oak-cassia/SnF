#pragma once

#include "snf/server/zone_command.hpp"
#include "snf/server/zone_id.hpp"

namespace snf::server
{
    struct ZoneInboundCommand
    {
        ZoneId zone;
        ZoneCommand command;
    };
}
