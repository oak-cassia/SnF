#pragma once

#include "snf/server/zone_command.hpp"
#include "snf/server/zone_id.hpp"

namespace snf::server
{
    struct PlayerLocation
    {
        ZoneId zone;
        ZonePosition position;

        [[nodiscard]] bool operator==(const PlayerLocation&) const noexcept = default;
    };
}
