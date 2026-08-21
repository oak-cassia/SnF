#pragma once

#include "snf/game/zone_command.hpp"
#include "snf/game/zone_id.hpp"

namespace snf::server
{
    struct PlayerLocation
    {
        ZoneId zone;
        ZonePosition position;

        [[nodiscard]] bool operator==(const PlayerLocation&) const noexcept = default;
    };
}
