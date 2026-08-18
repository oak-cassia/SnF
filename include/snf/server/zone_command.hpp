#pragma once

#include "snf/server/player_id.hpp"

#include <cstdint>
#include <variant>

namespace snf::server
{
    struct ZonePosition
    {
        std::int32_t x{0};
        std::int32_t y{0};

        [[nodiscard]] bool operator==(const ZonePosition&) const noexcept = default;
    };

    struct EnterZoneCommand
    {
        PlayerId player;
        std::uint64_t route_epoch{0};
        ZonePosition position;
    };

    struct LeaveZoneCommand
    {
        PlayerId player;
        std::uint64_t route_epoch{0};
    };

    struct MoveInZoneCommand
    {
        PlayerId player;
        std::uint64_t route_epoch{0};
        ZonePosition position;
    };

    struct ZoneSimulationTick
    {
    };

    using ZoneCommand =
        std::variant<EnterZoneCommand, LeaveZoneCommand, MoveInZoneCommand, ZoneSimulationTick>;
}
