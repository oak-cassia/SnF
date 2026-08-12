#pragma once

#include "snf/server/player_id.hpp"
#include "snf/server/timer_id.hpp"

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
        TimerId timer;
        std::uint64_t tick{0};
    };

    struct ArmZoneSimulationTimer
    {
        TimerId timer;
    };

    struct CancelZoneSimulationTimer
    {
        TimerId timer;
    };

    using ZoneCommand = std::variant<EnterZoneCommand,
                                     LeaveZoneCommand,
                                     MoveInZoneCommand,
                                     ArmZoneSimulationTimer,
                                     CancelZoneSimulationTimer,
                                     ZoneSimulationTick>;
}
