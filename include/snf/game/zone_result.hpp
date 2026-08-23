#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/zone_command.hpp"

#include <chrono>

#include <cstdint>
#include <optional>
#include <vector>

namespace snf::server
{
    enum class ZoneCommandStatus
    {
        Applied,
        AlreadyPresent,
        PlayerMissing,
        StaleRoute,
        TransitionInProgress,
        TransferFailed,
        InRoom,
    };

    struct ZoneResult
    {
        ZoneCommandStatus status{ZoneCommandStatus::Applied};
        std::optional<PlayerId> player;
        std::optional<ZonePosition> position;
        std::uint64_t route_epoch{0};
        std::uint64_t tick{0};
        std::vector<PlayerId> visible_players;
        std::optional<std::chrono::milliseconds> tick_after{};
    };
}
