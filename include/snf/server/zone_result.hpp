#pragma once

#include "snf/server/player_id.hpp"
#include "snf/server/zone_command.hpp"

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
    };

    struct TimerRequest
    {
        std::chrono::milliseconds delay{0};
    };

    struct ZoneResult
    {
        ZoneCommandStatus status{ZoneCommandStatus::Applied};
        std::optional<PlayerId> player;
        std::optional<ZonePosition> position;
        std::uint64_t route_epoch{0};
        std::uint64_t tick{0};
        // Deterministic ascending PlayerId order, excluding the requesting player.
        std::vector<PlayerId> visible_players;
        std::optional<TimerRequest> timer;
    };
}
