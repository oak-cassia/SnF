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
        // How long until this Zone wants its next tick. The binding turns it into a
        // timer; the Zone itself never names one.
        std::optional<std::chrono::milliseconds> tick_after;
    };
}
