#pragma once

#include "snf/server/player_id.hpp"
#include "snf/server/zone_command.hpp"
#include "snf/server/zone_id.hpp"
#include "snf/server/zone_result.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>

namespace snf::server
{
    struct ZoneActorConfig
    {
        std::int32_t aoi_radius{1000};
    };

    class ZoneActor
    {
    public:
        explicit ZoneActor(ZoneId zone, ZoneActorConfig config = {});

        [[nodiscard]] ZoneId id() const noexcept;
        [[nodiscard]] std::size_t playerCount() const noexcept;
        [[nodiscard]] std::uint64_t lastTick() const noexcept;
        [[nodiscard]] std::optional<TimerId> activeTimer() const noexcept;
        [[nodiscard]] std::optional<ZonePosition> positionOf(PlayerId player) const;
        [[nodiscard]] std::vector<PlayerId> visiblePlayers(PlayerId player) const;

        [[nodiscard]] ZoneResult handle(const ZoneCommand& command);

    private:
        struct Entity
        {
            ZonePosition position;
            std::uint64_t route_epoch{0};
        };

        [[nodiscard]] ZoneResult handleCommand(const EnterZoneCommand& command);
        [[nodiscard]] ZoneResult handleCommand(const LeaveZoneCommand& command);
        [[nodiscard]] ZoneResult handleCommand(const MoveInZoneCommand& command);
        [[nodiscard]] ZoneResult handleCommand(const ArmZoneSimulationTimer& command);
        [[nodiscard]] ZoneResult handleCommand(const CancelZoneSimulationTimer& command);
        [[nodiscard]] ZoneResult handleCommand(const ZoneSimulationTick& command);
        [[nodiscard]] bool isVisible(ZonePosition left, ZonePosition right) const noexcept;

        ZoneId _zone;
        std::int32_t _aoi_radius;
        std::uint64_t _last_tick{0};
        std::optional<TimerId> _active_timer;
        std::unordered_map<PlayerId, Entity, PlayerIdHash> _players;
    };
}
