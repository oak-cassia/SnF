#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/player_id.hpp"
#include "snf/server/zone_command.hpp"
#include "snf/server/zone_handoff.hpp"
#include "snf/server/zone_id.hpp"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace snf::server
{
    struct SessionRoute
    {
        snf::net::ConnectionId connection;
        PlayerId player;
        ZoneId zone;
        std::uint64_t route_epoch{0};

        [[nodiscard]] bool operator==(const SessionRoute&) const noexcept = default;
    };

    struct RouteAdmission
    {
        SessionRoute route;
        bool created{false};
    };

    struct ZoneHandoff
    {
        ZoneHandoffId id;
        SessionRoute source;
        ZoneId target_zone;
        std::uint64_t target_epoch{0};
        std::uint64_t restore_epoch{0};
        ZonePosition last_source_position;
        ZonePosition requested_target_position;
        std::uint32_t request_id{0};
        ZoneHandoffStep step{ZoneHandoffStep::LeaveSource};

        [[nodiscard]] bool operator==(const ZoneHandoff&) const noexcept = default;
    };

    struct RouteCoordinatorStats
    {
        std::uint64_t handoffs_started{0};
        std::uint64_t handoffs_completed{0};
        std::uint64_t handoffs_restored{0};
        std::uint64_t handoffs_rolled_back{0};
        std::uint64_t handoffs_abandoned{0};
        std::uint64_t handoffs_rejected{0};
        std::size_t pending_handoffs{0};
        std::size_t handoff_high_water_mark{0};
    };

    // Reactor-owned authority for the stable Player -> Zone destination and the
    // hidden transition record used while a cross-Zone handoff is in progress.
    class RouteCoordinator
    {
    public:
        explicit RouteCoordinator(std::size_t max_handoffs = 4096);

        [[nodiscard]] std::optional<RouteAdmission>
        tryEnter(snf::net::ConnectionId connection, PlayerId player, ZoneId zone);
        void rollbackEnter(const RouteAdmission& admission) noexcept;

        [[nodiscard]] std::optional<ZoneHandoff>
        tryBeginHandoff(snf::net::ConnectionId connection,
                        PlayerId player,
                        ZoneId target_zone,
                        ZonePosition source_position,
                        ZonePosition requested_target_position,
                        std::uint32_t request_id);
        [[nodiscard]] std::optional<ZoneHandoff>
        handoffFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] bool noteSourceLeft(snf::net::ConnectionId connection,
                                          ZoneHandoffId handoff,
                                          ZonePosition position) noexcept;
        [[nodiscard]] std::optional<std::uint64_t>
        beginSourceRestore(snf::net::ConnectionId connection, ZoneHandoffId handoff);
        [[nodiscard]] bool beginCleanup(snf::net::ConnectionId connection,
                                        ZoneHandoffId handoff,
                                        ZoneHandoffStep cleanup_step) noexcept;
        [[nodiscard]] std::optional<SessionRoute>
        completeTargetEnter(snf::net::ConnectionId connection, ZoneHandoffId handoff) noexcept;
        [[nodiscard]] std::optional<SessionRoute>
        completeSourceRestore(snf::net::ConnectionId connection, ZoneHandoffId handoff) noexcept;
        [[nodiscard]] bool rollbackHandoffBeforeLeave(snf::net::ConnectionId connection,
                                                      ZoneHandoffId handoff) noexcept;

        [[nodiscard]] std::optional<SessionRoute> routeFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] std::size_t routeCountFor(ZoneId zone) const;
        [[nodiscard]] RouteCoordinatorStats stats() const;
        void completeLeave(const SessionRoute& route) noexcept;
        void abandon(snf::net::ConnectionId connection) noexcept;

    private:
        using HandoffMap =
            std::unordered_map<snf::net::ConnectionId, ZoneHandoff, snf::net::ConnectionIdHash>;

        [[nodiscard]] HandoffMap::iterator findHandoff(snf::net::ConnectionId connection,
                                                       ZoneHandoffId handoff);

        const std::size_t _max_handoffs;
        mutable std::mutex _mutex;
        std::unordered_map<snf::net::ConnectionId, SessionRoute, snf::net::ConnectionIdHash>
            _routes;
        HandoffMap _handoffs;
        std::unordered_map<PlayerId, std::uint64_t, PlayerIdHash> _last_epoch;
        std::uint64_t _next_handoff_id{1};
        std::uint64_t _handoffs_started{0};
        std::uint64_t _handoffs_completed{0};
        std::uint64_t _handoffs_restored{0};
        std::uint64_t _handoffs_rolled_back{0};
        std::uint64_t _handoffs_abandoned{0};
        std::uint64_t _handoffs_rejected{0};
        std::size_t _handoff_high_water_mark{0};
    };
}
