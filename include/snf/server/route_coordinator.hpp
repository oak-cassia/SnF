#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/player_id.hpp"
#include "snf/server/zone_id.hpp"

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

    // Authoritative owner of the current Player -> Zone destination. The first
    // slice supports one active Zone and rejects cross-Zone transfer; transfer's
    // stop-old/activate-new/publish-new protocol is added when a second Zone is
    // exposed to gameplay.
    class RouteCoordinator
    {
    public:
        [[nodiscard]] std::optional<RouteAdmission>
        tryEnter(snf::net::ConnectionId connection, PlayerId player, ZoneId zone);
        void rollbackEnter(const RouteAdmission& admission) noexcept;

        [[nodiscard]] std::optional<SessionRoute> routeFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] std::size_t routeCountFor(ZoneId zone) const;
        void completeLeave(const SessionRoute& route) noexcept;
        void abandon(snf::net::ConnectionId connection) noexcept;

    private:
        mutable std::mutex _mutex;
        std::unordered_map<snf::net::ConnectionId, SessionRoute, snf::net::ConnectionIdHash>
            _routes;
        std::unordered_map<PlayerId, std::uint64_t, PlayerIdHash> _last_epoch;
    };
}
