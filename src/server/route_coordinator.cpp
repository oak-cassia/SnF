#include "snf/server/route_coordinator.hpp"

#include <limits>
#include <stdexcept>

namespace snf::server
{
    std::optional<RouteAdmission> RouteCoordinator::tryEnter(
        const snf::net::ConnectionId connection, const PlayerId player, const ZoneId zone)
    {
        if (zone.value == 0)
        {
            return std::nullopt;
        }

        std::lock_guard lock{_mutex};
        const auto existing = _routes.find(connection);
        if (existing != _routes.end())
        {
            if (existing->second.player != player || existing->second.zone != zone)
            {
                return std::nullopt;
            }
            return RouteAdmission{.route = existing->second, .created = false};
        }

        std::uint64_t& last_epoch = _last_epoch[player];
        if (last_epoch == std::numeric_limits<std::uint64_t>::max())
        {
            throw std::overflow_error{"Player route epoch exhausted"};
        }
        ++last_epoch;

        const SessionRoute route{
            .connection = connection,
            .player = player,
            .zone = zone,
            .route_epoch = last_epoch,
        };
        _routes.emplace(connection, route);
        return RouteAdmission{.route = route, .created = true};
    }

    void RouteCoordinator::rollbackEnter(const RouteAdmission& admission) noexcept
    {
        if (!admission.created)
        {
            return;
        }

        std::lock_guard lock{_mutex};
        const auto iterator = _routes.find(admission.route.connection);
        if (iterator != _routes.end() && iterator->second == admission.route)
        {
            _routes.erase(iterator);
        }
    }

    std::optional<SessionRoute>
    RouteCoordinator::routeFor(const snf::net::ConnectionId connection) const
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _routes.find(connection);
        if (iterator == _routes.end())
        {
            return std::nullopt;
        }
        return iterator->second;
    }

    std::size_t RouteCoordinator::routeCountFor(const ZoneId zone) const
    {
        std::lock_guard lock{_mutex};
        std::size_t count = 0;
        for (const auto& [connection, route] : _routes)
        {
            static_cast<void>(connection);
            if (route.zone == zone)
            {
                ++count;
            }
        }
        return count;
    }

    void RouteCoordinator::completeLeave(const SessionRoute& route) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _routes.find(route.connection);
        if (iterator != _routes.end() && iterator->second == route)
        {
            _routes.erase(iterator);
        }
    }

    void RouteCoordinator::abandon(const snf::net::ConnectionId connection) noexcept
    {
        std::lock_guard lock{_mutex};
        _routes.erase(connection);
    }
}
