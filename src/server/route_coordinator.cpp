#include "snf/server/route_coordinator.hpp"

#include <limits>
#include <stdexcept>

namespace snf::server
{
    RouteCoordinator::RouteCoordinator(const std::size_t max_handoffs)
        : _max_handoffs(max_handoffs)
    {
        if (_max_handoffs == 0)
        {
            throw std::invalid_argument{"Route handoff capacity must be positive"};
        }
        _handoffs.reserve(_max_handoffs);
    }

    std::optional<RouteAdmission> RouteCoordinator::tryEnter(const snf::net::ConnectionId connection, const PlayerId player, const ZoneId zone)
    {
        if (zone.value == 0)
        {
            return std::nullopt;
        }

        std::lock_guard lock{_mutex};
        if (_handoffs.contains(connection))
        {
            return std::nullopt;
        }
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

    std::optional<ZoneHandoff> RouteCoordinator::tryBeginHandoff(const snf::net::ConnectionId connection,
                                                                 const PlayerId player,
                                                                 const ZoneId target_zone,
                                                                 const ZonePosition source_position,
                                                                 const ZonePosition requested_target_position,
                                                                 const std::uint32_t request_id)
    {
        std::lock_guard lock{_mutex};
        const auto source = _routes.find(connection);
        if (target_zone.value == 0 || source == _routes.end() || source->second.player != player || source->second.zone == target_zone || _handoffs.contains(connection) ||
            _handoffs.size() == _max_handoffs || _next_handoff_id == std::numeric_limits<std::uint64_t>::max())
        {
            ++_handoffs_rejected;
            return std::nullopt;
        }

        std::uint64_t& last_epoch = _last_epoch[player];
        if (last_epoch == std::numeric_limits<std::uint64_t>::max())
        {
            throw std::overflow_error{"Player route epoch exhausted"};
        }
        const std::uint64_t target_epoch = ++last_epoch;
        ZoneHandoff handoff{
            .id = ZoneHandoffId{.value = _next_handoff_id++},
            .source = source->second,
            .target_zone = target_zone,
            .target_epoch = target_epoch,
            .restore_epoch = 0,
            .last_source_position = source_position,
            .requested_target_position = requested_target_position,
            .request_id = request_id,
            .step = ZoneHandoffStep::LeaveSource,
        };
        _handoffs.emplace(connection, handoff);
        ++_handoffs_started;
        _handoff_high_water_mark = std::max(_handoff_high_water_mark, _handoffs.size());
        return handoff;
    }

    std::optional<ZoneHandoff> RouteCoordinator::handoffFor(const snf::net::ConnectionId connection) const
    {
        std::lock_guard lock{_mutex};
        const auto handoff = _handoffs.find(connection);
        return handoff == _handoffs.end() ? std::nullopt : std::optional{handoff->second};
    }

    bool RouteCoordinator::noteSourceLeft(const snf::net::ConnectionId connection, const ZoneHandoffId handoff, const ZonePosition position) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findHandoff(connection, handoff);
        if (iterator == _handoffs.end() || iterator->second.step != ZoneHandoffStep::LeaveSource)
        {
            return false;
        }
        iterator->second.last_source_position = position;
        iterator->second.step = ZoneHandoffStep::EnterTarget;
        return true;
    }

    std::optional<std::uint64_t> RouteCoordinator::beginSourceRestore(const snf::net::ConnectionId connection, const ZoneHandoffId handoff)
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findHandoff(connection, handoff);
        if (iterator == _handoffs.end() || iterator->second.step != ZoneHandoffStep::EnterTarget)
        {
            return std::nullopt;
        }
        std::uint64_t& last_epoch = _last_epoch[iterator->second.source.player];
        if (last_epoch == std::numeric_limits<std::uint64_t>::max())
        {
            throw std::overflow_error{"Player route epoch exhausted"};
        }
        iterator->second.restore_epoch = ++last_epoch;
        iterator->second.step = ZoneHandoffStep::RestoreSource;
        return iterator->second.restore_epoch;
    }

    bool RouteCoordinator::beginCleanup(const snf::net::ConnectionId connection, const ZoneHandoffId handoff, const ZoneHandoffStep cleanup_step) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findHandoff(connection, handoff);
        if (iterator == _handoffs.end())
        {
            return false;
        }
        const bool cleans_target = cleanup_step == ZoneHandoffStep::CleanupTarget && iterator->second.step == ZoneHandoffStep::EnterTarget;
        const bool cleans_source = cleanup_step == ZoneHandoffStep::CleanupSource && iterator->second.step == ZoneHandoffStep::RestoreSource;
        if (!cleans_target && !cleans_source)
        {
            return false;
        }
        iterator->second.step = cleanup_step;
        return true;
    }

    std::optional<SessionRoute> RouteCoordinator::completeTargetEnter(const snf::net::ConnectionId connection, const ZoneHandoffId handoff) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findHandoff(connection, handoff);
        if (iterator == _handoffs.end() || iterator->second.step != ZoneHandoffStep::EnterTarget)
        {
            return std::nullopt;
        }
        const SessionRoute route{
            .connection = iterator->second.source.connection,
            .player = iterator->second.source.player,
            .zone = iterator->second.target_zone,
            .route_epoch = iterator->second.target_epoch,
        };
        _routes.insert_or_assign(route.connection, route);
        _handoffs.erase(iterator);
        ++_handoffs_completed;
        return route;
    }

    std::optional<SessionRoute> RouteCoordinator::completeSourceRestore(const snf::net::ConnectionId connection, const ZoneHandoffId handoff) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findHandoff(connection, handoff);
        if (iterator == _handoffs.end() || iterator->second.step != ZoneHandoffStep::RestoreSource || iterator->second.restore_epoch == 0)
        {
            return std::nullopt;
        }
        const SessionRoute route{
            .connection = iterator->second.source.connection,
            .player = iterator->second.source.player,
            .zone = iterator->second.source.zone,
            .route_epoch = iterator->second.restore_epoch,
        };
        _routes.insert_or_assign(route.connection, route);
        _handoffs.erase(iterator);
        ++_handoffs_restored;
        return route;
    }

    bool RouteCoordinator::rollbackHandoffBeforeLeave(const snf::net::ConnectionId connection, const ZoneHandoffId handoff) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findHandoff(connection, handoff);
        if (iterator == _handoffs.end() || iterator->second.step != ZoneHandoffStep::LeaveSource)
        {
            return false;
        }
        _handoffs.erase(iterator);
        ++_handoffs_rolled_back;
        return true;
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

    std::optional<SessionRoute> RouteCoordinator::routeFor(const snf::net::ConnectionId connection) const
    {
        std::lock_guard lock{_mutex};
        if (_handoffs.contains(connection))
        {
            return std::nullopt;
        }
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
            if (!_handoffs.contains(connection) && route.zone == zone)
            {
                ++count;
            }
        }
        for (const auto& [connection, handoff] : _handoffs)
        {
            static_cast<void>(connection);
            if (handoff.source.zone == zone || handoff.target_zone == zone)
            {
                ++count;
            }
        }
        return count;
    }

    RouteCoordinatorStats RouteCoordinator::stats() const
    {
        std::lock_guard lock{_mutex};
        return RouteCoordinatorStats{
            .handoffs_started = _handoffs_started,
            .handoffs_completed = _handoffs_completed,
            .handoffs_restored = _handoffs_restored,
            .handoffs_rolled_back = _handoffs_rolled_back,
            .handoffs_abandoned = _handoffs_abandoned,
            .handoffs_rejected = _handoffs_rejected,
            .pending_handoffs = _handoffs.size(),
            .handoff_high_water_mark = _handoff_high_water_mark,
        };
    }

    void RouteCoordinator::completeLeave(const SessionRoute& route) noexcept
    {
        std::lock_guard lock{_mutex};
        if (_handoffs.contains(route.connection))
        {
            return;
        }
        const auto iterator = _routes.find(route.connection);
        if (iterator != _routes.end() && iterator->second == route)
        {
            _routes.erase(iterator);
        }
    }

    void RouteCoordinator::abandon(const snf::net::ConnectionId connection) noexcept
    {
        std::lock_guard lock{_mutex};
        if (_handoffs.erase(connection) != 0)
        {
            ++_handoffs_abandoned;
        }
        _routes.erase(connection);
    }

    RouteCoordinator::HandoffMap::iterator RouteCoordinator::findHandoff(const snf::net::ConnectionId connection, const ZoneHandoffId handoff)
    {
        const auto iterator = _handoffs.find(connection);
        return iterator != _handoffs.end() && iterator->second.id == handoff ? iterator : _handoffs.end();
    }
}
