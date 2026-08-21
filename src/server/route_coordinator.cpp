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
        _room_entries.reserve(_max_handoffs);
        _in_rooms.reserve(_max_handoffs);
        _room_returns.reserve(_max_handoffs);
    }

    std::optional<RouteAdmission> RouteCoordinator::tryEnter(const snf::net::ConnectionId connection, const PlayerId player, const ZoneId zone)
    {
        if (zone.value == 0)
        {
            return std::nullopt;
        }

        std::lock_guard lock{_mutex};
        if (_handoffs.contains(connection) || _room_entries.contains(connection) || _in_rooms.contains(connection) || _room_returns.contains(connection))
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
        if (target_zone.value == 0 || source == _routes.end() || source->second.player != player || source->second.zone == target_zone ||
            _handoffs.contains(connection) || _room_entries.contains(connection) || _in_rooms.contains(connection) || _room_returns.contains(connection) ||
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

    std::optional<RoomEntry> RouteCoordinator::tryBeginRoomEntry(const snf::net::ConnectionId connection,
                                                                 const PlayerId player,
                                                                 const RoomId room,
                                                                 const std::uint32_t request_id)
    {
        std::lock_guard lock{_mutex};
        const auto source = _routes.find(connection);
        if (room.value == 0 || source == _routes.end() || source->second.player != player ||
            _handoffs.contains(connection) || _room_entries.contains(connection) || _in_rooms.contains(connection) || _room_returns.contains(connection) ||
            _room_entries.size() == _max_handoffs || _next_room_entry_id == std::numeric_limits<std::uint64_t>::max())
        {
            ++_room_entries_rejected;
            return std::nullopt;
        }

        RoomEntry entry{
            .id = RoomEntryId{.value = _next_room_entry_id++},
            .source = source->second,
            .room = room,
            .request_id = request_id,
            .step = RoomEntryStep::RequestSnapshot,
        };
        _room_entries.emplace(connection, entry);
        ++_room_entries_started;
        _room_entry_high_water_mark = std::max(_room_entry_high_water_mark, _room_entries.size());
        return entry;
    }

    std::optional<RoomEntry> RouteCoordinator::roomEntryFor(const snf::net::ConnectionId connection) const
    {
        std::lock_guard lock{_mutex};
        const auto entry = _room_entries.find(connection);
        return entry == _room_entries.end() ? std::nullopt : std::optional{entry->second};
    }

    bool RouteCoordinator::noteRoomSnapshotReady(const snf::net::ConnectionId connection, const RoomEntryId entry) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findRoomEntry(connection, entry);
        if (iterator == _room_entries.end() || iterator->second.step != RoomEntryStep::RequestSnapshot)
        {
            return false;
        }
        iterator->second.step = RoomEntryStep::JoinRoom;
        return true;
    }

    bool RouteCoordinator::noteRoomJoined(const snf::net::ConnectionId connection, const RoomEntryId entry) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findRoomEntry(connection, entry);
        if (iterator == _room_entries.end() || (iterator->second.step != RoomEntryStep::JoinRoom && iterator->second.step != RoomEntryStep::RequestSnapshot))
        {
            return false;
        }
        iterator->second.step = RoomEntryStep::LeaveSource;
        return true;
    }

    std::optional<InRoomState> RouteCoordinator::completeRoomEntry(const snf::net::ConnectionId connection, const RoomEntryId entry, const ZonePosition return_position) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findRoomEntry(connection, entry);
        if (iterator == _room_entries.end())
        {
            return std::nullopt;
        }
        const InRoomState in_room{
            .connection = connection,
            .player = iterator->second.source.player,
            .room = iterator->second.room,
            .return_zone = iterator->second.source.zone,
            .return_position = return_position,
        };
        _routes.erase(connection);
        _room_entries.erase(iterator);
        _in_rooms.insert_or_assign(connection, in_room);
        ++_room_entries_completed;
        return in_room;
    }

    bool RouteCoordinator::rollbackRoomEntryBeforeLeave(const snf::net::ConnectionId connection, const RoomEntryId entry) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findRoomEntry(connection, entry);
        if (iterator == _room_entries.end())
        {
            return false;
        }
        _room_entries.erase(iterator);
        ++_room_entries_rolled_back;
        return true;
    }

    std::optional<InRoomState> RouteCoordinator::inRoomFor(const snf::net::ConnectionId connection) const
    {
        std::lock_guard lock{_mutex};
        const auto it = _in_rooms.find(connection);
        return it == _in_rooms.end() ? std::nullopt : std::optional{it->second};
    }

    std::size_t RouteCoordinator::inRoomCountFor(const RoomId room) const
    {
        std::lock_guard lock{_mutex};
        std::size_t count = 0;
        for (const auto& [conn, in_room] : _in_rooms)
        {
            static_cast<void>(conn);
            if (in_room.room == room)
            {
                ++count;
            }
        }
        for (const auto& [conn, entry] : _room_entries)
        {
            static_cast<void>(conn);
            if (entry.room == room)
            {
                ++count;
            }
        }
        return count;
    }

    std::optional<RoomReturn> RouteCoordinator::tryBeginRoomReturn(const snf::net::ConnectionId connection, const RoomId room)
    {
        std::lock_guard lock{_mutex};
        const auto in_room = _in_rooms.find(connection);
        if (in_room == _in_rooms.end() || in_room->second.room != room || _room_returns.contains(connection) ||
            _room_returns.size() == _max_handoffs || _next_room_return_id == std::numeric_limits<std::uint64_t>::max())
        {
            return std::nullopt;
        }

        std::uint64_t& last_epoch = _last_epoch[in_room->second.player];
        if (last_epoch == std::numeric_limits<std::uint64_t>::max())
        {
            throw std::overflow_error{"Player route epoch exhausted"};
        }
        const std::uint64_t return_epoch = ++last_epoch;

        RoomReturn ret{
            .id = RoomReturnId{.value = _next_room_return_id++},
            .connection = connection,
            .player = in_room->second.player,
            .room = in_room->second.room,
            .return_zone = in_room->second.return_zone,
            .return_epoch = return_epoch,
            .return_position = in_room->second.return_position,
        };
        _in_rooms.erase(in_room);
        _room_returns.emplace(connection, ret);
        ++_room_returns_started;
        _room_return_high_water_mark = std::max(_room_return_high_water_mark, _room_returns.size());
        return ret;
    }

    std::optional<RoomReturn> RouteCoordinator::roomReturnFor(const snf::net::ConnectionId connection) const
    {
        std::lock_guard lock{_mutex};
        const auto ret = _room_returns.find(connection);
        return ret == _room_returns.end() ? std::nullopt : std::optional{ret->second};
    }

    std::optional<SessionRoute> RouteCoordinator::completeRoomReturn(const snf::net::ConnectionId connection, const RoomReturnId return_id) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findRoomReturn(connection, return_id);
        if (iterator == _room_returns.end())
        {
            return std::nullopt;
        }
        const SessionRoute route{
            .connection = iterator->second.connection,
            .player = iterator->second.player,
            .zone = iterator->second.return_zone,
            .route_epoch = iterator->second.return_epoch,
        };
        _routes.insert_or_assign(route.connection, route);
        _room_returns.erase(iterator);
        ++_room_returns_completed;
        return route;
    }

    bool RouteCoordinator::abandonRoomReturn(const snf::net::ConnectionId connection, const RoomReturnId return_id) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = findRoomReturn(connection, return_id);
        if (iterator == _room_returns.end())
        {
            return false;
        }
        _room_returns.erase(iterator);
        ++_room_returns_abandoned;
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
        if (_handoffs.contains(connection) || _room_entries.contains(connection) || _room_returns.contains(connection))
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
            if (!_handoffs.contains(connection) && !_room_entries.contains(connection) && !_room_returns.contains(connection) && route.zone == zone)
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
        for (const auto& [connection, entry] : _room_entries)
        {
            static_cast<void>(connection);
            if (entry.source.zone == zone)
            {
                ++count;
            }
        }
        for (const auto& [connection, ret] : _room_returns)
        {
            static_cast<void>(connection);
            if (ret.return_zone == zone)
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
            .room_entries_started = _room_entries_started,
            .room_entries_completed = _room_entries_completed,
            .room_entries_rolled_back = _room_entries_rolled_back,
            .room_entries_abandoned = _room_entries_abandoned,
            .room_entries_rejected = _room_entries_rejected,
            .pending_room_entries = _room_entries.size(),
            .room_entry_high_water_mark = _room_entry_high_water_mark,
            .room_returns_started = _room_returns_started,
            .room_returns_completed = _room_returns_completed,
            .room_returns_abandoned = _room_returns_abandoned,
            .pending_room_returns = _room_returns.size(),
            .room_return_high_water_mark = _room_return_high_water_mark,
            .in_room_count = _in_rooms.size(),
        };
    }

    void RouteCoordinator::completeLeave(const SessionRoute& route) noexcept
    {
        std::lock_guard lock{_mutex};
        if (_handoffs.contains(route.connection) || _room_entries.contains(route.connection) || _room_returns.contains(route.connection))
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
        if (_room_entries.erase(connection) != 0)
        {
            ++_room_entries_abandoned;
        }
        _in_rooms.erase(connection);
        if (_room_returns.erase(connection) != 0)
        {
            ++_room_returns_abandoned;
        }
        _routes.erase(connection);
    }

    RouteCoordinator::HandoffMap::iterator RouteCoordinator::findHandoff(const snf::net::ConnectionId connection, const ZoneHandoffId handoff)
    {
        const auto iterator = _handoffs.find(connection);
        return iterator != _handoffs.end() && iterator->second.id == handoff ? iterator : _handoffs.end();
    }

    RouteCoordinator::RoomEntryMap::iterator RouteCoordinator::findRoomEntry(const snf::net::ConnectionId connection, const RoomEntryId entry)
    {
        const auto iterator = _room_entries.find(connection);
        return iterator != _room_entries.end() && iterator->second.id == entry ? iterator : _room_entries.end();
    }

    RouteCoordinator::RoomReturnMap::iterator RouteCoordinator::findRoomReturn(const snf::net::ConnectionId connection, const RoomReturnId return_id)
    {
        const auto iterator = _room_returns.find(connection);
        return iterator != _room_returns.end() && iterator->second.id == return_id ? iterator : _room_returns.end();
    }
}
