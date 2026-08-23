#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/room_id.hpp"
#include "snf/game/zone_command.hpp"
#include "snf/game/zone_id.hpp"
#include "snf/net/connection_id.hpp"
#include "snf/server/room_entry.hpp"
#include "snf/server/zone_handoff.hpp"

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

    struct RoomEntry
    {
        RoomEntryId id;
        SessionRoute source;
        RoomId room;
        std::uint32_t request_id{0};
        RoomEntryStep step{RoomEntryStep::RequestSnapshot};

        [[nodiscard]] bool operator==(const RoomEntry&) const noexcept = default;
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

        std::uint64_t room_entries_started{0};
        std::uint64_t room_entries_completed{0};
        std::uint64_t room_entries_rolled_back{0};
        std::uint64_t room_entries_abandoned{0};
        std::uint64_t room_entries_rejected{0};
        std::size_t pending_room_entries{0};
        std::size_t room_entry_high_water_mark{0};

        std::uint64_t room_returns_started{0};
        std::uint64_t room_returns_completed{0};
        std::uint64_t room_returns_abandoned{0};
        std::size_t pending_room_returns{0};
        std::size_t room_return_high_water_mark{0};

        std::size_t in_room_count{0};
    };

    class RouteCoordinator
    {
    public:
        explicit RouteCoordinator(std::size_t max_handoffs = 4096);

        [[nodiscard]] std::optional<RouteAdmission> tryEnter(snf::net::ConnectionId connection, PlayerId player, ZoneId zone);
        void rollbackEnter(const RouteAdmission& admission) noexcept;

        [[nodiscard]] std::optional<ZoneHandoff>
        tryBeginHandoff(snf::net::ConnectionId connection, PlayerId player, ZoneId target_zone, ZonePosition source_position, ZonePosition requested_target_position, std::uint32_t request_id);
        [[nodiscard]] std::optional<ZoneHandoff> handoffFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] bool noteSourceLeft(snf::net::ConnectionId connection, ZoneHandoffId handoff, ZonePosition position) noexcept;
        [[nodiscard]] std::optional<std::uint64_t> beginSourceRestore(snf::net::ConnectionId connection, ZoneHandoffId handoff);
        [[nodiscard]] bool beginCleanup(snf::net::ConnectionId connection, ZoneHandoffId handoff, ZoneHandoffStep cleanup_step) noexcept;
        [[nodiscard]] std::optional<SessionRoute> completeTargetEnter(snf::net::ConnectionId connection, ZoneHandoffId handoff) noexcept;
        [[nodiscard]] std::optional<SessionRoute> completeSourceRestore(snf::net::ConnectionId connection, ZoneHandoffId handoff) noexcept;
        [[nodiscard]] bool rollbackHandoffBeforeLeave(snf::net::ConnectionId connection, ZoneHandoffId handoff) noexcept;

        [[nodiscard]] std::optional<RoomEntry>
        tryBeginRoomEntry(snf::net::ConnectionId connection, PlayerId player, RoomId room, std::uint32_t request_id);
        [[nodiscard]] std::optional<RoomEntry> roomEntryFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] bool noteRoomSnapshotReady(snf::net::ConnectionId connection, RoomEntryId entry) noexcept;
        [[nodiscard]] bool noteRoomJoined(snf::net::ConnectionId connection, RoomEntryId entry) noexcept;
        [[nodiscard]] std::optional<InRoomState> completeRoomEntry(snf::net::ConnectionId connection, RoomEntryId entry, ZonePosition return_position) noexcept;
        [[nodiscard]] bool rollbackRoomEntryBeforeLeave(snf::net::ConnectionId connection, RoomEntryId entry) noexcept;

        [[nodiscard]] std::optional<InRoomState> inRoomFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] std::size_t inRoomCountFor(RoomId room) const;

        [[nodiscard]] std::optional<RoomReturn> tryBeginRoomReturn(snf::net::ConnectionId connection, RoomId room);
        [[nodiscard]] std::optional<RoomReturn> roomReturnFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] std::optional<SessionRoute> completeRoomReturn(snf::net::ConnectionId connection, RoomReturnId return_id) noexcept;
        [[nodiscard]] bool abandonRoomReturn(snf::net::ConnectionId connection, RoomReturnId return_id) noexcept;

        [[nodiscard]] std::optional<SessionRoute> routeFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] std::size_t routeCountFor(ZoneId zone) const;
        [[nodiscard]] RouteCoordinatorStats stats() const;
        void completeLeave(const SessionRoute& route) noexcept;
        void abandon(snf::net::ConnectionId connection) noexcept;

    private:
        using HandoffMap = std::unordered_map<snf::net::ConnectionId, ZoneHandoff, snf::net::ConnectionIdHash>;
        using RoomEntryMap = std::unordered_map<snf::net::ConnectionId, RoomEntry, snf::net::ConnectionIdHash>;
        using InRoomMap = std::unordered_map<snf::net::ConnectionId, InRoomState, snf::net::ConnectionIdHash>;
        using RoomReturnMap = std::unordered_map<snf::net::ConnectionId, RoomReturn, snf::net::ConnectionIdHash>;

        [[nodiscard]] HandoffMap::iterator findHandoff(snf::net::ConnectionId connection, ZoneHandoffId handoff);
        [[nodiscard]] RoomEntryMap::iterator findRoomEntry(snf::net::ConnectionId connection, RoomEntryId entry);
        [[nodiscard]] RoomReturnMap::iterator findRoomReturn(snf::net::ConnectionId connection, RoomReturnId return_id);

        const std::size_t _max_handoffs;
        mutable std::mutex _mutex;
        std::unordered_map<snf::net::ConnectionId, SessionRoute, snf::net::ConnectionIdHash> _routes;
        HandoffMap _handoffs;
        RoomEntryMap _room_entries;
        InRoomMap _in_rooms;
        RoomReturnMap _room_returns;
        std::unordered_map<PlayerId, std::uint64_t, PlayerIdHash> _last_epoch;
        std::uint64_t _next_handoff_id{1};
        std::uint64_t _handoffs_started{0};
        std::uint64_t _handoffs_completed{0};
        std::uint64_t _handoffs_restored{0};
        std::uint64_t _handoffs_rolled_back{0};
        std::uint64_t _handoffs_abandoned{0};
        std::uint64_t _handoffs_rejected{0};
        std::size_t _handoff_high_water_mark{0};

        std::uint64_t _next_room_entry_id{1};
        std::uint64_t _room_entries_started{0};
        std::uint64_t _room_entries_completed{0};
        std::uint64_t _room_entries_rolled_back{0};
        std::uint64_t _room_entries_abandoned{0};
        std::uint64_t _room_entries_rejected{0};
        std::size_t _room_entry_high_water_mark{0};

        std::uint64_t _next_room_return_id{1};
        std::uint64_t _room_returns_started{0};
        std::uint64_t _room_returns_completed{0};
        std::uint64_t _room_returns_abandoned{0};
        std::size_t _room_return_high_water_mark{0};
    };
}
