#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/room_command.hpp"
#include "snf/game/room_id.hpp"
#include "snf/game/room_result.hpp"
#include "snf/game/zone_command.hpp"
#include "snf/game/zone_id.hpp"
#include "snf/net/connection_id.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    struct RoomEntryId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool valid() const noexcept
        {
            return value != 0;
        }

        [[nodiscard]] bool operator==(const RoomEntryId&) const noexcept = default;
    };

    struct RoomTransitionTicket
    {
        std::uint64_t value{0};

        [[nodiscard]] bool valid() const noexcept
        {
            return value != 0;
        }

        [[nodiscard]] bool operator==(const RoomTransitionTicket&) const noexcept = default;
    };

    struct RoomReturnId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool valid() const noexcept
        {
            return value != 0;
        }

        [[nodiscard]] bool operator==(const RoomReturnId&) const noexcept = default;
    };

    enum class RoomEntryStep
    {
        RequestSnapshot,
        JoinRoom,
        LeaveSource,
        ReturnZone,
    };

    struct RoomEntryContext
    {
        RoomEntryId entry_id{};
        RoomReturnId return_id{};
        RoomTransitionTicket ticket{};
        snf::net::ConnectionId connection{};
        PlayerId player{};
        RoomEntryStep step{RoomEntryStep::JoinRoom};

        [[nodiscard]] bool operator==(const RoomEntryContext&) const noexcept = default;
    };

    struct InRoomState
    {
        snf::net::ConnectionId connection;
        PlayerId player;
        RoomId room;
        ZoneId return_zone;
        ZonePosition return_position;

        [[nodiscard]] bool operator==(const InRoomState&) const noexcept = default;
    };

    struct RoomReturn
    {
        RoomReturnId id;
        snf::net::ConnectionId connection;
        PlayerId player;
        RoomId room;
        ZoneId return_zone;
        std::uint64_t return_epoch{0};
        ZonePosition return_position;

        [[nodiscard]] bool operator==(const RoomReturn&) const noexcept = default;
    };
}
