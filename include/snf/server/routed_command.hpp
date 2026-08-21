#pragma once

#include "snf/game/party_command.hpp"
#include "snf/game/party_id.hpp"
#include "snf/game/player_command.hpp"
#include "snf/game/zone_command.hpp"
#include "snf/game/zone_id.hpp"
#include "snf/net/connection_id.hpp"
#include "snf/server/connection_lifecycle.hpp"
#include "snf/server/party_inbound_command.hpp"
#include "snf/server/player_actor_id.hpp"
#include "snf/server/room_entry.hpp"
#include "snf/server/room_inbound_command.hpp"
#include "snf/server/zone_inbound_command.hpp"

#include <cstdint>
#include <optional>
#include <variant>

namespace snf::server
{
    // Target and command live in the same variant alternative, preventing an
    // invalid pairing such as a PlayerCommand addressed to a future Zone target.
    struct PlayerCommandRoute
    {
        PlayerActorId actor;
        PlayerCommand command;
        std::uint32_t request_id{0};
        // Present only for a JoinRoomRequest that a room entry started. It rides here
        // rather than on the command because it is reactor identity -- ticket, step,
        // connection -- and the game layer must not name any of it.
        std::optional<RoomEntryContext> room_entry{};
    };

    struct ConnectionClosedRoute
    {
        PlayerActorId actor;
        ConnectionCloseCause cause;
        bool has_location_snapshot{false};
        std::optional<PlayerLocation> last_location;
    };

    struct ZoneCommandRoute
    {
        ZoneId zone;
        ZoneCommand command;
        std::optional<ZoneReplyKind> reply_kind;
        std::uint32_t request_id{0};
    };

    struct PartyCommandRoute
    {
        PartyId party;
        PartyCommand command;
        std::optional<PartyReplyKind> reply_kind;
        std::uint32_t request_id{0};
    };

    // Internal cross-Zone stages already contain their immutable reactor reply
    // identity. Keeping them in a distinct route alternative prevents a caller
    // from accidentally attaching a normal client reply/credit to the command.
    struct ZoneHandoffCommandRoute
    {
        ZoneInboundCommand command;
    };

    struct RoomCommandRoute
    {
        RoomId room;
        RoomCommand command;
        std::optional<RoomReplyKind> reply_kind;
        std::uint32_t request_id{0};
    };

    using CommandRoute = std::variant<PlayerCommandRoute, ConnectionClosedRoute, ZoneCommandRoute, PartyCommandRoute, ZoneHandoffCommandRoute, RoomCommandRoute>;

    struct RoutedCommand
    {
        snf::net::ConnectionId connection;
        CommandRoute route;
    };
}
