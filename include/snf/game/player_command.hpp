#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/purchase.hpp"
#include "snf/game/room_id.hpp"

#include <cstddef>
#include <variant>
#include <vector>

namespace snf::server
{
    // No request id here. Correlating a reply with the frame that asked for it is
    // a protocol concern, so it rides in the envelope -- the same place
    // ZoneInboundCommand keeps it.
    struct PingCommand
    {
        std::vector<std::byte> payload;
    };

    struct AuthenticateCommand
    {
        PlayerId player;
    };

    struct PurchaseCommand
    {
        PurchaseIdempotencyKey idempotency_key;
        ProductId product;
    };

    // Answered by the Room, not here: this Player only decides which stats it
    // enters with. The reply travels with the request so the Room can answer the
    // frame that asked.
    struct JoinRoomRequest
    {
        RoomId room{};
    };

    using PlayerCommand = std::variant<PingCommand, AuthenticateCommand, PurchaseCommand, JoinRoomRequest>;
}
