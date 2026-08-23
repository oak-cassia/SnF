#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/purchase.hpp"
#include "snf/game/room_id.hpp"

#include <cstddef>
#include <variant>
#include <vector>

namespace snf::server
{
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

    struct JoinRoomRequest
    {
        RoomId room{};
    };

    using PlayerCommand = std::variant<PingCommand, AuthenticateCommand, PurchaseCommand, JoinRoomRequest>;
}
