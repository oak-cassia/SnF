#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/purchase.hpp"
#include "snf/game/room_join_request.hpp"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace snf::server
{
    struct PongResponse
    {
        std::vector<std::byte> payload;
    };

    struct AuthenticatedResponse
    {
        PlayerId player;
    };

    struct PurchaseResponse
    {
        PurchaseTransactionResult result;
    };

    using PlayerResponse = std::variant<PongResponse, AuthenticatedResponse, PurchaseResponse>;

    struct SendResponse
    {
        PlayerResponse response;
    };

    struct PlayerResult
    {
        std::vector<SendResponse> responses{};
        std::optional<RoomJoinRequest> room_join{};
    };
}
