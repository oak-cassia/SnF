#pragma once

#include "snf/server/player_id.hpp"
#include "snf/server/purchase.hpp"

#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace snf::server
{
    struct PongResponse
    {
        std::uint32_t request_id{0};
        std::vector<std::byte> payload;
    };

    struct AuthenticatedResponse
    {
        std::uint32_t request_id{0};
        PlayerId player;
    };

    struct PurchaseResponse
    {
        std::uint32_t request_id{0};
        PurchaseTransactionResult result;
    };

    using PlayerResponse = std::variant<PongResponse, AuthenticatedResponse, PurchaseResponse>;

    struct SendResponse
    {
        PlayerResponse response;
    };

    // The outbound leg only. Runtime-bound follow-ups live in FollowUpAction,
    // which is shared across actor kinds because its destinations are the
    // runtime's own; a response type is per-kind, so it stays here.
    struct PlayerResult
    {
        std::vector<SendResponse> responses;
    };
}
