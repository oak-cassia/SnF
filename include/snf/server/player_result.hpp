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

    // The outbound leg only. Anything a handler wants the runtime to do afterwards
    // is a named field on the per-kind result, and the binding translates it.
    struct PlayerResult
    {
        std::vector<SendResponse> responses;
    };
}
