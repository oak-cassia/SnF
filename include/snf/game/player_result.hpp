#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/purchase.hpp"

#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace snf::server
{
    // The request id is stamped on at the protocol boundary, from the envelope the
    // command arrived in, so a handler never carries one through.
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

    // The outbound leg only. Anything a handler wants the runtime to do afterwards
    // is a named field on the per-kind result, and the binding translates it.
    struct PlayerResult
    {
        std::vector<SendResponse> responses;
    };
}
