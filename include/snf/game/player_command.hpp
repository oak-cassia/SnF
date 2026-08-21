#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/purchase.hpp"

#include <cstddef>
#include <variant>
#include <vector>

namespace snf::server
{
    // No request id here. Correlating a reply with the frame that asked for it is
    // a protocol concern, so it rides in the envelope -- the same place
    // ZoneInboundCommand and PartyInboundCommand keep it.
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

    using PlayerCommand = std::variant<PingCommand, AuthenticateCommand, PurchaseCommand>;
}
