#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/player_result.hpp"

#include <stop_token>

namespace snf::server
{
    // Applies completed actor decisions outside the actor handler. Implementations
    // may block on backpressure, so callers supply their cancellation token.
    class PlayerEffectSink
    {
    public:
        virtual ~PlayerEffectSink() = default;

        // false means the entire result could not be applied. Effects published
        // before a later failure remain published; this is not a transaction.
        [[nodiscard]] virtual bool apply(snf::net::ConnectionId connection,
                                         PlayerResult result,
                                         std::stop_token stop_token) = 0;
    };
}
