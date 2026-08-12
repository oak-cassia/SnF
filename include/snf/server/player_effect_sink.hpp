#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/outbound_reservation.hpp"
#include "snf/server/player_result.hpp"

#include <cstddef>

namespace snf::server
{
    // Applies completed actor decisions outside the actor handler. Emission consumes
    // capacity the binding reserved beforehand, so this boundary never blocks and
    // never waits.
    class PlayerEffectSink
    {
    public:
        virtual ~PlayerEffectSink() = default;

        // How much outbound capacity a result needs. Only the sink knows how an effect
        // maps onto outbound actions, so only the sink can price a result.
        [[nodiscard]] virtual std::size_t
        requiredSlots(const PlayerResult& result) const noexcept = 0;

        // Emits the effects in order, consuming one reserved slot each. false means
        // the outbound backend was cancelled; effects emitted before that stay
        // emitted, because this is not a transaction.
        [[nodiscard]] virtual bool commit(snf::net::ConnectionId connection,
                                          PlayerResult result,
                                          OutboundReservation& reservation) = 0;
    };
}
