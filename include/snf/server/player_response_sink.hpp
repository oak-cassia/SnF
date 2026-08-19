#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/outbound_reservation.hpp"
#include "snf/server/player_result.hpp"

#include <cstddef>

namespace snf::server
{
    // Applies completed actor decisions outside the actor handler. Application consumes
    // capacity the binding reserved beforehand, so this boundary never blocks and
    // never waits.
    class PlayerResponseSink
    {
    public:
        virtual ~PlayerResponseSink() = default;

        // How much outbound capacity a result needs. Only the sink knows how a follow-up
        // maps onto outbound actions, so only the sink can price a result.
        [[nodiscard]] virtual std::size_t requiredSlots(const PlayerResult& result) const noexcept = 0;

        // Applies the follow-ups in order, consuming one reserved slot each. false means
        // the outbound backend was cancelled; follow-ups applied before that stay
        // applied, because this is not a transaction.
        [[nodiscard]] virtual bool applyResponses(snf::net::ConnectionId connection, PlayerResult result, OutboundReservation& reservation) = 0;
    };
}
