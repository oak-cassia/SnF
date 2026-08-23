#pragma once

#include "snf/game/player_result.hpp"
#include "snf/net/connection_id.hpp"
#include "snf/server/outbound_reservation.hpp"

#include <cstddef>
#include <cstdint>

namespace snf::server
{
    class PlayerResponseSink
    {
    public:
        virtual ~PlayerResponseSink() = default;

        [[nodiscard]] virtual std::size_t requiredSlots(const PlayerResult& result) const noexcept = 0;

        [[nodiscard]] virtual bool applyResponses(snf::net::ConnectionId connection, std::uint32_t request_id, PlayerResult result, OutboundReservation& reservation) = 0;
    };
}
