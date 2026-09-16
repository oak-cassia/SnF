#pragma once

#include "snf/game/player_result.hpp"
#include "snf/net/connection_id.hpp"
#include "snf/server/outbound_reservation.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/protocol_response_mapper.hpp"

#include <cstddef>
#include <cstdint>

namespace snf::server
{
    // The frame count computed by requiredSlots() must match the number of frames emitted in applyResponses().
    // This is guaranteed because ProtocolResponseMapper::map returns exactly one Frame per response.
    class ProtocolPlayerResponseSink
    {
    public:
        explicit ProtocolPlayerResponseSink(OutboundSink& outbound) noexcept;

        [[nodiscard]] std::size_t requiredSlots(const PlayerResult& result) const noexcept;

        [[nodiscard]] bool applyResponses(
            snf::net::ConnectionId connection,
            std::uint32_t request_id,
            PlayerResult result,
            OutboundReservation& reservation
        );

    private:
        OutboundSink& _outbound;
        ProtocolResponseMapper _response_mapper;
    };
}
