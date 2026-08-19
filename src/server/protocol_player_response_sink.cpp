#include "snf/server/protocol_player_response_sink.hpp"

#include <utility>

namespace snf::server
{
    ProtocolPlayerResponseSink::ProtocolPlayerResponseSink(OutboundSink& outbound) noexcept
        : _outbound(outbound)
    {
    }

    std::size_t ProtocolPlayerResponseSink::requiredSlots(const PlayerResult& result) const noexcept
    {
        // One outbound action per response. Only the sink knows this mapping, which
        // is why the binding asks rather than counting the vector itself.
        return result.responses.size();
    }

    bool ProtocolPlayerResponseSink::applyResponses(const snf::net::ConnectionId connection, const std::uint32_t request_id, PlayerResult result, OutboundReservation& reservation)
    {
        for (const SendResponse& response : result.responses)
        {
            const bool emitted = _outbound.commit(reservation,
                                                  SendFrame{
                                                      .connection = connection,
                                                      .frame = _response_mapper.map(response.response, request_id),
                                                  });
            if (!emitted)
            {
                return false;
            }
        }

        return true;
    }
}
