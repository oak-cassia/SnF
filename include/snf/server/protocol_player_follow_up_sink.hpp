#pragma once

#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_follow_up_sink.hpp"
#include "snf/server/protocol_response_mapper.hpp"

namespace snf::server
{
    // Protocol adapter for the player follow-up boundary. ActorRuntime deliberately
    // does not include this header or know that frames exist.
    class ProtocolPlayerFollowUpSink final : public PlayerFollowUpSink
    {
    public:
        explicit ProtocolPlayerFollowUpSink(OutboundSink& outbound) noexcept;

        [[nodiscard]] std::size_t requiredSlots(const PlayerResult& result) const noexcept override;

        [[nodiscard]] bool applyFollowUps(snf::net::ConnectionId connection,
                                          PlayerResult result,
                                          OutboundReservation& reservation) override;

    private:
        OutboundSink& _outbound;
        ProtocolResponseMapper _response_mapper;
    };
}
