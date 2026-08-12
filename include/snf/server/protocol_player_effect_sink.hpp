#pragma once

#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_domain_event_sink.hpp"
#include "snf/server/player_effect_sink.hpp"
#include "snf/server/protocol_response_mapper.hpp"

namespace snf::server
{
    // Protocol adapter for the player effect boundary. ActorRuntime deliberately
    // does not include this header or know that frames exist.
    class ProtocolPlayerEffectSink final : public PlayerEffectSink
    {
    public:
        explicit ProtocolPlayerEffectSink(OutboundSink& outbound,
                                          PlayerDomainEventSink* events = nullptr) noexcept;

        [[nodiscard]] std::size_t requiredSlots(const PlayerResult& result) const noexcept override;

        [[nodiscard]] bool commit(snf::net::ConnectionId connection,
                                  PlayerResult result,
                                  OutboundReservation& reservation) override;

    private:
        OutboundSink& _outbound;
        PlayerDomainEventSink* _events;
        ProtocolResponseMapper _response_mapper;
    };
}
