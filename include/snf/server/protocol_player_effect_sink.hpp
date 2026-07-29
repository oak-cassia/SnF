#pragma once

#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_effect_sink.hpp"
#include "snf/server/protocol_response_mapper.hpp"

namespace snf::server
{
    // Protocol adapter for the player effect boundary. ActorRuntime deliberately
    // does not include this header or know that frames exist.
    class ProtocolPlayerEffectSink final : public PlayerEffectSink
    {
    public:
        explicit ProtocolPlayerEffectSink(OutboundSink& outbound) noexcept;

        [[nodiscard]] bool apply(snf::net::ConnectionId connection,
                                 PlayerResult result,
                                 std::stop_token stop_token) override;

    private:
        OutboundSink& _outbound;
        ProtocolResponseMapper _response_mapper;
    };
}
