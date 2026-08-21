#pragma once

#include "snf/game/party_result.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/party_inbound_command.hpp"

namespace snf::server
{
    class ProtocolPartyResultSink
    {
    public:
        explicit ProtocolPartyResultSink(OutboundSink& outbound) noexcept;

        void accept(const PartyInboundCommand& command, const PartyResult& result);

    private:
        [[nodiscard]] snf::protocol::Frame map(const PartyInboundCommand& command, const PartyResult& result) const;

        OutboundSink& _outbound;
    };
}
