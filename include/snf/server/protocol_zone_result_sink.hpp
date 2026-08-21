#pragma once

#include "snf/game/zone_result.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/zone_inbound_command.hpp"

namespace snf::server
{
    class ProtocolZoneResultSink
    {
    public:
        explicit ProtocolZoneResultSink(OutboundSink& outbound) noexcept;

        void accept(const ZoneInboundCommand& command, const ZoneResult& result);
        void reportAdmissionFailure(snf::net::ConnectionId connection) noexcept;

    private:
        [[nodiscard]] snf::protocol::Frame map(const ZoneInboundCommand& command, const ZoneResult& result) const;

        OutboundSink& _outbound;
    };
}
