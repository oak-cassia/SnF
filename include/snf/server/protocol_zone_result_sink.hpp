#pragma once

#include "snf/server/outbound_sink.hpp"
#include "snf/server/zone_inbound_command.hpp"
#include "snf/server/zone_result.hpp"

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
