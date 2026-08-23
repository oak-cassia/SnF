#pragma once

#include "snf/game/zone_result.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/zone_inbound_command.hpp"

#include <cstdint>

namespace snf::server
{
    class ProtocolZoneResultSink
    {
    public:
        explicit ProtocolZoneResultSink(OutboundSink& outbound) noexcept;

        void accept(const ZoneInboundCommand& command, const ZoneResult& result);
        void replyStatus(
            snf::net::ConnectionId connection, PlayerId player, ZoneId zone, std::uint64_t route_epoch, ZonePosition position, std::uint32_t request_id, ZoneReplyKind kind, ZoneCommandStatus status);
        void reportAdmissionFailure(snf::net::ConnectionId connection) noexcept;

    private:
        [[nodiscard]] snf::protocol::Frame map(const ZoneInboundCommand& command, const ZoneResult& result) const;

        OutboundSink& _outbound;
    };
}
