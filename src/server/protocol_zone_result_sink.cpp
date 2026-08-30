#include "snf/server/protocol_zone_result_sink.hpp"

#include "snf/adapter/protocol_encoder.hpp"

#include <utility>

namespace snf::server
{
    ProtocolZoneResultSink::ProtocolZoneResultSink(OutboundSink& outbound) noexcept
        : _outbound(outbound)
    {
    }

    void ProtocolZoneResultSink::accept(const ZoneInboundCommand& command, const ZoneResult& result)
    {
        if (!command.reply)
        {
            return;
        }

        static_cast<void>(send_one_frame(_outbound, command.reply->connection, map(command, result)));
    }

    void ProtocolZoneResultSink::replyStatus(
        const snf::net::ConnectionId connection,
        const PlayerId player,
        const ZoneId zone,
        const std::uint64_t route_epoch,
        const ZonePosition position,
        const std::uint32_t request_id,
        const ZoneReplyKind kind,
        const ZoneCommandStatus status
    )
    {
        accept(
            ZoneInboundCommand{
                .zone = zone,
                .command =
                    EnterZoneCommand{
                        .player = player,
                        .route_epoch = route_epoch,
                        .position = position,
                    },
                .reply =
                    ZoneReplyContext{
                        .connection = connection,
                        .request_id = request_id,
                        .kind = kind,
                    },
                .handoff = std::nullopt,
            },
            ZoneResult{
                .status = status,
                .player = player,
                .position = position,
                .route_epoch = route_epoch,
                .tick = 0,
                .visible_players = {},
            }
        );
    }

    void ProtocolZoneResultSink::reportAdmissionFailure(const snf::net::ConnectionId connection) noexcept
    {
        _outbound.reportAdmissionFailure(connection);
    }

    snf::protocol::Frame ProtocolZoneResultSink::map(const ZoneInboundCommand& command, const ZoneResult& result) const
    {
        snf::adapter::ZoneReplyFrameKind kind = snf::adapter::ZoneReplyFrameKind::Moved;
        switch (command.reply->kind)
        {
        case ZoneReplyKind::Entered:
            kind = snf::adapter::ZoneReplyFrameKind::Entered;
            break;
        case ZoneReplyKind::Moved:
            kind = snf::adapter::ZoneReplyFrameKind::Moved;
            break;
        case ZoneReplyKind::Left:
            kind = snf::adapter::ZoneReplyFrameKind::Left;
            break;
        }

        return snf::adapter::encodeZoneReply(kind, command.zone, result, command.reply->request_id);
    }
}
