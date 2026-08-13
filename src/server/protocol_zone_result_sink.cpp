#include "snf/server/protocol_zone_result_sink.hpp"

#include "snf/server/outbound_action.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

namespace
{
    constexpr std::uint32_t BYTE_MASK = 0xFFU;

    void append_u16(std::vector<std::byte>& bytes, const std::uint16_t value)
    {
        bytes.push_back(static_cast<std::byte>((value >> 8U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>(value & BYTE_MASK));
    }

    void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value)
    {
        bytes.push_back(static_cast<std::byte>((value >> 24U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>((value >> 16U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>((value >> 8U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>(value & BYTE_MASK));
    }

    void append_u64(std::vector<std::byte>& bytes, const std::uint64_t value)
    {
        append_u32(bytes, static_cast<std::uint32_t>(value >> 32U));
        append_u32(bytes, static_cast<std::uint32_t>(value & 0xFFFFFFFFULL));
    }
}

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

        auto reservation = _outbound.tryReserve(command.reply->connection, 1);
        if (!reservation)
        {
            _outbound.reportAdmissionFailure(command.reply->connection);
            return;
        }

        if (!_outbound.commit(*reservation,
                              SendFrame{
                                  .connection = command.reply->connection,
                                  .frame = map(command, result),
                              }))
        {
            _outbound.reportAdmissionFailure(command.reply->connection);
        }
    }

    void
    ProtocolZoneResultSink::reportAdmissionFailure(const snf::net::ConnectionId connection) noexcept
    {
        _outbound.reportAdmissionFailure(connection);
    }

    snf::protocol::Frame ProtocolZoneResultSink::map(const ZoneInboundCommand& command,
                                                     const ZoneResult& result) const
    {
        snf::protocol::MessageType type = snf::protocol::MessageType::Moved;
        switch (command.reply->kind)
        {
        case ZoneReplyKind::Entered:
            type = snf::protocol::MessageType::ZoneEntered;
            break;
        case ZoneReplyKind::Moved:
            type = snf::protocol::MessageType::Moved;
            break;
        case ZoneReplyKind::Left:
            type = snf::protocol::MessageType::ZoneLeft;
            break;
        }

        const ZonePosition position = result.position.value_or(ZonePosition{});
        constexpr std::size_t FIXED_PAYLOAD_SIZE = 1 + 8 + 8 + 4 + 4 + 2;
        constexpr std::size_t MAX_VISIBLE_BY_PAYLOAD =
            (snf::protocol::MAX_PAYLOAD_SIZE - FIXED_PAYLOAD_SIZE) / 8;
        const std::size_t visible_count =
            std::min(result.visible_players.size(),
                     std::min(static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max()),
                              MAX_VISIBLE_BY_PAYLOAD));

        std::vector<std::byte> payload;
        payload.reserve(1 + 8 + 8 + 4 + 4 + 2 + visible_count * 8);
        payload.push_back(static_cast<std::byte>(static_cast<std::uint8_t>(result.status)));
        append_u64(payload, command.zone.value);
        append_u64(payload, result.route_epoch);
        append_u32(payload, static_cast<std::uint32_t>(position.x));
        append_u32(payload, static_cast<std::uint32_t>(position.y));
        append_u16(payload, static_cast<std::uint16_t>(visible_count));
        for (std::size_t index = 0; index < visible_count; ++index)
        {
            append_u64(payload, result.visible_players[index].value);
        }

        return snf::protocol::Frame{
            .type = type,
            .request_id = command.reply->request_id,
            .payload = std::move(payload),
        };
    }
}
