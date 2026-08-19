#include "snf/server/protocol_party_result_sink.hpp"

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
        append_u32(bytes, static_cast<std::uint32_t>(value));
    }
}

namespace snf::server
{
    ProtocolPartyResultSink::ProtocolPartyResultSink(OutboundSink& outbound) noexcept
        : _outbound(outbound)
    {
    }

    void ProtocolPartyResultSink::accept(const PartyInboundCommand& command, const PartyResult& result)
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

    snf::protocol::Frame ProtocolPartyResultSink::map(const PartyInboundCommand& command, const PartyResult& result) const
    {
        const snf::protocol::MessageType type = command.reply->kind == PartyReplyKind::Joined ? snf::protocol::MessageType::PartyJoined : snf::protocol::MessageType::PartyLeft;
        constexpr std::size_t FIXED_PAYLOAD_SIZE = 1 + 8 + 8 + 2;
        constexpr std::size_t MAX_MEMBERS_BY_PAYLOAD = (snf::protocol::MAX_PAYLOAD_SIZE - FIXED_PAYLOAD_SIZE) / 8;
        const std::size_t member_count = std::min(result.members.size(), std::min(static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max()), MAX_MEMBERS_BY_PAYLOAD));

        std::vector<std::byte> payload;
        payload.reserve(FIXED_PAYLOAD_SIZE + member_count * 8);
        payload.push_back(static_cast<std::byte>(static_cast<std::uint8_t>(result.status)));
        append_u64(payload, result.party.value);
        append_u64(payload, result.membership_epoch);
        append_u16(payload, static_cast<std::uint16_t>(member_count));
        for (std::size_t index = 0; index < member_count; ++index)
        {
            append_u64(payload, result.members[index].value);
        }

        return snf::protocol::Frame{
            .type = type,
            .request_id = command.reply->request_id,
            .payload = std::move(payload),
        };
    }
}
