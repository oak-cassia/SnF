#include "snf/server/protocol_room_result_sink.hpp"

#include "snf/adapter/protocol_encoder.hpp"
#include "snf/protocol/payload_writer.hpp"

#include <utility>

namespace
{
    constexpr std::size_t ENCODED_FRAME_OVERHEAD = snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE;
}

namespace snf::server
{
    ProtocolRoomResultSink::ProtocolRoomResultSink(OutboundSink& outbound, const PlayerSessionDirectory& sessions) noexcept
        : _outbound(outbound)
        , _sessions(sessions)
    {
    }

    ProtocolRoomResultSinkStats ProtocolRoomResultSink::stats() const noexcept
    {
        return ProtocolRoomResultSinkStats{
            .oversized_battle_digests = _oversized_battle_digests.load(std::memory_order_relaxed),
            .battle_digest_frames = _battle_digest_frames.load(std::memory_order_relaxed),
            .battle_digest_fanout_bytes = _battle_digest_fanout_bytes.load(std::memory_order_relaxed),
        };
    }

    void ProtocolRoomResultSink::accept(const RoomInboundCommand& command, const RoomResult& result)
    {
        publishReply(command, result);
        publishDigest(result);
        publishClear(result);
        publishFailure(result);
    }

    void ProtocolRoomResultSink::publishReply(const RoomInboundCommand& command, const RoomResult& result)
    {
        if (!command.reply)
        {
            return;
        }

        snf::adapter::RoomReplyFrameKind kind = snf::adapter::RoomReplyFrameKind::Joined;
        switch (command.reply->kind)
        {
        case RoomReplyKind::Joined:
            kind = snf::adapter::RoomReplyFrameKind::Joined;
            break;
        case RoomReplyKind::BattleStarted:
            kind = snf::adapter::RoomReplyFrameKind::BattleStarted;
            break;
        case RoomReplyKind::SkillAcknowledged:
            kind = snf::adapter::RoomReplyFrameKind::SkillAcknowledged;
            break;
        case RoomReplyKind::MoveAcknowledged:
            kind = snf::adapter::RoomReplyFrameKind::MoveAcknowledged;
            break;
        }

        static_cast<void>(send(command.reply->connection, snf::adapter::encodeRoomReply(kind, command.room, result, command.reply->request_id)));
    }

    void ProtocolRoomResultSink::publishDigest(const RoomResult& result)
    {
        if (!result.digest)
        {
            return;
        }

        const auto frame = snf::adapter::encodeBattleDigest(result, snf::protocol::UNSOLICITED_REQUEST_ID);
        if (!frame)
        {
            _oversized_battle_digests.fetch_add(1, std::memory_order_relaxed);
            for (const PlayerId player : result.audience)
            {
                if (const auto connection = _sessions.connectionFor(player))
                {
                    _outbound.reportAdmissionFailure(*connection);
                }
            }
            return;
        }

        for (const PlayerId player : result.audience)
        {
            const auto connection = _sessions.connectionFor(player);
            if (!connection)
            {
                continue;
            }
            if (send(*connection, *frame))
            {
                _battle_digest_frames.fetch_add(1, std::memory_order_relaxed);
                _battle_digest_fanout_bytes.fetch_add(ENCODED_FRAME_OVERHEAD + frame->payload.size(), std::memory_order_relaxed);
            }
        }
    }

    void ProtocolRoomResultSink::publishClear(const RoomResult& result)
    {
        if (result.outcome != BattleOutcome::Cleared)
        {
            return;
        }

        for (const StreetExperienceGrant& grant : result.grants)
        {
            if (const auto connection = _sessions.connectionFor(grant.player))
            {
                static_cast<void>(send(*connection, snf::adapter::encodeBattleCleared(grant.experience, snf::protocol::UNSOLICITED_REQUEST_ID)));
            }
        }
    }

    void ProtocolRoomResultSink::publishFailure(const RoomResult& result)
    {
        if (result.outcome != BattleOutcome::Failed)
        {
            return;
        }

        const auto frame = snf::adapter::encodeBattleFailure(result, snf::protocol::UNSOLICITED_REQUEST_ID);
        for (const PlayerId player : result.audience)
        {
            if (const auto connection = _sessions.connectionFor(player))
            {
                static_cast<void>(send(*connection, frame));
            }
        }
    }

    bool ProtocolRoomResultSink::send(const snf::net::ConnectionId connection, snf::protocol::Frame frame)
    {
        return send_one_frame(_outbound, connection, std::move(frame));
    }

    void ProtocolRoomResultSink::replyJoined(
        const snf::net::ConnectionId connection,
        const std::uint32_t request_id,
        const RoomId room,
        const RoomCommandStatus status,
        const RoomPhase phase
    )
    {
        static_cast<void>(send(
            connection,
            snf::adapter::encodeRoomReply(snf::adapter::RoomReplyFrameKind::Joined, room, RoomResult{.status = status, .phase = phase}, request_id)
        ));
    }

    void ProtocolRoomResultSink::replyReturnedToZone(const snf::net::ConnectionId connection, const ZoneId zone, const ZonePosition position)
    {
        std::vector<std::byte> payload;
        payload.reserve(8 + 4 + 4);
        snf::protocol::append_u64(payload, zone.value);
        snf::protocol::append_u32(payload, static_cast<std::uint32_t>(position.x));
        snf::protocol::append_u32(payload, static_cast<std::uint32_t>(position.y));

        static_cast<void>(send(
            connection,
            snf::protocol::Frame{
                .type = snf::protocol::MessageType::ReturnedToZone,
                .request_id = snf::protocol::UNSOLICITED_REQUEST_ID,
                .payload = std::move(payload),
            }
        ));
    }

    void ProtocolRoomResultSink::reportAdmissionFailure(const snf::net::ConnectionId connection) noexcept
    {
        _outbound.reportAdmissionFailure(connection);
    }
}
