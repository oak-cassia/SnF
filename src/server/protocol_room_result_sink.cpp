#include "snf/server/protocol_room_result_sink.hpp"

#include "snf/server/outbound_action.hpp"

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

namespace
{
    constexpr std::uint32_t BYTE_MASK = 0xFFU;

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
    ProtocolRoomResultSink::ProtocolRoomResultSink(OutboundSink& outbound, const PlayerSessionDirectory& sessions) noexcept
        : _outbound(outbound)
        , _sessions(sessions)
    {
    }

    void ProtocolRoomResultSink::accept(const RoomInboundCommand& command, const RoomResult& result)
    {
        publishReply(command, result);
        publishSkill(command, result);
        publishClear(result);
        publishFailure(result);
    }

    std::vector<std::byte> ProtocolRoomResultSink::skillPayload(const RoomResult& result) const
    {
        // One layout for the reply and for the copy every other participant gets, so
        // an observer reads the same frame the caster does and compares the actor to
        // itself. A rejected cast fills the outcome fields with zeroes: it names the
        // player who asked, and reports that nothing landed.
        std::vector<std::byte> payload;
        payload.reserve(1 + 1 + 8 + 4 + 8 + 8);
        payload.push_back(static_cast<std::byte>(static_cast<std::uint8_t>(result.status)));
        payload.push_back(static_cast<std::byte>(static_cast<std::uint8_t>(result.phase)));
        append_u64(payload, result.skill ? result.skill->actor.value : result.player.value_or(PlayerId{}).value);
        append_u32(payload, result.skill ? result.skill->skill.value : 0);
        append_u64(payload, result.skill ? result.skill->damage : 0);
        append_u64(payload, result.boss_health);
        return payload;
    }

    void ProtocolRoomResultSink::publishReply(const RoomInboundCommand& command, const RoomResult& result)
    {
        if (!command.reply)
        {
            return;
        }

        if (command.reply->kind == RoomReplyKind::SkillApplied)
        {
            static_cast<void>(send(
                command.reply->connection,
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::SkillApplied,
                    .request_id = command.reply->request_id,
                    .payload = skillPayload(result),
                }
            ));
            return;
        }

        std::vector<std::byte> payload;
        payload.reserve(1 + 1 + 8);
        payload.push_back(static_cast<std::byte>(static_cast<std::uint8_t>(result.status)));
        payload.push_back(static_cast<std::byte>(static_cast<std::uint8_t>(result.phase)));
        append_u64(payload, command.room.value);

        static_cast<void>(send(
            command.reply->connection,
            snf::protocol::Frame{
                .type =
                    command.reply->kind == RoomReplyKind::Joined ? snf::protocol::MessageType::RoomJoined : snf::protocol::MessageType::BattleStarted,
                .request_id = command.reply->request_id,
                .payload = std::move(payload),
            }
        ));
    }

    void ProtocolRoomResultSink::publishSkill(const RoomInboundCommand& command, const RoomResult& result)
    {
        if (!result.skill)
        {
            return;
        }

        for (const PlayerId player : result.audience)
        {
            // The caster was answered above. Sending them the unsolicited copy as
            // well would report one cast twice.
            if (player == result.skill->actor && command.reply)
            {
                continue;
            }
            const auto connection = _sessions.connectionFor(player);
            if (!connection)
            {
                // Offline, or on the way out. A cast is not owed to anyone the way a
                // reward is -- the battle state they missed is gone with them.
                continue;
            }
            static_cast<void>(send(
                *connection,
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::SkillApplied,
                    .request_id = snf::protocol::UNSOLICITED_REQUEST_ID,
                    .payload = skillPayload(result),
                }
            ));
        }
    }

    void ProtocolRoomResultSink::publishFailure(const RoomResult& result)
    {
        if (result.phase != RoomPhase::Failed || result.status != RoomCommandStatus::Applied)
        {
            return;
        }

        for (const PlayerId player : result.audience)
        {
            const auto connection = _sessions.connectionFor(player);
            if (!connection)
            {
                continue;
            }

            std::vector<std::byte> payload;
            payload.reserve(8);
            // What the boss had left, which is the only thing a failure has to say.
            append_u64(payload, result.boss_health);

            static_cast<void>(send(
                *connection,
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::BattleFailed,
                    .request_id = snf::protocol::UNSOLICITED_REQUEST_ID,
                    .payload = std::move(payload),
                }
            ));
        }
    }

    void ProtocolRoomResultSink::publishClear(const RoomResult& result)
    {
        for (const StreetExperienceGrant& grant : result.grants)
        {
            const auto connection = _sessions.connectionFor(grant.player);
            if (!connection)
            {
                // Offline, or on the way out. The reward itself is not lost -- the tell
                // that carries it applies against the stored record either way -- so the
                // player sees the new level on the next login.
                continue;
            }

            std::vector<std::byte> payload;
            payload.reserve(8);
            append_u64(payload, grant.experience);

            static_cast<void>(send(
                *connection,
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::BattleCleared,
                    .request_id = snf::protocol::UNSOLICITED_REQUEST_ID,
                    .payload = std::move(payload),
                }
            ));
        }
    }

    bool ProtocolRoomResultSink::send(const snf::net::ConnectionId connection, snf::protocol::Frame frame)
    {
        auto reservation = _outbound.tryReserve(connection, 1);
        if (!reservation)
        {
            _outbound.reportAdmissionFailure(connection);
            return false;
        }
        if (!_outbound.commit(
                *reservation,
                SendFrame{
                    .connection = connection,
                    .frame = std::move(frame),
                }
            ))
        {
            _outbound.reportAdmissionFailure(connection);
            return false;
        }
        return true;
    }
}
