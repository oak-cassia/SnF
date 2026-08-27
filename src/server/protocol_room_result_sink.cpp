#include "snf/server/protocol_room_result_sink.hpp"

#include "snf/protocol/frame.hpp"
#include "snf/server/outbound_action.hpp"

#include <cstddef>
#include <limits>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    constexpr std::uint32_t BYTE_MASK = 0xFFU;
    constexpr std::size_t DIGEST_HEADER_SIZE = 8 + 1 + 2;
    constexpr std::size_t ENCODED_FRAME_OVERHEAD = snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE;

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

    [[nodiscard]] constexpr std::size_t encoded_event_size(const snf::server::BattleEvent& event) noexcept
    {
        return std::visit(
            [](const auto& value) -> std::size_t
            {
                using Event = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<Event, snf::server::EnemySpawned>)
                {
                    return 1 + 4 + 1 + 8;
                }
                else if constexpr (std::is_same_v<Event, snf::server::EnemyDamaged>)
                {
                    return 1 + 4 + 8 + 4 + 8 + 8;
                }
                else if constexpr (std::is_same_v<Event, snf::server::EnemyDied>)
                {
                    return 1 + 4;
                }
                else if constexpr (std::is_same_v<Event, snf::server::SkillWhiffed>)
                {
                    return 1 + 8 + 4;
                }
                else if constexpr (std::is_same_v<Event, snf::server::ArenaStarted>)
                {
                    return 1 + 4 + 4;
                }
                else if constexpr (std::is_same_v<Event, snf::server::ParticipantSpawned>)
                {
                    return 1 + 8 + 4 + 4 + 8;
                }
                else if constexpr (std::is_same_v<Event, snf::server::ParticipantMoved>)
                {
                    return 1 + 8 + 4 + 4;
                }
                else if constexpr (std::is_same_v<Event, snf::server::EnemyPositioned>)
                {
                    return 1 + 4 + 4 + 4;
                }
                else if constexpr (std::is_same_v<Event, snf::server::ParticipantDamaged>)
                {
                    return 1 + 8 + 4 + 8 + 8;
                }
                else if constexpr (std::is_same_v<Event, snf::server::ParticipantDied> || std::is_same_v<Event, snf::server::ParticipantLeft>)
                {
                    return 1 + 8;
                }
                else if constexpr (std::is_same_v<Event, snf::server::ProjectileSpawned>)
                {
                    return 1 + 4 + 8 + 4 + 4 + 4 + 4;
                }
                else if constexpr (std::is_same_v<Event, snf::server::ProjectileMoved>)
                {
                    return 1 + 4 + 4 + 4;
                }
                else
                {
                    static_assert(std::is_same_v<Event, snf::server::ProjectileRemoved>);
                    return 1 + 4 + 1;
                }
            },
            event
        );
    }

    void append_event(std::vector<std::byte>& payload, const snf::server::BattleEvent& event)
    {
        std::visit(
            [&payload](const auto& value)
            {
                using Event = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<Event, snf::server::EnemySpawned>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::EnemySpawned));
                    append_u32(payload, value.id.value);
                    payload.push_back(static_cast<std::byte>(value.kind));
                    append_u64(payload, value.health);
                }
                else if constexpr (std::is_same_v<Event, snf::server::EnemyDamaged>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::EnemyDamaged));
                    append_u32(payload, value.target.value);
                    append_u64(payload, value.actor.value);
                    append_u32(payload, value.skill_id.value);
                    append_u64(payload, value.amount);
                    append_u64(payload, value.health);
                }
                else if constexpr (std::is_same_v<Event, snf::server::EnemyDied>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::EnemyDied));
                    append_u32(payload, value.id.value);
                }
                else if constexpr (std::is_same_v<Event, snf::server::SkillWhiffed>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::SkillWhiffed));
                    append_u64(payload, value.actor.value);
                    append_u32(payload, value.skill_id.value);
                }
                else if constexpr (std::is_same_v<Event, snf::server::ArenaStarted>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::ArenaStarted));
                    append_u32(payload, value.width);
                    append_u32(payload, value.height);
                }
                else if constexpr (std::is_same_v<Event, snf::server::ParticipantSpawned>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::ParticipantSpawned));
                    append_u64(payload, value.player.value);
                    append_u32(payload, value.position.x);
                    append_u32(payload, value.position.y);
                    append_u64(payload, value.health);
                }
                else if constexpr (std::is_same_v<Event, snf::server::ParticipantMoved>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::ParticipantMoved));
                    append_u64(payload, value.player.value);
                    append_u32(payload, value.position.x);
                    append_u32(payload, value.position.y);
                }
                else if constexpr (std::is_same_v<Event, snf::server::EnemyPositioned>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::EnemyPositioned));
                    append_u32(payload, value.enemy.value);
                    append_u32(payload, value.position.x);
                    append_u32(payload, value.position.y);
                }
                else if constexpr (std::is_same_v<Event, snf::server::ParticipantDamaged>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::ParticipantDamaged));
                    append_u64(payload, value.target.value);
                    append_u32(payload, value.attacker.value);
                    append_u64(payload, value.amount);
                    append_u64(payload, value.health);
                }
                else if constexpr (std::is_same_v<Event, snf::server::ParticipantDied>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::ParticipantDied));
                    append_u64(payload, value.player.value);
                }
                else if constexpr (std::is_same_v<Event, snf::server::ParticipantLeft>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::ParticipantLeft));
                    append_u64(payload, value.player.value);
                }
                else if constexpr (std::is_same_v<Event, snf::server::ProjectileSpawned>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::ProjectileSpawned));
                    append_u32(payload, value.projectile.value);
                    append_u64(payload, value.owner.value);
                    append_u32(payload, value.skill_id.value);
                    append_u32(payload, value.target.value);
                    append_u32(payload, value.position.x);
                    append_u32(payload, value.position.y);
                }
                else if constexpr (std::is_same_v<Event, snf::server::ProjectileMoved>)
                {
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::ProjectileMoved));
                    append_u32(payload, value.projectile.value);
                    append_u32(payload, value.position.x);
                    append_u32(payload, value.position.y);
                }
                else
                {
                    static_assert(std::is_same_v<Event, snf::server::ProjectileRemoved>);
                    payload.push_back(static_cast<std::byte>(snf::server::BattleEventKind::ProjectileRemoved));
                    append_u32(payload, value.projectile.value);
                    payload.push_back(static_cast<std::byte>(value.reason));
                }
            },
            event
        );
    }
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

        if (command.reply->kind == RoomReplyKind::SkillAcknowledged || command.reply->kind == RoomReplyKind::MoveAcknowledged)
        {
            std::vector<std::byte> payload{
                static_cast<std::byte>(result.status),
                static_cast<std::byte>(result.phase),
            };
            static_cast<void>(send(
                command.reply->connection,
                snf::protocol::Frame{
                    .type = command.reply->kind == RoomReplyKind::SkillAcknowledged ? snf::protocol::MessageType::SkillAcknowledged
                                                                                    : snf::protocol::MessageType::MoveAcknowledged,
                    .request_id = command.reply->request_id,
                    .payload = std::move(payload),
                }
            ));
            return;
        }

        std::vector<std::byte> payload;
        payload.reserve(1 + 1 + 8);
        payload.push_back(static_cast<std::byte>(result.status));
        payload.push_back(static_cast<std::byte>(result.phase));
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

    std::optional<std::size_t> ProtocolRoomResultSink::digestPayloadSize(const BattleDigest& digest) const noexcept
    {
        if (digest.events.size() > std::numeric_limits<std::uint16_t>::max())
        {
            return std::nullopt;
        }

        std::size_t size = DIGEST_HEADER_SIZE;
        for (const BattleEvent& event : digest.events)
        {
            const std::size_t event_size = encoded_event_size(event);
            if (event_size > snf::protocol::MAX_PAYLOAD_SIZE - size)
            {
                return std::nullopt;
            }
            size += event_size;
        }
        return size;
    }

    std::vector<std::byte> ProtocolRoomResultSink::digestPayload(const RoomResult& result) const
    {
        const BattleDigest& digest = *result.digest;
        const std::size_t size = *digestPayloadSize(digest);
        std::vector<std::byte> payload;
        payload.reserve(size);
        append_u64(payload, digest.sequence);
        payload.push_back(static_cast<std::byte>(result.phase));
        append_u16(payload, static_cast<std::uint16_t>(digest.events.size()));
        for (const BattleEvent& event : digest.events)
        {
            append_event(payload, event);
        }
        return payload;
    }

    void ProtocolRoomResultSink::publishDigest(const RoomResult& result)
    {
        if (!result.digest)
        {
            return;
        }

        if (!digestPayloadSize(*result.digest))
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

        const std::vector<std::byte> payload = digestPayload(result);
        for (const PlayerId player : result.audience)
        {
            const auto connection = _sessions.connectionFor(player);
            if (!connection)
            {
                continue;
            }
            if (send(
                    *connection,
                    snf::protocol::Frame{
                        .type = snf::protocol::MessageType::BattleDigest,
                        .request_id = snf::protocol::UNSOLICITED_REQUEST_ID,
                        .payload = payload,
                    }
                ))
            {
                _battle_digest_frames.fetch_add(1, std::memory_order_relaxed);
                _battle_digest_fanout_bytes.fetch_add(ENCODED_FRAME_OVERHEAD + payload.size(), std::memory_order_relaxed);
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
            const auto connection = _sessions.connectionFor(grant.player);
            if (!connection)
            {
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

    void ProtocolRoomResultSink::publishFailure(const RoomResult& result)
    {
        if (result.outcome != BattleOutcome::Failed)
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
            payload.reserve(8 + 1 + 1);
            append_u64(payload, result.boss_health);
            payload.push_back(static_cast<std::byte>(result.boss_spawned ? 1 : 0));
            payload.push_back(static_cast<std::byte>(result.failure_reason.value()));
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
