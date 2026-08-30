#include "snf/adapter/protocol_encoder.hpp"

#include "snf/protocol/payload_writer.hpp"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <type_traits>
#include <utility>

namespace
{
    using snf::protocol::append_big_endian;
    using snf::protocol::append_u16;
    using snf::protocol::append_u32;
    using snf::protocol::append_u64;

    constexpr std::size_t DIGEST_HEADER_SIZE = 8 + 1 + 2;

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

namespace snf::adapter
{
    snf::protocol::Frame encodePlayerResponse(const snf::server::PlayerResponse& response, const std::uint32_t request_id)
    {
        return std::visit(
            [request_id](const auto& value) -> snf::protocol::Frame
            {
                using Response = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<Response, snf::server::PongResponse>)
                {
                    return snf::protocol::Frame{
                        .type = snf::protocol::MessageType::Pong,
                        .request_id = request_id,
                        .payload = value.payload,
                    };
                }
                else if constexpr (std::is_same_v<Response, snf::server::AuthenticatedResponse>)
                {
                    std::vector<std::byte> payload(8);
                    std::uint64_t remaining = value.player.value;
                    for (std::size_t index = 8; index > 0; --index)
                    {
                        payload[index - 1] = static_cast<std::byte>(remaining & 0xFFU);
                        remaining >>= 8U;
                    }
                    return snf::protocol::Frame{
                        .type = snf::protocol::MessageType::Authenticated,
                        .request_id = request_id,
                        .payload = std::move(payload),
                    };
                }
                else if constexpr (std::is_same_v<Response, snf::server::PurchaseResponse>)
                {
                    std::vector<std::byte> payload;
                    payload.reserve(30);
                    payload.push_back(static_cast<std::byte>(value.result.status));
                    payload.push_back(static_cast<std::byte>(value.result.replayed ? 1 : 0));
                    append_big_endian(payload, value.result.idempotency_key.value);
                    append_big_endian(payload, value.result.product.value);
                    append_big_endian(payload, value.result.currency_balance);
                    append_big_endian(payload, value.result.purchased_item_count);
                    return snf::protocol::Frame{
                        .type = snf::protocol::MessageType::PurchaseResult,
                        .request_id = request_id,
                        .payload = std::move(payload),
                    };
                }
                else
                {
                    static_assert(std::is_same_v<Response, snf::server::EquipSkillResponse>);
                    std::vector<std::byte> payload;
                    payload.reserve(5);
                    payload.push_back(static_cast<std::byte>(value.status));
                    append_big_endian(payload, value.equipped_skill_id.value);
                    return snf::protocol::Frame{
                        .type = snf::protocol::MessageType::EquipSkillResult,
                        .request_id = request_id,
                        .payload = std::move(payload),
                    };
                }
            },
            response
        );
    }

    snf::protocol::Frame encodeZoneReply(
        const ZoneReplyFrameKind kind,
        const snf::server::ZoneId zone,
        const snf::server::ZoneResult& result,
        const std::uint32_t request_id
    )
    {
        snf::protocol::MessageType type = snf::protocol::MessageType::Moved;
        switch (kind)
        {
        case ZoneReplyFrameKind::Entered:
            type = snf::protocol::MessageType::ZoneEntered;
            break;
        case ZoneReplyFrameKind::Moved:
            type = snf::protocol::MessageType::Moved;
            break;
        case ZoneReplyFrameKind::Left:
            type = snf::protocol::MessageType::ZoneLeft;
            break;
        }

        constexpr std::size_t FIXED_PAYLOAD_SIZE = 1 + 8 + 8 + 4 + 4 + 2;
        constexpr std::size_t MAX_VISIBLE_BY_PAYLOAD = (snf::protocol::MAX_PAYLOAD_SIZE - FIXED_PAYLOAD_SIZE) / 8;
        std::vector<std::byte> payload;
        const std::size_t visible_count = std::min(
            result.visible_players.size(), std::min(static_cast<std::size_t>(std::numeric_limits<std::uint16_t>::max()), MAX_VISIBLE_BY_PAYLOAD)
        );
        payload.reserve(FIXED_PAYLOAD_SIZE + (visible_count * 8));

        payload.push_back(static_cast<std::byte>(result.status));
        append_u64(payload, zone.value);
        append_u64(payload, result.route_epoch);
        append_u32(payload, static_cast<std::uint32_t>(result.position ? result.position->x : 0));
        append_u32(payload, static_cast<std::uint32_t>(result.position ? result.position->y : 0));
        append_u16(payload, static_cast<std::uint16_t>(visible_count));
        for (std::size_t index = 0; index < visible_count; ++index)
        {
            append_u64(payload, result.visible_players[index].value);
        }

        return snf::protocol::Frame{
            .type = type,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    snf::protocol::Frame encodeRoomReply(
        const RoomReplyFrameKind kind,
        const snf::server::RoomId room,
        const snf::server::RoomResult& result,
        const std::uint32_t request_id
    )
    {
        if (kind == RoomReplyFrameKind::SkillAcknowledged || kind == RoomReplyFrameKind::MoveAcknowledged)
        {
            return snf::protocol::Frame{
                .type = kind == RoomReplyFrameKind::SkillAcknowledged ? snf::protocol::MessageType::SkillAcknowledged
                                                                      : snf::protocol::MessageType::MoveAcknowledged,
                .request_id = request_id,
                .payload =
                    {
                        static_cast<std::byte>(result.status),
                        static_cast<std::byte>(result.phase),
                    },
            };
        }

        std::vector<std::byte> payload;
        payload.reserve(1 + 1 + 8);
        payload.push_back(static_cast<std::byte>(result.status));
        payload.push_back(static_cast<std::byte>(result.phase));
        append_u64(payload, room.value);

        return snf::protocol::Frame{
            .type = kind == RoomReplyFrameKind::Joined ? snf::protocol::MessageType::RoomJoined : snf::protocol::MessageType::BattleStarted,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    std::optional<snf::protocol::Frame> encodeBattleDigest(const snf::server::RoomResult& result, const std::uint32_t request_id)
    {
        if (!result.digest.has_value())
        {
            return std::nullopt;
        }

        const auto& digest = *result.digest;
        if (digest.events.size() > std::numeric_limits<std::uint16_t>::max())
        {
            return std::nullopt;
        }

        std::size_t size = DIGEST_HEADER_SIZE;
        for (const auto& event : digest.events)
        {
            const std::size_t event_size = encoded_event_size(event);
            if (size > snf::protocol::MAX_PAYLOAD_SIZE || event_size > snf::protocol::MAX_PAYLOAD_SIZE - size)
            {
                return std::nullopt;
            }
            size += event_size;
        }

        std::vector<std::byte> payload;
        payload.reserve(size);
        append_u64(payload, digest.sequence);
        payload.push_back(static_cast<std::byte>(result.phase));
        append_u16(payload, static_cast<std::uint16_t>(digest.events.size()));
        for (const auto& event : digest.events)
        {
            append_event(payload, event);
        }

        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::BattleDigest,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    snf::protocol::Frame encodeBattleCleared(const std::uint64_t experience, const std::uint32_t request_id)
    {
        std::vector<std::byte> payload;
        payload.reserve(8);
        append_u64(payload, experience);
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::BattleCleared,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    snf::protocol::Frame encodeBattleFailure(const snf::server::RoomResult& result, const std::uint32_t request_id)
    {
        std::vector<std::byte> payload;
        payload.reserve(8 + 1 + 1);
        append_u64(payload, result.boss_health);
        payload.push_back(static_cast<std::byte>(result.boss_spawned ? 1 : 0));
        payload.push_back(static_cast<std::byte>(result.failure_reason.value_or(snf::server::BattleFailureReason::Deadline)));
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::BattleFailed,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }
}
