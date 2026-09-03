#pragma once

#include "snf/game/player_command.hpp"
#include "snf/game/player_id.hpp"
#include "snf/game/room_command.hpp"
#include "snf/game/room_id.hpp"
#include "snf/game/room_result.hpp"
#include "snf/game/street_experience_grant.hpp"
#include "snf/game/zone_command.hpp"
#include "snf/game/zone_id.hpp"
#include "snf/game/zone_result.hpp"
#include "snf/protocol/frame.hpp"
#include "snf/worker/actor_envelope.hpp"
#include "snf/worker/identity.hpp"

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <vector>

namespace snf::adapter
{
    [[nodiscard]] constexpr std::uint64_t logicalCharge(const std::uint64_t fixed, const std::size_t dynamic_capacity) noexcept
    {
        const auto dynamic = static_cast<std::uint64_t>(dynamic_capacity);
        return dynamic > std::numeric_limits<std::uint64_t>::max() - fixed ? std::numeric_limits<std::uint64_t>::max() : fixed + dynamic;
    }

    enum class WorkflowStep : std::uint8_t
    {
        None = 0,
        ZoneEnter = 1,
        ZoneMove = 2,
        ZoneLeave = 3,
        RoomJoinStep1_JoinRoom = 4,
        RoomJoinStep2_LeaveZone = 5,
        RoomReturnStep1_ZoneEnter = 6,
        RoomTerminalNotification = 7,
    };

    struct WorkflowReplyTo
    {
        snf::server::PlayerId player{0};
        snf::worker::ConnectionGeneration connection_generation{};
        std::uint64_t correlation_id{0};
        WorkflowStep step{WorkflowStep::None};
        std::uint64_t route_epoch{0};
        std::uint32_t request_id{0};
    };

    struct PlayerCommandMessage
    {
        std::optional<snf::worker::ConnectionRef> connection{std::nullopt};
        std::uint32_t request_id{0};
        snf::server::PlayerCommand command;
    };

    struct ExperienceGrantMessage
    {
        snf::server::StreetExperienceGrant grant;
    };

    struct ZoneCommandMessage
    {
        std::optional<snf::worker::ConnectionRef> connection{std::nullopt};
        std::uint32_t request_id{0};
        snf::server::ZoneCommand command;
        std::optional<WorkflowReplyTo> reply_to{std::nullopt};
    };

    struct ZoneTickMessage
    {
        std::uint64_t tick{0};
    };

    struct RoomCommandMessage
    {
        std::optional<snf::worker::ConnectionRef> connection{std::nullopt};
        std::uint32_t request_id{0};
        snf::server::RoomCommand command;
        std::optional<WorkflowReplyTo> reply_to{std::nullopt};
    };

    struct RoomDeadlineMessage
    {
        snf::server::RoomId room;
        std::uint64_t sequence{0};
    };

    struct RoomTickMessage
    {
        snf::server::RoomId room;
        std::uint64_t tick{0};
    };

    struct PingMessage
    {
        snf::worker::ConnectionRef connection;
        std::uint32_t request_id{0};
        std::vector<std::byte> payload;
    };

    // Wakes a player actor to persist itself. Scheduled as a one-shot timer by the
    // actor's own turn, so only one can be outstanding at a time.
    struct PlayerSaveMessage
    {
        snf::server::PlayerId player{};
    };

    // Tells the player actor that the connection it is bound to is gone. The sink
    // sends it from onConnectionClosed, because the actor holds the
    // player -> connection half of the session identity and would otherwise keep
    // refusing the same player's next connection as a conflict.
    struct PlayerConnectionClosedMessage
    {
        snf::worker::ConnectionRef connection;
    };

    struct EnterZoneRequest
    {
        snf::server::ZoneId zone;
        snf::server::ZonePosition position;
    };

    struct MoveRequest
    {
        snf::server::ZonePosition position;
    };

    struct LeaveRequest
    {
    };

    using ZoneRequest = std::variant<EnterZoneRequest, MoveRequest, LeaveRequest>;

    struct PlayerZoneRequestMessage
    {
        snf::worker::ConnectionRef connection;
        std::uint32_t request_id{0};
        ZoneRequest request;
    };

    struct ZoneOutcomeMessage
    {
        snf::server::PlayerId player{0};
        snf::worker::ConnectionGeneration connection_generation{};
        std::uint64_t correlation_id{0};
        WorkflowStep step{WorkflowStep::None};
        snf::server::ZoneId zone{0};
        std::uint64_t route_epoch{0};
        std::uint32_t request_id{0};
        snf::server::ZoneResult result;
    };

    struct RoomOutcomeMessage
    {
        snf::server::PlayerId player{0};
        snf::worker::ConnectionGeneration connection_generation{};
        std::uint64_t correlation_id{0};
        WorkflowStep step{WorkflowStep::None};
        snf::server::RoomId room{0};
        std::uint32_t request_id{0};
        snf::server::RoomResult result;
    };

    struct PlayerWorkflowTimeoutMessage
    {
        std::uint64_t correlation_id{0};
        WorkflowStep step{WorkflowStep::None};
    };

    struct RoomJoinRequest
    {
        snf::server::RoomId room;
    };

    struct BattleStartRequest
    {
        snf::server::RoomId room;
    };

    struct RoomLeaveRequest
    {
    };

    struct UseSkillRequest
    {
        snf::server::RoomId room;
        snf::server::SkillId skill_id;
        std::uint64_t request_sequence{0};
    };

    struct SetMoveIntentRequest
    {
        snf::server::RoomId room;
        snf::server::MoveDirection direction{snf::server::MoveDirection::Stop};
        std::uint64_t request_sequence{0};
    };

    using RoomRequest = std::variant<RoomJoinRequest, BattleStartRequest, RoomLeaveRequest, UseSkillRequest, SetMoveIntentRequest>;

    struct PlayerRoomRequestMessage
    {
        snf::worker::ConnectionRef connection;
        std::uint32_t request_id{0};
        RoomRequest request;
    };
}

namespace snf::worker
{
    template <> struct ActorPayloadTraits<snf::adapter::PlayerSaveMessage>
    {
        static constexpr std::uint32_t TAG = 9;
        static std::uint64_t calculateCharge(const snf::adapter::PlayerSaveMessage&) noexcept
        {
            return sizeof(snf::adapter::PlayerSaveMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::PlayerConnectionClosedMessage>
    {
        static constexpr std::uint32_t TAG = 10;
        static std::uint64_t calculateCharge(const snf::adapter::PlayerConnectionClosedMessage&) noexcept
        {
            return sizeof(snf::adapter::PlayerConnectionClosedMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::PlayerCommandMessage>
    {
        static constexpr std::uint32_t TAG = 1;
        static std::uint64_t calculateCharge(const snf::adapter::PlayerCommandMessage& msg) noexcept
        {
            if (std::holds_alternative<snf::server::PingCommand>(msg.command))
            {
                return snf::adapter::logicalCharge(
                    sizeof(snf::adapter::PlayerCommandMessage), std::get<snf::server::PingCommand>(msg.command).payload.capacity()
                );
            }
            return sizeof(snf::adapter::PlayerCommandMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::ExperienceGrantMessage>
    {
        static constexpr std::uint32_t TAG = 2;
        static std::uint64_t calculateCharge(const snf::adapter::ExperienceGrantMessage&) noexcept
        {
            return sizeof(snf::adapter::ExperienceGrantMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::ZoneCommandMessage>
    {
        static constexpr std::uint32_t TAG = 3;
        static std::uint64_t calculateCharge(const snf::adapter::ZoneCommandMessage&) noexcept
        {
            return sizeof(snf::adapter::ZoneCommandMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::ZoneTickMessage>
    {
        static constexpr std::uint32_t TAG = 4;
        static std::uint64_t calculateCharge(const snf::adapter::ZoneTickMessage&) noexcept
        {
            return sizeof(snf::adapter::ZoneTickMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::RoomCommandMessage>
    {
        static constexpr std::uint32_t TAG = 5;
        static std::uint64_t calculateCharge(const snf::adapter::RoomCommandMessage&) noexcept
        {
            return sizeof(snf::adapter::RoomCommandMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::RoomDeadlineMessage>
    {
        static constexpr std::uint32_t TAG = 6;
        static std::uint64_t calculateCharge(const snf::adapter::RoomDeadlineMessage&) noexcept
        {
            return sizeof(snf::adapter::RoomDeadlineMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::RoomTickMessage>
    {
        static constexpr std::uint32_t TAG = 7;
        static std::uint64_t calculateCharge(const snf::adapter::RoomTickMessage&) noexcept
        {
            return sizeof(snf::adapter::RoomTickMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::PingMessage>
    {
        static constexpr std::uint32_t TAG = 8;
        static std::uint64_t calculateCharge(const snf::adapter::PingMessage& msg) noexcept
        {
            return snf::adapter::logicalCharge(sizeof(snf::adapter::PingMessage), msg.payload.capacity());
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::PlayerZoneRequestMessage>
    {
        static constexpr std::uint32_t TAG = 11;
        static std::uint64_t calculateCharge(const snf::adapter::PlayerZoneRequestMessage&) noexcept
        {
            return sizeof(snf::adapter::PlayerZoneRequestMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::ZoneOutcomeMessage>
    {
        static constexpr std::uint32_t TAG = 12;
        static std::uint64_t calculateCharge(const snf::adapter::ZoneOutcomeMessage& msg) noexcept
        {
            return snf::adapter::logicalCharge(
                sizeof(snf::adapter::ZoneOutcomeMessage), msg.result.visible_players.capacity() * sizeof(snf::server::PlayerId)
            );
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::RoomOutcomeMessage>
    {
        static constexpr std::uint32_t TAG = 13;
        static std::uint64_t calculateCharge(const snf::adapter::RoomOutcomeMessage& msg) noexcept
        {
            return snf::adapter::logicalCharge(
                sizeof(snf::adapter::RoomOutcomeMessage),
                msg.result.audience.capacity() * sizeof(snf::server::PlayerId) +
                    msg.result.grants.capacity() * sizeof(snf::server::StreetExperienceGrant)
            );
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::PlayerWorkflowTimeoutMessage>
    {
        static constexpr std::uint32_t TAG = 14;
        static std::uint64_t calculateCharge(const snf::adapter::PlayerWorkflowTimeoutMessage&) noexcept
        {
            return sizeof(snf::adapter::PlayerWorkflowTimeoutMessage);
        }
    };

    template <> struct ActorPayloadTraits<snf::adapter::PlayerRoomRequestMessage>
    {
        static constexpr std::uint32_t TAG = 15;
        static std::uint64_t calculateCharge(const snf::adapter::PlayerRoomRequestMessage&) noexcept
        {
            return sizeof(snf::adapter::PlayerRoomRequestMessage);
        }
    };
}

namespace snf::adapter
{
    using GameActorPayloadRegistry = snf::worker::ActorPayloadRegistry<
        PlayerCommandMessage,
        ExperienceGrantMessage,
        ZoneCommandMessage,
        ZoneTickMessage,
        RoomCommandMessage,
        RoomDeadlineMessage,
        RoomTickMessage,
        PingMessage,
        PlayerSaveMessage,
        PlayerConnectionClosedMessage,
        PlayerZoneRequestMessage,
        ZoneOutcomeMessage,
        RoomOutcomeMessage,
        PlayerWorkflowTimeoutMessage,
        PlayerRoomRequestMessage>;
}
