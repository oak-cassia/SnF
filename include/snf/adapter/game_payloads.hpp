#pragma once

#include "snf/game/player_command.hpp"
#include "snf/game/player_id.hpp"
#include "snf/game/room_command.hpp"
#include "snf/game/room_id.hpp"
#include "snf/game/street_experience_grant.hpp"
#include "snf/game/zone_command.hpp"
#include "snf/game/zone_id.hpp"
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
        PlayerSaveMessage>;
}
