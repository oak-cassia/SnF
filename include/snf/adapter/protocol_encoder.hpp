#pragma once

#include "snf/game/player_result.hpp"
#include "snf/game/room_id.hpp"
#include "snf/game/room_result.hpp"
#include "snf/game/zone_id.hpp"
#include "snf/game/zone_result.hpp"
#include "snf/protocol/frame.hpp"

#include <cstdint>
#include <optional>

namespace snf::adapter
{
    enum class ZoneReplyFrameKind : std::uint8_t
    {
        Entered,
        Moved,
        Left,
    };

    enum class RoomReplyFrameKind : std::uint8_t
    {
        Joined,
        BattleStarted,
        SkillAcknowledged,
        MoveAcknowledged,
    };

    [[nodiscard]] snf::protocol::Frame encodePlayerResponse(const snf::server::PlayerResponse& response, std::uint32_t request_id);

    [[nodiscard]] snf::protocol::Frame encodeZoneReply(
        ZoneReplyFrameKind kind,
        snf::server::ZoneId zone,
        const snf::server::ZoneResult& result,
        std::uint32_t request_id
    );

    [[nodiscard]] snf::protocol::Frame encodeRoomReply(
        RoomReplyFrameKind kind,
        snf::server::RoomId room,
        const snf::server::RoomResult& result,
        std::uint32_t request_id
    );

    [[nodiscard]] std::optional<snf::protocol::Frame> encodeBattleDigest(const snf::server::RoomResult& result, std::uint32_t request_id = 0);

    [[nodiscard]] snf::protocol::Frame encodeBattleCleared(std::uint64_t experience, std::uint32_t request_id = 0);

    [[nodiscard]] snf::protocol::Frame encodeBattleFailure(const snf::server::RoomResult& result, std::uint32_t request_id = 0);
    [[nodiscard]] snf::protocol::Frame encodeReturnedToZone(
        snf::server::ZoneId zone,
        snf::server::ZonePosition position,
        std::uint32_t request_id = 0
    );
}
