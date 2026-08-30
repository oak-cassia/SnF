#pragma once

#include "snf/game/player_result.hpp"
#include "snf/game/room_result.hpp"
#include "snf/game/zone_result.hpp"
#include "snf/protocol/frame.hpp"

#include <cstdint>

namespace snf::adapter
{
    [[nodiscard]] snf::protocol::Frame encodePlayerResponse(
        const snf::server::PlayerResponse& response,
        std::uint32_t request_id
    );

    [[nodiscard]] snf::protocol::Frame encodeZoneReply(
        const snf::server::ZoneResult& result,
        std::uint32_t request_id
    );

    [[nodiscard]] snf::protocol::Frame encodeRoomReply(
        const snf::server::RoomResult& result,
        std::uint32_t request_id
    );

    [[nodiscard]] snf::protocol::Frame encodeBattleDigest(
        const snf::server::BattleDigest& digest,
        std::uint32_t request_id = 0
    );

    [[nodiscard]] snf::protocol::Frame encodeBattleOutcome(
        const snf::server::RoomResult& result,
        std::uint32_t request_id = 0
    );
}
