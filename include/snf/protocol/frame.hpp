#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace snf::protocol
{
    inline constexpr std::uint32_t FRAME_LENGTH_FIELD_SIZE = 4;
    inline constexpr std::uint32_t FRAME_TYPE_SIZE = 2;
    inline constexpr std::uint32_t FRAME_REQUEST_ID_SIZE = 4;

    inline constexpr std::uint32_t MIN_BODY_SIZE = FRAME_TYPE_SIZE + FRAME_REQUEST_ID_SIZE;
    inline constexpr std::uint32_t MAX_BODY_SIZE = 64 * 1024;
    inline constexpr std::uint32_t MAX_PAYLOAD_SIZE = MAX_BODY_SIZE - MIN_BODY_SIZE;

    enum class MessageType : std::uint16_t
    {
        Ping = 1,
        Pong = 2,
        Authenticate = 3,
        Authenticated = 4,
        EnterZone = 5,
        ZoneEntered = 6,
        Move = 7,
        Moved = 8,
        LeaveZone = 9,
        ZoneLeft = 10,
        Purchase = 11,
        PurchaseResult = 12,
        PartyJoin = 13,
        PartyJoined = 14,
        PartyLeave = 15,
        PartyLeft = 16,
        RoomJoin = 17,
        RoomJoined = 18,
        BattleStart = 19,
        BattleStarted = 20,
        BattleCleared = 21,
        RoomLeave = 22,
        RoomLeft = 23,
        ReturnedToZone = 24,
        UseSkill = 25,
        SkillApplied = 26,
        BattleFailed = 27,
        BattleDigest = 28,
        SkillAcknowledged = 29,
    };

    // A request id of zero marks a frame the server sent on its own rather than in
    // answer to a request. BattleCleared is the first: a battle ends on the Room's
    // own timer, so there is no client frame to correlate it with.
    inline constexpr std::uint32_t UNSOLICITED_REQUEST_ID = 0;

    struct Frame
    {
        MessageType type;
        std::uint32_t request_id;
        std::vector<std::byte> payload;
    };
}
