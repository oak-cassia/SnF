#pragma once

#include "snf/server/follow_up_action.hpp"
#include "snf/server/player_id.hpp"

#include <optional>
#include <vector>

namespace snf::server
{
    enum class RoomPhase : std::uint8_t
    {
        Waiting,
        Running,
        Cleared,
    };

    enum class RoomCommandStatus : std::uint8_t
    {
        Applied,
        AlreadyJoined,
        RoomFull,
        // The command does not belong to the phase the Room is in. A second
        // BattleCompleted lands here, which is what keeps a clear from paying out
        // twice however the timer or the mailbox behaves.
        WrongPhase,
    };

    // Non-copyable, because TellActor owns a move-only TellPayload. A recorder in a
    // test has to keep the fields it asserts on rather than the whole result.
    struct RoomResult
    {
        RoomCommandStatus status{RoomCommandStatus::Applied};
        RoomPhase phase{RoomPhase::Waiting};
        std::optional<PlayerId> player;
        std::vector<FollowUpAction> follow_ups;
    };
}
