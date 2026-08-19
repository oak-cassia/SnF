#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/street_experience_grant.hpp"

#include <chrono>
#include <cstdint>
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

    // What the Room decided, in game terms only. Turning complete_after into a
    // timer and grants into messages is the binding's job, so nothing here names
    // an ActorKey, a mailbox, or a payload carrier -- and the result stays a plain
    // copyable value that a test can hold on to.
    struct RoomResult
    {
        RoomCommandStatus status{RoomCommandStatus::Applied};
        RoomPhase phase{RoomPhase::Waiting};
        std::optional<PlayerId> player{};
        // How long this battle still needs. Set once, when it starts.
        std::optional<std::chrono::milliseconds> complete_after{};
        std::vector<StreetExperienceGrant> grants{};
    };
}
