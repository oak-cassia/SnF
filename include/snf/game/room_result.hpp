#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/skill_id.hpp"
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
        // The boss outlived the deadline. Appended rather than inserted: the phase
        // crosses the wire as a byte, so the existing values are part of the
        // protocol.
        Failed,
    };

    enum class RoomCommandStatus : std::uint8_t
    {
        Applied,
        AlreadyJoined,
        RoomFull,
        // The command does not belong to the phase the Room is in. A second
        // BattleDeadline lands here, which is what keeps a failure from being
        // declared twice however the timer or the mailbox behaves.
        WrongPhase,
        // Appended rather than inserted: the status crosses the wire as a byte, so
        // the existing values are part of the protocol.
        NotJoined,
        EntryFailed,
        // A request_sequence at or below the caster's high-water mark. The cast it
        // names already landed, so nothing is applied a second time.
        DuplicateRequest,
        SkillOnCooldown,
        UnknownSkill,
    };

    // What one cast did. Every participant is told the same values, which is why
    // this is one struct rather than a payload built per recipient.
    struct SkillOutcome
    {
        PlayerId actor{};
        SkillId skill{};
        std::uint64_t damage{0};
    };

    // What the Room decided, in game terms only. Turning deadline_after into a
    // timer, grants into messages and the audience into sockets is the binding's
    // and the sink's job, so nothing here names an ActorKey, a mailbox, a
    // connection or a payload carrier -- and the result stays a plain copyable
    // value that a test can hold on to.
    struct RoomResult
    {
        RoomCommandStatus status{RoomCommandStatus::Applied};
        RoomPhase phase{RoomPhase::Waiting};
        std::optional<PlayerId> player{};
        // How long the battle has left to kill the boss. Set once, when it starts.
        std::optional<std::chrono::milliseconds> deadline_after{};
        // The boss as of this result, so a reply carries the state the caster is
        // acting against rather than the caller having to ask separately.
        std::uint64_t boss_health{0};
        // Absent unless a cast was applied. A rejected cast reports its status and
        // changes nothing.
        std::optional<SkillOutcome> skill{};
        // Who should be told about this result, ascending PlayerId. Empty when only
        // the requester cares. A Room names players and never connections, so
        // resolving these to sockets stays outside the game model -- and the return
        // to a Zone reads this list rather than the rewards, because a failed battle
        // still has to send everyone home.
        std::vector<PlayerId> audience{};
        // Rewards, which a failure does not pay.
        std::vector<StreetExperienceGrant> grants{};
    };
}
