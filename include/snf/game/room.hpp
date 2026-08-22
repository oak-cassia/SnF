#pragma once

#include "snf/game/room_command.hpp"
#include "snf/game/room_id.hpp"
#include "snf/game/room_result.hpp"
#include "snf/game/skill_catalog.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace snf::server
{
    struct RoomConfig
    {
        // The battle's deadline rather than its length: the boss surviving it is
        // what fails the battle. Nothing can kill a participant yet, so this is the
        // only way to fail.
        std::chrono::milliseconds battle_duration{5000};
        std::size_t max_participants{4};
        std::uint64_t clear_experience{300};
        std::uint64_t boss_health{1000};
    };

    // The game model, not the execution unit: being an actor is how a Room is run,
    // which is RoomActorBinding's business. This is a pure state machine with no
    // clock, no sockets and no runtime types, which is what keeps its tests
    // deterministic. It is told when the turn handling a command began instead of
    // reading a clock -- see docs/actor-messaging-and-game-time.md.
    //
    //   Waiting --StartBattle--> Running --boss reaches 0 HP--> Cleared
    //                                    --BattleDeadline-----> Failed
    //
    // A LeaveRoom is accepted in either non-terminal phase: before the battle it
    // frees the seat, during one it forfeits the reward.
    //
    class Room
    {
    public:
        explicit Room(RoomId room, RoomConfig config = {});

        [[nodiscard]] RoomId id() const noexcept;
        [[nodiscard]] RoomPhase phase() const noexcept;
        [[nodiscard]] std::size_t participantCount() const noexcept;
        [[nodiscard]] std::uint64_t bossHealth() const noexcept;
        // The snapshot this player entered with, absent when they are not in.
        [[nodiscard]] std::optional<CombatStats> statsOf(PlayerId player) const;

        // observed_at is when the turn handling this command began, which the owning
        // Worker knows and the Room must not go looking for.
        [[nodiscard]] RoomResult handle(const RoomCommand& command, std::chrono::steady_clock::time_point observed_at);

    private:
        struct SkillCooldown
        {
            SkillId skill;
            // The first moment this skill may be cast again. A deadline rather than
            // a counter that something has to decrement: nothing needs to visit a
            // participant on a schedule just to let time pass.
            std::chrono::steady_clock::time_point ready_at;
        };

        struct Participant
        {
            PlayerId player;
            CombatStats stats;
            // High-water mark, not a set of seen values: sequences increase, so one
            // number rejects every resend and cannot grow without bound. It only
            // advances when a cast is applied, so a rejected sequence stays usable.
            std::uint64_t applied_sequence{0};
            // Ascending SkillId. One entry per skill actually cast.
            std::vector<SkillCooldown> cooldowns;
        };

        [[nodiscard]] RoomResult handleCommand(const JoinRoom& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const LeaveRoom& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const StartBattle& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const UseSkill& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const BattleDeadline& command, std::chrono::steady_clock::time_point observed_at);

        [[nodiscard]] Participant* findParticipant(PlayerId player);
        // Every participant the Room still holds, ascending PlayerId.
        [[nodiscard]] std::vector<PlayerId> audience() const;
        [[nodiscard]] RoomResult refuse(RoomCommandStatus status, std::optional<PlayerId> player) const;

        RoomId _room;
        RoomConfig _config;
        RoomPhase _phase{RoomPhase::Waiting};
        std::uint64_t _boss_health{0};

        // Ascending PlayerId, so a clear emits its rewards in a deterministic order.
        std::vector<Participant> _participants;
    };
}
