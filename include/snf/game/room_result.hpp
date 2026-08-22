#pragma once

#include "snf/game/enemy.hpp"
#include "snf/game/player_id.hpp"
#include "snf/game/skill_id.hpp"
#include "snf/game/street_experience_grant.hpp"

#include <chrono>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace snf::server
{
    enum class RoomPhase : std::uint8_t
    {
        Waiting,
        Running,
        Cleared,
        Failed,
    };

    enum class RoomCommandStatus : std::uint8_t
    {
        Applied,
        AlreadyJoined,
        RoomFull,
        WrongPhase,
        NotJoined,
        EntryFailed,
        DuplicateRequest,
        SkillOnCooldown,
        UnknownSkill,
        // The command reached a Running Room at or after its absolute deadline.
        // Appended because command statuses cross the wire as one byte.
        BattleExpired,
    };

    enum class BattleOutcome : std::uint8_t
    {
        Cleared,
        Failed,
    };

    // Crosses the wire as an event tag. Keep existing values fixed and append new
    // kinds only at the end.
    enum class BattleEventKind : std::uint8_t
    {
        EnemySpawned = 0,
        EnemyDamaged = 1,
        EnemyDied = 2,
        SkillWhiffed = 3,
    };

    struct EnemySpawned
    {
        EnemyId id{};
        EnemyKind kind{EnemyKind::Minion};
        std::uint64_t health{0};

        [[nodiscard]] bool operator==(const EnemySpawned&) const noexcept = default;
    };

    struct EnemyDamaged
    {
        EnemyId target{};
        PlayerId actor{};
        SkillId skill{};
        std::uint64_t amount{0};
        std::uint64_t health{0};

        [[nodiscard]] bool operator==(const EnemyDamaged&) const noexcept = default;
    };

    struct EnemyDied
    {
        EnemyId id{};

        [[nodiscard]] bool operator==(const EnemyDied&) const noexcept = default;
    };

    struct SkillWhiffed
    {
        PlayerId actor{};
        SkillId skill{};

        [[nodiscard]] bool operator==(const SkillWhiffed&) const noexcept = default;
    };

    using BattleEvent = std::variant<EnemySpawned, EnemyDamaged, EnemyDied, SkillWhiffed>;

    // One ordered observation window. The sequence advances only when a digest is
    // actually emitted, including start, capacity and terminal flushes outside a
    // simulation tick.
    struct BattleDigest
    {
        std::uint64_t sequence{0};
        std::vector<BattleEvent> events{};
    };

    struct RoomResult
    {
        RoomCommandStatus status{RoomCommandStatus::Applied};
        RoomPhase phase{RoomPhase::Waiting};
        std::optional<PlayerId> player{};
        std::optional<std::chrono::milliseconds> deadline_after{};
        std::optional<std::chrono::milliseconds> tick_after{};
        std::uint64_t boss_health{0};
        bool boss_spawned{false};
        std::optional<BattleDigest> digest{};
        // Present only on the result that changed Running into a terminal phase.
        std::optional<BattleOutcome> outcome{};
        std::vector<PlayerId> audience{};
        std::vector<StreetExperienceGrant> grants{};
    };
}
