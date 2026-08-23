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
        BattleExpired,
        ParticipantDead,
        RuntimeOverloaded,
    };

    enum class BattleOutcome : std::uint8_t
    {
        Cleared,
        Failed,
    };

    enum class BattleFailureReason : std::uint8_t
    {
        Deadline = 0,
        ParticipantsDefeated = 1,
    };

    enum class BattleEventKind : std::uint8_t
    {
        EnemySpawned = 0,
        EnemyDamaged = 1,
        EnemyDied = 2,
        SkillWhiffed = 3,
        ArenaStarted = 4,
        ParticipantSpawned = 5,
        ParticipantMoved = 6,
        EnemyPositioned = 7,
        ParticipantDamaged = 8,
        ParticipantDied = 9,
        ParticipantLeft = 10,
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

    struct ArenaStarted
    {
        std::uint32_t width{0};
        std::uint32_t height{0};

        [[nodiscard]] bool operator==(const ArenaStarted&) const noexcept = default;
    };

    struct ParticipantSpawned
    {
        PlayerId player{};
        ArenaPosition position{};
        std::uint64_t health{0};

        [[nodiscard]] bool operator==(const ParticipantSpawned&) const noexcept = default;
    };

    struct ParticipantMoved
    {
        PlayerId player{};
        ArenaPosition position{};

        [[nodiscard]] bool operator==(const ParticipantMoved&) const noexcept = default;
    };

    struct EnemyPositioned
    {
        EnemyId enemy{};
        ArenaPosition position{};

        [[nodiscard]] bool operator==(const EnemyPositioned&) const noexcept = default;
    };

    struct ParticipantDamaged
    {
        PlayerId target{};
        EnemyId attacker{};
        std::uint64_t amount{0};
        std::uint64_t health{0};

        [[nodiscard]] bool operator==(const ParticipantDamaged&) const noexcept = default;
    };

    struct ParticipantDied
    {
        PlayerId player{};

        [[nodiscard]] bool operator==(const ParticipantDied&) const noexcept = default;
    };

    struct ParticipantLeft
    {
        PlayerId player{};

        [[nodiscard]] bool operator==(const ParticipantLeft&) const noexcept = default;
    };

    using BattleEvent = std::variant<
        EnemySpawned,
        EnemyDamaged,
        EnemyDied,
        SkillWhiffed,
        ArenaStarted,
        ParticipantSpawned,
        ParticipantMoved,
        EnemyPositioned,
        ParticipantDamaged,
        ParticipantDied,
        ParticipantLeft>;

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
        std::optional<BattleOutcome> outcome{};
        std::optional<BattleFailureReason> failure_reason{};
        std::vector<PlayerId> audience{};
        std::vector<StreetExperienceGrant> grants{};
    };
}
