#pragma once

#include "snf/game/enemy.hpp"
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
        std::chrono::milliseconds battle_duration{90000};
        std::size_t max_participants{4};
        std::uint64_t clear_experience{300};
        std::uint64_t boss_health{1000};
        std::chrono::milliseconds tick_interval{100};
        std::chrono::milliseconds wave_interval{20000};
        std::size_t wave_count{2};
        std::size_t minions_per_wave{10};
        std::uint64_t minion_health{30};
        std::chrono::milliseconds boss_spawn_after{40000};
        // A lifetime spawn bound, not merely the number alive at once.
        std::size_t max_spawned_enemies{64};
        // A flush threshold rather than a drop boundary. The command that crosses
        // it stays atomic and may take the final count slightly above this value.
        std::size_t digest_flush_threshold{512};
    };

    // A clock-free state machine. Its binding supplies observed_at and translates
    // deadline_after/tick_after into Worker-local timers.
    class Room
    {
    public:
        explicit Room(RoomId room, RoomConfig config = {});

        [[nodiscard]] RoomId id() const noexcept;
        [[nodiscard]] RoomPhase phase() const noexcept;
        [[nodiscard]] std::size_t participantCount() const noexcept;
        [[nodiscard]] std::size_t enemyCount() const noexcept;
        [[nodiscard]] bool bossSpawned() const noexcept;
        [[nodiscard]] std::uint64_t bossHealth() const noexcept;
        [[nodiscard]] std::optional<CombatStats> statsOf(PlayerId player) const;

        [[nodiscard]] RoomResult handle(const RoomCommand& command, std::chrono::steady_clock::time_point observed_at);

    private:
        struct SkillCooldown
        {
            SkillId skill;
            std::chrono::steady_clock::time_point ready_at;
        };

        struct Participant
        {
            PlayerId player;
            CombatStats stats;
            std::uint64_t applied_sequence{0};
            std::vector<SkillCooldown> cooldowns;
        };

        [[nodiscard]] RoomResult handleCommand(const JoinRoom& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const LeaveRoom& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const StartBattle& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const UseSkill& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const BattleDeadline& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const RoomSimulationTick& command, std::chrono::steady_clock::time_point observed_at);

        [[nodiscard]] Participant* findParticipant(PlayerId player);
        [[nodiscard]] Enemy* firstLivingEnemy();
        [[nodiscard]] const Enemy* boss() const noexcept;
        [[nodiscard]] std::vector<PlayerId> audience() const;
        [[nodiscard]] RoomResult baseResult(RoomCommandStatus status, std::optional<PlayerId> player) const;
        [[nodiscard]] RoomResult failBattle(RoomCommandStatus status, std::optional<PlayerId> player);
        [[nodiscard]] std::optional<BattleDigest> takeDigest();
        void spawnWave();
        void spawnBoss();
        void rewardClear(RoomResult& result) const;

        RoomId _room;
        RoomConfig _config;
        RoomPhase _phase{RoomPhase::Waiting};
        std::chrono::steady_clock::time_point _battle_deadline_at{};
        std::chrono::steady_clock::time_point _next_wave_at{};
        std::chrono::steady_clock::time_point _boss_spawn_at{};
        std::size_t _spawned_wave_count{0};
        std::uint32_t _next_enemy_id{1};
        std::uint64_t _digest_sequence{0};
        bool _boss_spawned{false};

        std::vector<Participant> _participants;
        std::vector<Enemy> _enemies;
        std::vector<BattleEvent> _pending_events;
    };
}
