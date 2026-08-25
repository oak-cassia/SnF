#pragma once

#include "snf/game/enemy.hpp"
#include "snf/game/room_command.hpp"
#include "snf/game/room_id.hpp"
#include "snf/game/room_result.hpp"

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
        std::uint64_t boss_health{500};
        std::chrono::milliseconds tick_interval{100};
        std::chrono::milliseconds wave_interval{20000};
        std::size_t wave_count{2};
        std::size_t minions_per_wave{10};
        std::uint64_t minion_health{60};
        std::chrono::milliseconds boss_spawn_after{40000};
        std::size_t max_spawned_enemies{64};
        std::size_t digest_flush_threshold{512};
        std::uint32_t arena_width{100};
        std::uint32_t arena_height{100};
        std::uint32_t player_move_speed{4};
        std::uint32_t participant_spawn_spacing{4};
        std::uint32_t minion_spawn_radius{25};
        std::uint32_t minion_move_speed{1};
        std::uint32_t boss_move_speed{1};
        std::uint64_t minion_attack_damage{3};
        std::uint64_t boss_attack_damage{10};
        std::uint32_t minion_attack_range{3};
        std::uint32_t boss_attack_range{5};
        std::chrono::milliseconds minion_attack_cooldown{1000};
        std::chrono::milliseconds boss_attack_cooldown{2000};
    };

    class Room
    {
    public:
        explicit Room(RoomId room, RoomConfig config = {});

        [[nodiscard]] RoomId id() const noexcept;
        [[nodiscard]] RoomPhase phase() const noexcept;
        [[nodiscard]] std::size_t participantCount() const noexcept;
        [[nodiscard]] std::size_t enemyCount() const noexcept;
        [[nodiscard]] bool canStartBattle() const noexcept;
        [[nodiscard]] bool bossSpawned() const noexcept;
        [[nodiscard]] std::uint64_t bossHealth() const noexcept;
        [[nodiscard]] std::optional<CombatStats> statsOf(PlayerId player) const;
        [[nodiscard]] std::optional<std::uint64_t> healthOf(PlayerId player) const;
        [[nodiscard]] std::optional<ArenaPosition> positionOf(PlayerId player) const;

        [[nodiscard]] RoomResult handle(const RoomCommand& command, std::chrono::steady_clock::time_point observed_at);

    private:
        struct SkillCooldown
        {
            SkillId skill_id;
            std::chrono::steady_clock::time_point ready_at;
        };

        struct Participant
        {
            PlayerId player;
            CombatStats stats;
            std::uint64_t current_health{0};
            ArenaPosition position{};
            MoveDirection move_intent{MoveDirection::Stop};
            std::uint64_t applied_skill_sequence{0};
            std::uint64_t applied_movement_sequence{0};
            std::vector<SkillCooldown> cooldowns;
        };

        [[nodiscard]] RoomResult handleCommand(const JoinRoom& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const LeaveRoom& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const StartBattle& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const UseSkill& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const BattleDeadline& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const RoomSimulationTick& command, std::chrono::steady_clock::time_point observed_at);
        [[nodiscard]] RoomResult handleCommand(const SetMoveIntent& command, std::chrono::steady_clock::time_point observed_at);

        [[nodiscard]] Participant* findParticipant(PlayerId player);
        [[nodiscard]] Participant* nearestLivingParticipant(ArenaPosition origin);
        [[nodiscard]] const Enemy* boss() const noexcept;
        [[nodiscard]] bool allParticipantsDead() const noexcept;
        [[nodiscard]] std::uint32_t enemyMoveSpeed(EnemyKind kind) const noexcept;
        [[nodiscard]] std::uint32_t enemyAttackRange(EnemyKind kind) const noexcept;
        [[nodiscard]] std::uint64_t enemyAttackDamage(EnemyKind kind) const noexcept;
        [[nodiscard]] std::chrono::milliseconds enemyAttackCooldown(EnemyKind kind) const noexcept;
        [[nodiscard]] std::vector<PlayerId> audience() const;
        [[nodiscard]] RoomResult baseResult(RoomCommandStatus status, std::optional<PlayerId> player) const;
        [[nodiscard]] RoomResult failBattle(RoomCommandStatus status, std::optional<PlayerId> player, BattleFailureReason reason);
        [[nodiscard]] std::optional<BattleDigest> takeDigest();
        void initializeArena();
        void moveParticipants();
        [[nodiscard]] bool actEnemies(std::chrono::steady_clock::time_point observed_at);
        void spawnWave(std::chrono::steady_clock::time_point observed_at);
        void spawnBoss(std::chrono::steady_clock::time_point observed_at);
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
