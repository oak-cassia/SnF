#include "snf/game/room.hpp"

#include <cassert>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <variant>
#include <vector>

import snf.game.skill_catalog;

namespace
{
    using namespace std::chrono_literals;
    using snf::server::ARCANE_BOLT;
    using snf::server::ArenaPosition;
    using snf::server::ArenaStarted;
    using snf::server::BattleFailureReason;
    using snf::server::BattleOutcome;
    using snf::server::EnemyDamaged;
    using snf::server::EnemyDied;
    using snf::server::EnemyId;
    using snf::server::EnemyKind;
    using snf::server::EnemyPositioned;
    using snf::server::EnemySpawned;
    using snf::server::JoinRoom;
    using snf::server::MoveDirection;
    using snf::server::ParticipantDamaged;
    using snf::server::ParticipantDied;
    using snf::server::ParticipantLeft;
    using snf::server::ParticipantMoved;
    using snf::server::ParticipantSpawned;
    using snf::server::PlayerId;
    using snf::server::Projectile;
    using snf::server::ProjectileId;
    using snf::server::ProjectileMoved;
    using snf::server::ProjectileRemoved;
    using snf::server::ProjectileRemovalReason;
    using snf::server::ProjectileSpawned;
    using snf::server::Room;
    using snf::server::RoomCommandStatus;
    using snf::server::RoomConfig;
    using snf::server::RoomId;
    using snf::server::RoomPhase;
    using snf::server::RoomSimulationTick;
    using snf::server::SetMoveIntent;
    using snf::server::SkillId;
    using snf::server::SkillWhiffed;
    using snf::server::SLASH;
    using snf::server::StartBattle;
    using snf::server::UseSkill;

    [[nodiscard]] std::chrono::steady_clock::time_point at(const std::int64_t milliseconds)
    {
        return std::chrono::steady_clock::time_point{std::chrono::milliseconds{milliseconds}};
    }

    [[nodiscard]] RoomConfig wave_room()
    {
        return RoomConfig{
            .battle_duration = 5000ms,
            .max_participants = 4,
            .clear_experience = 300,
            .boss_health = 50,
            .tick_interval = 100ms,
            .wave_interval = 1000ms,
            .wave_count = 2,
            .minions_per_wave = 2,
            .minion_health = 10,
            .boss_spawn_after = 3000ms,
            .max_spawned_enemies = 8,
            .digest_flush_threshold = 512,
            .arena_width = 20,
            .arena_height = 20,
            .player_move_speed = 2,
            .participant_spawn_spacing = 2,
            .minion_spawn_radius = 5,
            .minion_move_speed = 2,
            .boss_move_speed = 1,
            .minion_attack_damage = 5,
            .boss_attack_damage = 10,
            .minion_attack_range = 1,
            .boss_attack_range = 2,
            .minion_attack_cooldown = 1000ms,
            .boss_attack_cooldown = 2000ms,
        };
    }

    [[nodiscard]] RoomConfig boss_room()
    {
        RoomConfig config = wave_room();
        config.wave_count = 0;
        config.minions_per_wave = 0;
        config.boss_spawn_after = 1000ms;
        return config;
    }

    [[nodiscard]] RoomConfig single_minion_room()
    {
        RoomConfig config = wave_room();
        config.wave_count = 1;
        config.minions_per_wave = 1;
        config.max_spawned_enemies = 2;
        config.minion_attack_cooldown = 10000ms;
        return config;
    }

    void join(
        Room& room,
        const PlayerId player,
        const std::uint64_t attack = 10,
        const std::uint64_t health = 100,
        const SkillId equipped_skill_id = SLASH
    )
    {
        const auto result = room.handle(
            JoinRoom{.player = player, .stats = {.attack = attack, .health = health}, .equipped_skill_id = equipped_skill_id},
            at(0)
        );
        assert(result.status == RoomCommandStatus::Applied);
    }

    [[nodiscard]] Room
    started(
        const RoomConfig& config,
        const PlayerId player = PlayerId{.value = 7},
        const std::uint64_t attack = 10,
        const std::uint64_t health = 100,
        const SkillId equipped_skill_id = SLASH
    )
    {
        Room room{RoomId{.value = 1}, config};
        join(room, player, attack, health, equipped_skill_id);
        const auto result = room.handle(StartBattle{}, at(0));
        assert(result.status == RoomCommandStatus::Applied);
        return room;
    }

    template <typename Event> [[nodiscard]] const Event& event_at(const snf::server::BattleDigest& digest, const std::size_t index)
    {
        const auto* event = std::get_if<Event>(&digest.events.at(index));
        assert(event != nullptr);
        return *event;
    }

    template <typename Event> [[nodiscard]] std::vector<const Event*> events_of(const snf::server::BattleDigest& digest)
    {
        std::vector<const Event*> events;
        for (const snf::server::BattleEvent& event : digest.events)
        {
            if (const auto* value = std::get_if<Event>(&event))
            {
                events.push_back(value);
            }
        }
        return events;
    }

    void test_room_config_rejects_invalid_simulation_and_arena_bounds()
    {
        auto rejects = [](const RoomConfig& config)
        {
            bool threw = false;
            try
            {
                static_cast<void>(Room{RoomId{.value = 1}, config});
            }
            catch (const std::invalid_argument&)
            {
                threw = true;
            }
            assert(threw);
        };

        assert(RoomConfig{}.max_active_projectiles == 128);
        RoomConfig invalid = wave_room();
        invalid.max_active_projectiles = 0;
        rejects(invalid);
        if constexpr (sizeof(std::size_t) > sizeof(std::uint32_t))
        {
            invalid = wave_room();
            invalid.max_active_projectiles = static_cast<std::size_t>(std::numeric_limits<std::uint32_t>::max()) + 1;
            rejects(invalid);
        }
        invalid = wave_room();
        invalid.tick_interval = 0ms;
        rejects(invalid);
        invalid = wave_room();
        invalid.boss_spawn_after = invalid.battle_duration;
        rejects(invalid);
        invalid = wave_room();
        invalid.digest_flush_threshold = 0;
        rejects(invalid);
        invalid = wave_room();
        invalid.max_spawned_enemies = 4;
        rejects(invalid);
        invalid = wave_room();
        invalid.boss_spawn_after = 500ms;
        rejects(invalid);
        invalid = wave_room();
        invalid.minions_per_wave = 0;
        rejects(invalid);
        invalid = wave_room();
        invalid.wave_count = std::numeric_limits<std::size_t>::max();
        invalid.minions_per_wave = 2;
        rejects(invalid);
        invalid = wave_room();
        invalid.arena_width = 0;
        rejects(invalid);
        invalid = wave_room();
        invalid.minion_spawn_radius = 10;
        rejects(invalid);
        invalid = wave_room();
        invalid.player_move_speed = 0;
        rejects(invalid);
        invalid = wave_room();
        invalid.participant_spawn_spacing = 20;
        rejects(invalid);
        invalid = wave_room();
        invalid.minion_attack_cooldown = 0ms;
        rejects(invalid);
    }

    void test_joining_is_bounded_and_keeps_snapshot_separate_from_current_state()
    {
        RoomConfig config = wave_room();
        config.max_participants = 1;
        Room room{RoomId{.value = 1}, config};

        join(room, PlayerId{.value = 10}, 37, 123);
        const auto refused = room.handle(JoinRoom{.player = PlayerId{.value = 20}}, at(0));

        assert(refused.status == RoomCommandStatus::RoomFull);
        assert(room.statsOf(PlayerId{.value = 10}) == (snf::server::CombatStats{.attack = 37, .health = 123}));
        assert(room.healthOf(PlayerId{.value = 10}) == 123);
        assert(room.participantCount() == 1);
    }

    void test_start_publishes_arena_participants_and_spawn_positions_in_order()
    {
        Room room{RoomId{.value = 1}, wave_room()};
        join(room, PlayerId{.value = 20});
        join(room, PlayerId{.value = 10});

        const auto result = room.handle(StartBattle{}, at(0));

        assert(result.phase == RoomPhase::Running);
        assert(result.deadline_after == 5000ms);
        assert(result.tick_after == 100ms);
        assert(result.digest && result.digest->sequence == 1);
        assert(result.digest->events.size() == 7);
        assert(event_at<ArenaStarted>(*result.digest, 0) == (ArenaStarted{.width = 20, .height = 20}));
        assert(event_at<ParticipantSpawned>(*result.digest, 1).player == PlayerId{.value = 10});
        assert(event_at<ParticipantSpawned>(*result.digest, 1).position == (ArenaPosition{.x = 9, .y = 10}));
        assert(event_at<ParticipantSpawned>(*result.digest, 2).player == PlayerId{.value = 20});
        assert(event_at<ParticipantSpawned>(*result.digest, 2).position == (ArenaPosition{.x = 11, .y = 10}));
        assert(event_at<EnemySpawned>(*result.digest, 3).id.value == 1);
        assert(event_at<EnemyPositioned>(*result.digest, 4).position == (ArenaPosition{.x = 10, .y = 5}));
        assert(event_at<EnemySpawned>(*result.digest, 5).id.value == 2);
        assert(event_at<EnemyPositioned>(*result.digest, 6).position == (ArenaPosition{.x = 10, .y = 15}));
        assert((result.audience == std::vector<PlayerId>{PlayerId{.value = 10}, PlayerId{.value = 20}}));
    }

    void test_movement_intent_persists_and_uses_an_independent_sequence()
    {
        Room room = started(boss_room());

        const auto move =
            room.handle(SetMoveIntent{.player = PlayerId{.value = 7}, .direction = MoveDirection::NorthEast, .request_sequence = 1}, at(0));
        const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = snf::server::SLASH, .request_sequence = 1}, at(0));
        const auto first_tick = room.handle(RoomSimulationTick{}, at(100));
        const auto second_tick = room.handle(RoomSimulationTick{}, at(200));
        const auto duplicate =
            room.handle(SetMoveIntent{.player = PlayerId{.value = 7}, .direction = MoveDirection::Stop, .request_sequence = 1}, at(200));
        const auto stop =
            room.handle(SetMoveIntent{.player = PlayerId{.value = 7}, .direction = MoveDirection::Stop, .request_sequence = 2}, at(200));
        const auto quiet = room.handle(RoomSimulationTick{}, at(300));

        assert(move.status == RoomCommandStatus::Applied);
        assert(cast.status == RoomCommandStatus::Applied);
        assert(first_tick.digest && events_of<ParticipantMoved>(*first_tick.digest).size() == 1);
        assert(room.positionOf(PlayerId{.value = 7}) == (ArenaPosition{.x = 14, .y = 6}));
        assert(second_tick.digest && events_of<ParticipantMoved>(*second_tick.digest).size() == 1);
        assert(duplicate.status == RoomCommandStatus::DuplicateRequest);
        assert(stop.status == RoomCommandStatus::Applied);
        assert(!quiet.digest);
    }

    void test_the_default_opening_cast_whiffs_and_consumes_cooldown_and_sequence()
    {
        Room room = started(RoomConfig{});

        const auto whiff = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = snf::server::SLASH, .request_sequence = 1}, at(0));
        const auto tick = room.handle(RoomSimulationTick{}, at(100));
        const auto early = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = snf::server::SLASH, .request_sequence = 2}, at(999));
        const auto duplicate = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = snf::server::SLASH, .request_sequence = 1}, at(1000));

        assert(whiff.status == RoomCommandStatus::Applied && !whiff.digest);
        assert(early.status == RoomCommandStatus::SkillOnCooldown);
        assert(duplicate.status == RoomCommandStatus::DuplicateRequest);
        assert(tick.digest && events_of<SkillWhiffed>(*tick.digest).size() == 1);
    }

    void test_cast_hits_every_enemy_in_range_in_enemy_id_order()
    {
        RoomConfig config = wave_room();
        config.wave_count = 1;
        config.max_spawned_enemies = 3;
        config.digest_flush_threshold = 2;
        Room room = started(config);

        const auto cast =
            room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = snf::server::SLASH, .request_sequence = 1}, at(0));

        assert(cast.digest && cast.digest->events.size() == 4);
        assert(event_at<EnemyDamaged>(*cast.digest, 0).target.value == 1);
        assert(event_at<EnemyDied>(*cast.digest, 1).id.value == 1);
        assert(event_at<EnemyDamaged>(*cast.digest, 2).target.value == 2);
        assert(event_at<EnemyDied>(*cast.digest, 3).id.value == 2);
        static_cast<void>(room.handle(RoomSimulationTick{}, at(100)));
        assert(room.enemyCount() == 0);
    }

    void test_digest_threshold_keeps_damage_and_death_atomic()
    {
        RoomConfig config = wave_room();
        config.wave_count = 1;
        config.minions_per_wave = 1;
        config.max_spawned_enemies = 2;
        config.digest_flush_threshold = 2;
        Room room = started(config);

        const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = snf::server::SLASH, .request_sequence = 1}, at(0));

        assert(cast.digest && cast.digest->events.size() == 2);
        static_cast<void>(event_at<EnemyDamaged>(*cast.digest, 0));
        static_cast<void>(event_at<EnemyDied>(*cast.digest, 1));
    }

    void test_enemy_movement_and_attack_are_interleaved_and_cooldown_is_absolute()
    {
        RoomConfig config = wave_room();
        config.wave_count = 1;
        config.minions_per_wave = 1;
        config.max_spawned_enemies = 2;
        config.minion_attack_damage = 25;
        config.minion_attack_cooldown = 200ms;
        Room room = started(config);

        const auto first = room.handle(RoomSimulationTick{}, at(100));
        const auto second = room.handle(RoomSimulationTick{}, at(200));
        const auto quiet = room.handle(RoomSimulationTick{}, at(300));
        const auto ready = room.handle(RoomSimulationTick{}, at(400));

        assert(first.digest && events_of<EnemyPositioned>(*first.digest).front()->position == (ArenaPosition{.x = 10, .y = 7}));
        assert(second.digest && second.digest->events.size() == 2);
        assert(event_at<EnemyPositioned>(*second.digest, 0).position == (ArenaPosition{.x = 10, .y = 9}));
        assert(event_at<ParticipantDamaged>(*second.digest, 1).health == 75);
        assert(!quiet.digest);
        assert(ready.digest && event_at<ParticipantDamaged>(*ready.digest, 0).health == 50);
    }

    void test_a_late_tick_applies_at_most_one_enemy_attack()
    {
        RoomConfig config = wave_room();
        config.wave_count = 1;
        config.minions_per_wave = 1;
        config.max_spawned_enemies = 2;
        config.minion_attack_damage = 25;
        config.minion_attack_range = 100;
        config.minion_attack_cooldown = 100ms;
        Room room = started(config);

        const auto late = room.handle(RoomSimulationTick{}, at(1000));
        const auto immediately_after = room.handle(RoomSimulationTick{}, at(1001));

        const auto damage = events_of<ParticipantDamaged>(*late.digest);
        assert(damage.size() == 1 && damage.front()->amount == 25 && damage.front()->health == 75);
        assert(!immediately_after.digest);
    }

    void test_enemies_retarget_after_death_and_fail_once_when_everyone_dies()
    {
        RoomConfig config = wave_room();
        config.wave_count = 1;
        config.minion_attack_damage = 100;
        config.minion_attack_range = 100;
        config.minion_attack_cooldown = 100ms;
        Room room{RoomId{.value = 1}, config};
        join(room, PlayerId{.value = 10});
        join(room, PlayerId{.value = 20});
        static_cast<void>(room.handle(StartBattle{}, at(0)));

        const auto failed = room.handle(RoomSimulationTick{}, at(100));
        const auto stale = room.handle(RoomSimulationTick{}, at(200));

        assert(failed.phase == RoomPhase::Failed);
        assert(failed.outcome == BattleOutcome::Failed);
        assert(failed.failure_reason == BattleFailureReason::ParticipantsDefeated);
        assert(failed.digest && failed.digest->events.size() == 4);
        assert(event_at<ParticipantDamaged>(*failed.digest, 0).target == PlayerId{.value = 10});
        assert(event_at<ParticipantDied>(*failed.digest, 1).player == PlayerId{.value = 10});
        assert(event_at<ParticipantDamaged>(*failed.digest, 2).target == PlayerId{.value = 20});
        assert(event_at<ParticipantDied>(*failed.digest, 3).player == PlayerId{.value = 20});
        assert(!failed.tick_after);
        assert(stale.status == RoomCommandStatus::WrongPhase && !stale.outcome);
    }

    void test_dead_participants_cannot_act_but_still_observe_and_receive_clear_reward()
    {
        RoomConfig config = wave_room();
        config.wave_count = 1;
        config.minions_per_wave = 1;
        config.minion_attack_damage = 100;
        config.minion_attack_range = 100;
        config.minion_attack_cooldown = 100ms;
        config.boss_spawn_after = 100ms;
        config.boss_health = 10;
        Room room{RoomId{.value = 1}, config};
        join(room, PlayerId{.value = 10});
        join(room, PlayerId{.value = 20});
        static_cast<void>(room.handle(StartBattle{}, at(0)));
        static_cast<void>(room.handle(RoomSimulationTick{}, at(100)));

        const auto dead_cast = room.handle(UseSkill{.player = PlayerId{.value = 10}, .skill_id = snf::server::SLASH, .request_sequence = 1}, at(100));
        const auto dead_move =
            room.handle(SetMoveIntent{.player = PlayerId{.value = 10}, .direction = MoveDirection::East, .request_sequence = 1}, at(100));
        const auto cleared = room.handle(UseSkill{.player = PlayerId{.value = 20}, .skill_id = snf::server::SLASH, .request_sequence = 1}, at(100));
        const auto stale = room.handle(UseSkill{.player = PlayerId{.value = 20}, .skill_id = snf::server::SLASH, .request_sequence = 2}, at(1100));

        assert(dead_cast.status == RoomCommandStatus::ParticipantDead);
        assert(dead_move.status == RoomCommandStatus::ParticipantDead);
        assert(cleared.outcome == BattleOutcome::Cleared);
        assert(events_of<EnemyDamaged>(*cleared.digest).size() == 2);
        assert(events_of<EnemyDied>(*cleared.digest).size() == 2);
        assert(cleared.audience == (std::vector<PlayerId>{PlayerId{.value = 10}, PlayerId{.value = 20}}));
        assert(cleared.grants.size() == 2);
        assert(stale.status == RoomCommandStatus::WrongPhase && !stale.outcome);
    }

    void test_late_tick_catches_up_spawns_but_new_enemies_wait_until_the_next_tick()
    {
        RoomConfig config = wave_room();
        config.wave_count = 3;
        config.minions_per_wave = 1;
        config.max_spawned_enemies = 4;
        config.boss_spawn_after = 4000ms;
        config.minion_attack_range = 100;
        config.minion_attack_cooldown = 10000ms;
        Room room = started(config);

        const auto tick = room.handle(RoomSimulationTick{}, at(2500));

        const auto spawned = events_of<EnemySpawned>(*tick.digest);
        const auto positioned = events_of<EnemyPositioned>(*tick.digest);
        assert(spawned.size() == 2 && spawned[0]->id.value == 2 && spawned[1]->id.value == 3);
        assert(positioned.size() == 2);
        assert(room.enemyCount() == 3);
        assert(tick.tick_after == 100ms);
    }

    void test_boss_spawns_at_its_absolute_time_after_the_minions()
    {
        RoomConfig config = wave_room();
        config.minion_attack_range = 100;
        config.minion_attack_cooldown = 10000ms;
        Room room = started(config);

        static_cast<void>(room.handle(RoomSimulationTick{}, at(2999)));
        const auto spawned = room.handle(RoomSimulationTick{}, at(3000));

        const auto enemies = events_of<EnemySpawned>(*spawned.digest);
        assert(enemies.size() == 1 && enemies.front()->kind == EnemyKind::Boss && enemies.front()->id.value == 5);
        assert(events_of<EnemyPositioned>(*spawned.digest).front()->position == (ArenaPosition{.x = 10, .y = 0}));
        assert(room.bossSpawned() && room.bossHealth() == 50);
    }

    void test_leave_is_observable_and_forfeits_reward()
    {
        RoomConfig config = boss_room();
        config.boss_health = 10;
        Room room{RoomId{.value = 1}, config};
        join(room, PlayerId{.value = 10});
        join(room, PlayerId{.value = 20});
        static_cast<void>(room.handle(StartBattle{}, at(0)));

        const auto left = room.handle(snf::server::LeaveRoom{.player = PlayerId{.value = 20}}, at(0));
        static_cast<void>(room.handle(RoomSimulationTick{}, at(1000)));
        const auto cleared = room.handle(UseSkill{.player = PlayerId{.value = 10}, .skill_id = snf::server::SLASH, .request_sequence = 1}, at(1000));

        assert(left.digest && event_at<ParticipantLeft>(*left.digest, 0).player == PlayerId{.value = 20});
        assert(left.audience == std::vector<PlayerId>{PlayerId{.value = 10}});
        assert(cleared.audience == std::vector<PlayerId>{PlayerId{.value = 10}});
        assert(cleared.grants == (std::vector<snf::server::StreetExperienceGrant>{{.player = PlayerId{.value = 10}, .experience = 300}}));
    }

    void test_deadline_failure_has_an_explicit_reason_and_flushes_once()
    {
        RoomConfig config = boss_room();
        config.battle_duration = 3000ms;
        config.boss_spawn_after = 2000ms;
        Room room = started(config);
        const auto allowed = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = snf::server::SLASH, .request_sequence = 1}, at(2999));

        const auto expired =
            room.handle(SetMoveIntent{.player = PlayerId{.value = 7}, .direction = MoveDirection::East, .request_sequence = 1}, at(3000));
        const auto deadline = room.handle(snf::server::BattleDeadline{}, at(3000));

        assert(allowed.status == RoomCommandStatus::Applied);
        assert(expired.status == RoomCommandStatus::BattleExpired);
        assert(expired.outcome == BattleOutcome::Failed);
        assert(expired.failure_reason == BattleFailureReason::Deadline);
        assert(expired.digest && events_of<SkillWhiffed>(*expired.digest).size() == 1);
        assert(!expired.tick_after);
        assert(deadline.status == RoomCommandStatus::WrongPhase && !deadline.outcome);
    }

    void test_quiet_ticks_emit_no_digest_and_do_not_advance_sequence()
    {
        Room room = started(boss_room());

        const auto quiet = room.handle(RoomSimulationTick{}, at(100));
        const auto spawned = room.handle(RoomSimulationTick{}, at(1000));

        assert(!quiet.digest);
        assert(spawned.digest && spawned.digest->sequence == 2);
    }

    void test_each_participant_can_only_use_the_skill_snapshotted_at_join()
    {
        Room room{RoomId{.value = 1}, wave_room()};
        join(room, PlayerId{.value = 7}, 10, 100, SLASH);
        join(room, PlayerId{.value = 8}, 10, 100, ARCANE_BOLT);
        static_cast<void>(room.handle(StartBattle{}, at(0)));

        const auto unknown = room.handle(
            UseSkill{.player = PlayerId{.value = 7}, .skill_id = SkillId{.value = 999}, .request_sequence = 1}, at(0)
        );
        const auto slash_using_bolt = room.handle(
            UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0)
        );
        const auto bolt_using_slash = room.handle(
            UseSkill{.player = PlayerId{.value = 8}, .skill_id = SLASH, .request_sequence = 1}, at(0)
        );

        assert(unknown.status == RoomCommandStatus::UnknownSkill);
        assert(slash_using_bolt.status == RoomCommandStatus::SkillNotEquipped);
        assert(bolt_using_slash.status == RoomCommandStatus::SkillNotEquipped);
        assert(room.projectileCount() == 0);
        assert(room.enemyCount() == 2);

        // Reusing sequence 1 proves rejected requests did not consume sequence or cooldown.
        const auto bolt = room.handle(
            UseSkill{.player = PlayerId{.value = 8}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0)
        );
        assert(bolt.status == RoomCommandStatus::Applied);
        assert(room.projectileCount() == 1);
        const auto slash = room.handle(
            UseSkill{.player = PlayerId{.value = 7}, .skill_id = SLASH, .request_sequence = 1}, at(0)
        );
        assert(slash.status == RoomCommandStatus::Applied);
    }

    void test_arcane_bolt_target_selection()
    {
        // 1. Single nearest enemy
        {
            Room room = started(wave_room(), PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);
            // Move player north by 2: (10, 10) -> (10, 8). Minion 1 at (10, 5) -> (10, 7). Minion 2 at (10, 15) -> (10, 13).
            static_cast<void>(room.handle(SetMoveIntent{.player = PlayerId{.value = 7}, .direction = MoveDirection::North, .request_sequence = 1}, at(0)));
            static_cast<void>(room.handle(RoomSimulationTick{}, at(100)));
            assert(room.positionOf(PlayerId{.value = 7}) == (ArenaPosition{.x = 10, .y = 8}));

            const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 2}, at(100));
            assert(cast.status == RoomCommandStatus::Applied);
            assert(room.projectileCount() == 1);
            const auto projectile = room.projectileById(ProjectileId{.value = 1});
            assert(projectile && projectile->target == (EnemyId{.value = 1}));
        }

        // 2. Higher ID is closer
        {
            Room room = started(wave_room(), PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);
            // Move player south by 2: (10, 10) -> (10, 12). Minion 1 at (10, 5) -> (10, 7). Minion 2 at (10, 15) -> (10, 13).
            static_cast<void>(room.handle(SetMoveIntent{.player = PlayerId{.value = 7}, .direction = MoveDirection::South, .request_sequence = 1}, at(0)));
            static_cast<void>(room.handle(RoomSimulationTick{}, at(100)));
            assert(room.positionOf(PlayerId{.value = 7}) == (ArenaPosition{.x = 10, .y = 12}));

            const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 2}, at(100));
            assert(cast.status == RoomCommandStatus::Applied);
            assert(room.projectileCount() == 1);
            const auto projectile = room.projectileById(ProjectileId{.value = 1});
            assert(projectile && projectile->target == (EnemyId{.value = 2}));
        }

        // 3. Tie-breaker selects smaller EnemyId when distances are equal
        {
            Room room = started(wave_room(), PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);
            // Player at (10, 10), Minion 1 at (10, 5) (dist 5), Minion 2 at (10, 15) (dist 5)
            const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0));
            assert(cast.status == RoomCommandStatus::Applied);
            assert(room.projectileCount() == 1);
            const auto projectile = room.projectileById(ProjectileId{.value = 1});
            assert(projectile && projectile->target == (EnemyId{.value = 1}));
        }

        // 4. Dead enemy is excluded even if closer
        {
            RoomConfig config = wave_room();
            config.arena_width = 40;
            config.arena_height = 40;
            config.minion_spawn_radius = 15;
            Room room = started(config, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);
            // Move north: player at (20, 18). Minion 1 at (20, 7) [dist 11], Minion 2 at (20, 33) [dist 15].
            static_cast<void>(room.handle(SetMoveIntent{.player = PlayerId{.value = 7}, .direction = MoveDirection::North, .request_sequence = 1}, at(0)));
            static_cast<void>(room.handle(RoomSimulationTick{}, at(100)));

            // Kill Minion 1 with the equipped projectile, then verify the next cast excludes it.
            const auto first_bolt = room.handle(
                UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 2}, at(100)
            );
            assert(first_bolt.status == RoomCommandStatus::Applied);
            static_cast<void>(room.handle(RoomSimulationTick{}, at(200)));
            static_cast<void>(room.handle(RoomSimulationTick{}, at(300)));
            static_cast<void>(room.handle(RoomSimulationTick{}, at(400)));
            assert(room.enemyCount() == 1);

            const auto bolt = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 3}, at(1600));
            assert(bolt.status == RoomCommandStatus::Applied);
            assert(room.projectileCount() == 1);
            const auto projectile = room.projectileById(ProjectileId{.value = 2});
            assert(projectile && projectile->target == (EnemyId{.value = 2}));
        }

        // 5. Out of acquisition range whiffs and consumes sequence & cooldown
        {
            RoomConfig config = wave_room();
            config.arena_width = 200;
            config.arena_height = 200;
            config.minion_spawn_radius = 50; // Distance is 50 > acquisition_range (40)
            Room room = started(config, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);

            const auto whiff = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0));
            assert(whiff.status == RoomCommandStatus::Applied);
            assert(room.projectileCount() == 0);

            const auto tick = room.handle(RoomSimulationTick{}, at(100));
            assert(tick.digest && events_of<SkillWhiffed>(*tick.digest).size() == 1);

            // Duplicate sequence is rejected
            const auto duplicate = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(100));
            assert(duplicate.status == RoomCommandStatus::DuplicateRequest);

            // Cooldown (1500ms) is active
            const auto early = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 2}, at(1499));
            assert(early.status == RoomCommandStatus::SkillOnCooldown);
        }
    }

    void test_arcane_bolt_spawns_projectile_snapshot_without_immediate_damage()
    {
        RoomConfig config = boss_room();
        config.digest_flush_threshold = 1;
        Room room = started(config, PlayerId{.value = 7}, 37, 100, ARCANE_BOLT);
        static_cast<void>(room.handle(RoomSimulationTick{}, at(1000)));
        const std::uint64_t health_before_cast = room.bossHealth();

        const auto first_cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(1000));
        assert(first_cast.status == RoomCommandStatus::Applied);
        assert(room.projectileCount() == 1);
        assert(room.bossHealth() == health_before_cast);
        assert(
            first_cast.digest &&
            event_at<ProjectileSpawned>(*first_cast.digest, 0) ==
                (ProjectileSpawned{
                    .projectile = ProjectileId{.value = 1},
                    .owner = PlayerId{.value = 7},
                    .skill_id = ARCANE_BOLT,
                    .target = EnemyId{.value = 1},
                    .position = ArenaPosition{.x = 10, .y = 10},
                })
        );

        const auto projectile1 = room.projectileById(ProjectileId{.value = 1});
        assert(projectile1.has_value());
        assert(projectile1->id == (ProjectileId{.value = 1}));
        assert(projectile1->owner == (PlayerId{.value = 7}));
        assert(projectile1->skill_id == ARCANE_BOLT);
        assert(projectile1->target == (EnemyId{.value = 1}));
        assert(projectile1->position == (ArenaPosition{.x = 10, .y = 10}));
        assert(projectile1->speed_per_tick == 4);
        assert(projectile1->hit_range == 1);
        assert(projectile1->damage == 59); // 37 * 160% = 59
        assert(projectile1->expires_at == at(4000)); // 1000ms + 3000ms lifetime

        // Non-existent projectile ID query returns nullopt
        assert(!room.projectileById(ProjectileId{.value = 999}));

        // Second cast after cooldown (1500ms) creates projectile 2 with incremental ID
        const auto second_cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 2}, at(2500));
        assert(second_cast.status == RoomCommandStatus::Applied);
        assert(room.projectileCount() == 2);
        assert(room.bossHealth() == health_before_cast);
        assert(second_cast.digest && event_at<ProjectileSpawned>(*second_cast.digest, 0).projectile == (ProjectileId{.value = 2}));

        const auto projectile2 = room.projectileById(ProjectileId{.value = 2});
        assert(projectile2.has_value());
        assert(projectile2->id == (ProjectileId{.value = 2}));
        assert(projectile2->owner == (PlayerId{.value = 7}));
        assert(projectile2->skill_id == ARCANE_BOLT);
        assert(projectile2->target == (EnemyId{.value = 1}));
        assert(projectile2->position == (ArenaPosition{.x = 10, .y = 10}));
        assert(projectile2->speed_per_tick == 4);
        assert(projectile2->hit_range == 1);
        assert(projectile2->damage == 59);
        assert(projectile2->expires_at == at(5500)); // 2500ms + 3000ms lifetime
    }

    void test_arcane_bolt_request_semantics_and_cooldown()
    {
        Room room = started(wave_room(), PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);

        const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0));
        assert(cast.status == RoomCommandStatus::Applied);
        assert(room.projectileCount() == 1);

        // Request before 1500ms cooldown returns SkillOnCooldown
        const auto early = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 2}, at(1499));
        assert(early.status == RoomCommandStatus::SkillOnCooldown);
        assert(room.projectileCount() == 1);

        // Duplicate sequence does not spawn a second projectile
        const auto duplicate = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(1500));
        assert(duplicate.status == RoomCommandStatus::DuplicateRequest);
        assert(room.projectileCount() == 1);

        // Request at/after 1500ms succeeds
        const auto ready = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 2}, at(1500));
        assert(ready.status == RoomCommandStatus::Applied);
        assert(room.projectileCount() == 2);
    }

    void test_projectile_tracks_the_current_target_position_once_per_tick_and_hits_after_moving()
    {
        RoomConfig config = single_minion_room();
        config.arena_width = 40;
        config.arena_height = 40;
        config.minion_spawn_radius = 15;
        Room room = started(config, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);

        static_cast<void>(
            room.handle(SetMoveIntent{.player = PlayerId{.value = 7}, .direction = MoveDirection::East, .request_sequence = 1}, at(0))
        );
        const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0));
        const auto first = room.handle(RoomSimulationTick{}, at(100));
        const auto projectile_after_first = room.projectileById(ProjectileId{.value = 1});
        const auto second = room.handle(RoomSimulationTick{}, at(200));
        const auto projectile_after_second = room.projectileById(ProjectileId{.value = 1});
        const auto hit = room.handle(RoomSimulationTick{}, at(300));
        const auto after_hit = room.handle(RoomSimulationTick{}, at(400));

        assert(cast.status == RoomCommandStatus::Applied);
        assert(projectile_after_first && projectile_after_first->position == (ArenaPosition{.x = 20, .y = 16}));
        assert(first.digest && first.digest->events.size() == 4);
        assert(event_at<ProjectileSpawned>(*first.digest, 0).projectile == (ProjectileId{.value = 1}));
        assert(event_at<ProjectileMoved>(*first.digest, 2).position == (ArenaPosition{.x = 20, .y = 16}));
        assert(first.digest && events_of<EnemyPositioned>(*first.digest).front()->position == (ArenaPosition{.x = 22, .y = 7}));
        assert(projectile_after_second && projectile_after_second->position == (ArenaPosition{.x = 22, .y = 12}));
        assert(second.digest && event_at<ProjectileMoved>(*second.digest, 1).position == (ArenaPosition{.x = 22, .y = 12}));
        assert(second.digest && events_of<EnemyPositioned>(*second.digest).front()->position == (ArenaPosition{.x = 24, .y = 9}));
        assert(room.projectileCount() == 0);
        assert(hit.digest && hit.digest->events.size() == 5);
        static_cast<void>(event_at<ProjectileMoved>(*hit.digest, 1));
        static_cast<void>(event_at<EnemyDamaged>(*hit.digest, 2));
        static_cast<void>(event_at<EnemyDied>(*hit.digest, 3));
        assert(
            event_at<ProjectileRemoved>(*hit.digest, 4) ==
            (ProjectileRemoved{.projectile = ProjectileId{.value = 1}, .reason = ProjectileRemovalReason::Hit})
        );
        assert(!after_hit.digest || events_of<EnemyDamaged>(*after_hit.digest).empty());
    }

    void test_projectile_hits_before_moving_and_is_not_applied_again()
    {
        RoomConfig config = single_minion_room();
        config.minion_spawn_radius = 1;
        config.minion_health = 30;
        Room room = started(config, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);

        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0)));
        const auto hit = room.handle(RoomSimulationTick{}, at(100));
        const auto next = room.handle(RoomSimulationTick{}, at(200));

        assert(room.projectileCount() == 0);
        assert(hit.digest && hit.digest->events.size() == 3);
        static_cast<void>(event_at<ProjectileSpawned>(*hit.digest, 0));
        static_cast<void>(event_at<EnemyDamaged>(*hit.digest, 1));
        assert(events_of<ProjectileMoved>(*hit.digest).empty());
        assert(events_of<EnemyDamaged>(*hit.digest).front()->health == 14);
        assert(event_at<ProjectileRemoved>(*hit.digest, 2).reason == ProjectileRemovalReason::Hit);
        assert(!next.digest || events_of<EnemyDamaged>(*next.digest).empty());
    }

    void test_projectile_drops_a_lost_target_without_retargeting()
    {
        RoomConfig config = wave_room();
        config.wave_count = 1;
        config.max_spawned_enemies = 3;
        config.arena_width = 40;
        config.arena_height = 40;
        config.minion_spawn_radius = 15;
        Room room{RoomId{.value = 1}, config};
        join(room, PlayerId{.value = 7}, 10, 100, SLASH);
        join(room, PlayerId{.value = 8}, 10, 100, ARCANE_BOLT);
        static_cast<void>(room.handle(StartBattle{}, at(0)));

        static_cast<void>(
            room.handle(SetMoveIntent{.player = PlayerId{.value = 7}, .direction = MoveDirection::North, .request_sequence = 1}, at(0))
        );
        static_cast<void>(
            room.handle(SetMoveIntent{.player = PlayerId{.value = 8}, .direction = MoveDirection::North, .request_sequence = 1}, at(0))
        );
        static_cast<void>(room.handle(RoomSimulationTick{}, at(100)));
        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 8}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(100)));
        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = SLASH, .request_sequence = 2}, at(100)));
        const auto lost = room.handle(RoomSimulationTick{}, at(200));

        assert(room.enemyCount() == 1);
        assert(room.projectileCount() == 0);
        assert(lost.digest);
        const auto damage = events_of<EnemyDamaged>(*lost.digest);
        assert(damage.size() == 1 && damage.front()->skill_id == SLASH);
        assert(events_of<ProjectileMoved>(*lost.digest).empty());
        const auto removed = events_of<ProjectileRemoved>(*lost.digest);
        assert(removed.size() == 1 && removed.front()->reason == ProjectileRemovalReason::TargetLost);
    }

    void test_projectile_lifetime_boundary_expires_before_movement_or_damage()
    {
        RoomConfig config = single_minion_room();
        config.arena_width = 200;
        config.arena_height = 200;
        config.minion_spawn_radius = 40;
        Room room = started(config, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);

        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0)));
        const auto before_expiry = room.handle(RoomSimulationTick{}, at(2999));
        const auto projectile = room.projectileById(ProjectileId{.value = 1});
        const auto expired = room.handle(RoomSimulationTick{}, at(3000));

        assert(before_expiry.status == RoomCommandStatus::Applied);
        assert(projectile && projectile->position == (ArenaPosition{.x = 100, .y = 96}));
        assert(room.projectileCount() == 0);
        assert(expired.digest && events_of<EnemyDamaged>(*expired.digest).empty());
        const auto removed = events_of<ProjectileRemoved>(*expired.digest);
        assert(removed.size() == 1 && removed.front()->reason == ProjectileRemovalReason::Expired);
        assert(events_of<ProjectileMoved>(*expired.digest).empty());
    }

    void test_projectiles_resolve_in_id_order_and_later_shots_drop_the_dead_target()
    {
        RoomConfig config = single_minion_room();
        Room room{RoomId{.value = 1}, config};
        join(room, PlayerId{.value = 20}, 10, 100, ARCANE_BOLT);
        join(room, PlayerId{.value = 10}, 10, 100, ARCANE_BOLT);
        static_cast<void>(room.handle(StartBattle{}, at(0)));

        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 20}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0)));
        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 10}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0)));
        const auto tick = room.handle(RoomSimulationTick{}, at(100));

        assert(room.projectileCount() == 0);
        assert(tick.digest);
        const auto damage = events_of<EnemyDamaged>(*tick.digest);
        assert(damage.size() == 1 && damage.front()->actor == (PlayerId{.value = 20}));
        assert(damage.front()->amount == 10 && damage.front()->health == 0);
        assert(events_of<EnemyDied>(*tick.digest).size() == 1);
        const auto removed = events_of<ProjectileRemoved>(*tick.digest);
        assert(removed.size() == 2);
        assert(removed[0]->projectile == (ProjectileId{.value = 1}) && removed[0]->reason == ProjectileRemovalReason::Hit);
        assert(removed[1]->projectile == (ProjectileId{.value = 2}) && removed[1]->reason == ProjectileRemovalReason::TargetLost);
    }

    void test_projectile_boss_kill_clears_before_enemy_actions_and_discards_remaining_shots()
    {
        RoomConfig config = boss_room();
        config.boss_health = 16;
        config.boss_attack_range = 10;
        config.boss_attack_cooldown = 100ms;
        Room room{RoomId{.value = 1}, config};
        join(room, PlayerId{.value = 20}, 10, 100, ARCANE_BOLT);
        join(room, PlayerId{.value = 10}, 10, 100, ARCANE_BOLT);
        static_cast<void>(room.handle(StartBattle{}, at(0)));
        static_cast<void>(room.handle(RoomSimulationTick{}, at(1000)));

        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 20}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(1000)));
        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 10}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(1000)));
        const auto attacked = room.handle(RoomSimulationTick{}, at(1100));
        const auto health_before_clear = room.healthOf(PlayerId{.value = 10});
        const auto cleared = room.handle(RoomSimulationTick{}, at(1200));
        const auto stale = room.handle(RoomSimulationTick{}, at(1300));

        assert(attacked.digest && events_of<ParticipantDamaged>(*attacked.digest).size() == 1);
        assert(health_before_clear == 90);
        assert(room.healthOf(PlayerId{.value = 10}) == health_before_clear);
        assert(cleared.phase == RoomPhase::Cleared && cleared.outcome == BattleOutcome::Cleared);
        assert(cleared.digest && events_of<EnemyDamaged>(*cleared.digest).size() == 1);
        assert(events_of<EnemyDied>(*cleared.digest).size() == 1);
        assert(events_of<ParticipantDamaged>(*cleared.digest).empty());
        const auto removed = events_of<ProjectileRemoved>(*cleared.digest);
        assert(removed.size() == 1 && removed.front()->reason == ProjectileRemovalReason::Hit);
        assert(cleared.grants.size() == 2);
        assert(room.projectileCount() == 0);
        assert(stale.status == RoomCommandStatus::WrongPhase && !stale.outcome && stale.grants.empty());
    }

    void test_projectile_survives_its_caster_leaving_while_the_room_keeps_running()
    {
        RoomConfig config = single_minion_room();
        Room room{RoomId{.value = 1}, config};
        join(room, PlayerId{.value = 10}, 10, 100, ARCANE_BOLT);
        join(room, PlayerId{.value = 20});
        static_cast<void>(room.handle(StartBattle{}, at(0)));

        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 10}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0)));
        const auto leave = room.handle(snf::server::LeaveRoom{.player = PlayerId{.value = 10}}, at(0));
        const auto hit = room.handle(RoomSimulationTick{}, at(100));

        assert(leave.status == RoomCommandStatus::Applied && room.participantCount() == 1);
        assert(leave.digest && leave.digest->events.size() == 2);
        static_cast<void>(event_at<ProjectileSpawned>(*leave.digest, 0));
        static_cast<void>(event_at<ParticipantLeft>(*leave.digest, 1));
        assert(room.projectileCount() == 0);
        assert(hit.digest);
        const auto damage = events_of<EnemyDamaged>(*hit.digest);
        assert(damage.size() == 1 && damage.front()->actor == (PlayerId{.value = 10}));
        assert(events_of<ProjectileRemoved>(*hit.digest).front()->reason == ProjectileRemovalReason::Hit);
        assert(hit.audience == std::vector<PlayerId>{PlayerId{.value = 20}});
    }

    void test_projectile_removal_restores_capacity_without_reusing_ids()
    {
        RoomConfig config = single_minion_room();
        config.max_active_projectiles = 1;
        config.arena_width = 200;
        config.arena_height = 200;
        config.minion_spawn_radius = 40;
        Room room = started(config, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);

        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0)));
        const auto expired = room.handle(RoomSimulationTick{}, at(3000));
        const auto next = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 2}, at(3000));

        assert(expired.digest && events_of<ProjectileRemoved>(*expired.digest).front()->reason == ProjectileRemovalReason::Expired);
        assert(next.status == RoomCommandStatus::Applied);
        assert(room.projectileCount() == 1);
        const auto projectile = room.projectileById(ProjectileId{.value = 2});
        assert(projectile && projectile->id == (ProjectileId{.value = 2}));
    }

    void test_projectile_capacity_saturation_and_atomicity()
    {
        RoomConfig config = wave_room();
        config.max_active_projectiles = 2;
        Room room = started(config, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);

        const auto cast1 = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0));
        assert(cast1.status == RoomCommandStatus::Applied);
        assert(room.projectileCount() == 1);

        const auto cast2 = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 2}, at(1500));
        assert(cast2.status == RoomCommandStatus::Applied);
        assert(room.projectileCount() == 2);

        // 3rd cast reaches capacity: rejected with ProjectileCapacityExceeded
        const auto cast3 = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 3}, at(3000));
        assert(cast3.status == RoomCommandStatus::ProjectileCapacityExceeded);
        assert(room.projectileCount() == 2);

        // Repeated request with sequence 3 is still ProjectileCapacityExceeded
        const auto retry3 = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 3}, at(3000));
        assert(retry3.status == RoomCommandStatus::ProjectileCapacityExceeded);

        // Atomicity: sequence 3 and cooldown were not consumed. Once capacity is freed,
        // the same equipped skill can reuse sequence 3 at the same timestamp.
        static_cast<void>(room.handle(RoomSimulationTick{}, at(3000)));
        const auto accepted = room.handle(
            UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 3}, at(3000)
        );
        assert(accepted.status == RoomCommandStatus::Applied);
    }

    void test_projectile_lifecycle_cleanup()
    {
        // 1. A Slash-equipped participant clearing the room removes another participant's projectile.
        {
            RoomConfig config = boss_room();
            config.boss_health = 10;
            Room room{RoomId{.value = 1}, config};
            join(room, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);
            join(room, PlayerId{.value = 8}, 10, 100, SLASH);
            static_cast<void>(room.handle(StartBattle{}, at(0)));
            // Spawn boss at 1000ms
            static_cast<void>(room.handle(RoomSimulationTick{}, at(1000)));
            assert(room.bossSpawned());

            // Cast ArcaneBolt (creates 1 projectile)
            const auto bolt = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(1000));
            assert(bolt.status == RoomCommandStatus::Applied);
            assert(room.projectileCount() == 1);

            // Cast Slash -> kills boss -> room Cleared
            const auto slash = room.handle(UseSkill{.player = PlayerId{.value = 8}, .skill_id = SLASH, .request_sequence = 1}, at(1000));
            assert(slash.status == RoomCommandStatus::Applied);
            assert(room.phase() == RoomPhase::Cleared);
            assert(room.projectileCount() == 0);
        }

        // 2. Battle failed clears projectiles
        {
            RoomConfig config = wave_room();
            Room room = started(config, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);

            const auto bolt = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0));
            assert(bolt.status == RoomCommandStatus::Applied);
            assert(room.projectileCount() == 1);

            // Deadline failure
            const auto tick = room.handle(RoomSimulationTick{}, at(5000));
            assert(room.phase() == RoomPhase::Failed);
            assert(room.projectileCount() == 0);
        }

        // 3. Last participant leaving clears projectiles
        {
            RoomConfig config = wave_room();
            Room room = started(config, PlayerId{.value = 7}, 10, 100, ARCANE_BOLT);

            const auto bolt = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill_id = ARCANE_BOLT, .request_sequence = 1}, at(0));
            assert(bolt.status == RoomCommandStatus::Applied);
            assert(room.projectileCount() == 1);

            const auto leave = room.handle(snf::server::LeaveRoom{.player = PlayerId{.value = 7}}, at(0));
            assert(leave.status == RoomCommandStatus::Applied);
            assert(room.participantCount() == 0);
            assert(room.projectileCount() == 0);
        }
    }
}

void run_room_tests()
{
    test_room_config_rejects_invalid_simulation_and_arena_bounds();
    test_joining_is_bounded_and_keeps_snapshot_separate_from_current_state();
    test_start_publishes_arena_participants_and_spawn_positions_in_order();
    test_movement_intent_persists_and_uses_an_independent_sequence();
    test_the_default_opening_cast_whiffs_and_consumes_cooldown_and_sequence();
    test_cast_hits_every_enemy_in_range_in_enemy_id_order();
    test_digest_threshold_keeps_damage_and_death_atomic();
    test_enemy_movement_and_attack_are_interleaved_and_cooldown_is_absolute();
    test_a_late_tick_applies_at_most_one_enemy_attack();
    test_enemies_retarget_after_death_and_fail_once_when_everyone_dies();
    test_dead_participants_cannot_act_but_still_observe_and_receive_clear_reward();
    test_late_tick_catches_up_spawns_but_new_enemies_wait_until_the_next_tick();
    test_boss_spawns_at_its_absolute_time_after_the_minions();
    test_leave_is_observable_and_forfeits_reward();
    test_deadline_failure_has_an_explicit_reason_and_flushes_once();
    test_quiet_ticks_emit_no_digest_and_do_not_advance_sequence();
    test_each_participant_can_only_use_the_skill_snapshotted_at_join();
    test_arcane_bolt_target_selection();
    test_arcane_bolt_spawns_projectile_snapshot_without_immediate_damage();
    test_arcane_bolt_request_semantics_and_cooldown();
    test_projectile_tracks_the_current_target_position_once_per_tick_and_hits_after_moving();
    test_projectile_hits_before_moving_and_is_not_applied_again();
    test_projectile_drops_a_lost_target_without_retargeting();
    test_projectile_lifetime_boundary_expires_before_movement_or_damage();
    test_projectiles_resolve_in_id_order_and_later_shots_drop_the_dead_target();
    test_projectile_boss_kill_clears_before_enemy_actions_and_discards_remaining_shots();
    test_projectile_survives_its_caster_leaving_while_the_room_keeps_running();
    test_projectile_removal_restores_capacity_without_reusing_ids();
    test_projectile_capacity_saturation_and_atomicity();
    test_projectile_lifecycle_cleanup();
}
