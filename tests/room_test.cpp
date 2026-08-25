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
    using snf::server::ArenaPosition;
    using snf::server::ArenaStarted;
    using snf::server::BattleFailureReason;
    using snf::server::BattleOutcome;
    using snf::server::EnemyDamaged;
    using snf::server::EnemyDied;
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
    using snf::server::Room;
    using snf::server::RoomCommandStatus;
    using snf::server::RoomConfig;
    using snf::server::RoomId;
    using snf::server::RoomPhase;
    using snf::server::RoomSimulationTick;
    using snf::server::SetMoveIntent;
    using snf::server::SkillWhiffed;
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

    void join(Room& room, const PlayerId player, const std::uint64_t attack = 10, const std::uint64_t health = 100)
    {
        const auto result = room.handle(JoinRoom{.player = player, .stats = {.attack = attack, .health = health}}, at(0));
        assert(result.status == RoomCommandStatus::Applied);
    }

    [[nodiscard]] Room
    started(const RoomConfig& config, const PlayerId player = PlayerId{.value = 7}, const std::uint64_t attack = 10, const std::uint64_t health = 100)
    {
        Room room{RoomId{.value = 1}, config};
        join(room, player, attack, health);
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

        RoomConfig invalid = wave_room();
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
        const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(0));
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

        const auto whiff = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(0));
        const auto early = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 2}, at(999));
        const auto duplicate = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(1000));
        const auto tick = room.handle(RoomSimulationTick{}, at(100));

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
            room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(0));

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

        const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(0));

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

        const auto dead_cast = room.handle(UseSkill{.player = PlayerId{.value = 10}, .skill = snf::server::SLASH, .request_sequence = 1}, at(100));
        const auto dead_move =
            room.handle(SetMoveIntent{.player = PlayerId{.value = 10}, .direction = MoveDirection::East, .request_sequence = 1}, at(100));
        const auto cleared = room.handle(UseSkill{.player = PlayerId{.value = 20}, .skill = snf::server::SLASH, .request_sequence = 1}, at(100));
        const auto stale = room.handle(UseSkill{.player = PlayerId{.value = 20}, .skill = snf::server::SLASH, .request_sequence = 2}, at(1100));

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
        const auto cleared = room.handle(UseSkill{.player = PlayerId{.value = 10}, .skill = snf::server::SLASH, .request_sequence = 1}, at(1000));

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
        const auto allowed = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(2999));

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
}
