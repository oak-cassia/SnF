#include "snf/game/room.hpp"
#include "snf/game/skill_catalog.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <variant>
#include <vector>

namespace
{
    using namespace std::chrono_literals;
    using snf::server::BattleOutcome;
    using snf::server::EnemyDamaged;
    using snf::server::EnemyDied;
    using snf::server::EnemyKind;
    using snf::server::EnemySpawned;
    using snf::server::JoinRoom;
    using snf::server::PlayerId;
    using snf::server::Room;
    using snf::server::RoomCommandStatus;
    using snf::server::RoomConfig;
    using snf::server::RoomId;
    using snf::server::RoomPhase;
    using snf::server::RoomSimulationTick;
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

    void join(Room& room, const PlayerId player, const std::uint64_t attack = 10)
    {
        const auto result = room.handle(JoinRoom{.player = player, .stats = {.attack = attack, .health = 100}}, at(0));
        assert(result.status == RoomCommandStatus::Applied);
    }

    [[nodiscard]] Room started(const RoomConfig& config, const PlayerId player = PlayerId{.value = 7}, const std::uint64_t attack = 10)
    {
        Room room{RoomId{.value = 1}, config};
        join(room, player, attack);
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

    void test_room_config_rejects_invalid_simulation_bounds()
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
    }

    void test_joining_is_bounded_and_keeps_the_combat_snapshot()
    {
        RoomConfig config = wave_room();
        config.max_participants = 1;
        Room room{RoomId{.value = 1}, config};

        join(room, PlayerId{.value = 10}, 37);
        const auto refused = room.handle(JoinRoom{.player = PlayerId{.value = 20}}, at(0));

        assert(refused.status == RoomCommandStatus::RoomFull);
        assert(room.statsOf(PlayerId{.value = 10})->attack == 37);
        assert(room.participantCount() == 1);
    }

    void test_start_spawns_the_first_wave_and_arms_both_timers()
    {
        Room room{RoomId{.value = 1}, wave_room()};
        join(room, PlayerId{.value = 20});
        join(room, PlayerId{.value = 10});

        const auto result = room.handle(StartBattle{}, at(0));

        assert(result.phase == RoomPhase::Running);
        assert(result.deadline_after == 5000ms);
        assert(result.tick_after == 100ms);
        assert(result.digest && result.digest->sequence == 1);
        assert(result.digest->events.size() == 2);
        assert(event_at<EnemySpawned>(*result.digest, 0).id.value == 1);
        assert(event_at<EnemySpawned>(*result.digest, 1).id.value == 2);
        assert((result.audience == std::vector<PlayerId>{PlayerId{.value = 10}, PlayerId{.value = 20}}));
        assert(room.enemyCount() == 2);
        assert(!room.bossSpawned());
        assert(room.bossHealth() == 0);
    }

    void test_a_late_tick_catches_up_every_due_wave_in_order()
    {
        RoomConfig config = wave_room();
        config.wave_count = 3;
        config.minions_per_wave = 1;
        config.max_spawned_enemies = 4;
        config.boss_spawn_after = 4000ms;
        Room room = started(config);

        const auto tick = room.handle(RoomSimulationTick{}, at(2500));

        assert(tick.digest && tick.digest->sequence == 2);
        assert(tick.digest->events.size() == 2);
        assert(event_at<EnemySpawned>(*tick.digest, 0).id.value == 2);
        assert(event_at<EnemySpawned>(*tick.digest, 1).id.value == 3);
        assert(room.enemyCount() == 3);
        assert(tick.tick_after == 100ms);
    }

    void test_the_next_wave_uses_its_absolute_boundary()
    {
        Room room = started(wave_room());

        const auto before = room.handle(RoomSimulationTick{}, at(999));
        const auto at_boundary = room.handle(RoomSimulationTick{}, at(1000));

        assert(!before.digest);
        assert(at_boundary.digest && at_boundary.digest->events.size() == 2);
        assert(event_at<EnemySpawned>(*at_boundary.digest, 0).id.value == 3);
        assert(event_at<EnemySpawned>(*at_boundary.digest, 1).id.value == 4);
    }

    void test_the_boss_spawns_at_its_absolute_time_after_the_minions()
    {
        Room room = started(wave_room());

        const auto before = room.handle(RoomSimulationTick{}, at(2999));
        const auto spawned = room.handle(RoomSimulationTick{}, at(3000));

        assert(before.digest);
        assert(spawned.digest);
        const EnemySpawned& boss = event_at<EnemySpawned>(*spawned.digest, spawned.digest->events.size() - 1);
        assert(boss.kind == EnemyKind::Boss);
        assert(boss.id.value == 5);
        assert(room.bossSpawned());
        assert(room.bossHealth() == 50);
    }

    void test_casts_hit_the_first_living_enemy_even_before_cleanup()
    {
        Room room{RoomId{.value = 1}, wave_room()};
        join(room, PlayerId{.value = 10}, 10);
        join(room, PlayerId{.value = 20}, 10);
        static_cast<void>(room.handle(StartBattle{}, at(0)));

        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 10}, .skill = snf::server::SLASH, .request_sequence = 1}, at(0)));
        static_cast<void>(room.handle(UseSkill{.player = PlayerId{.value = 20}, .skill = snf::server::SLASH, .request_sequence = 1}, at(0)));
        const auto tick = room.handle(RoomSimulationTick{}, at(100));

        assert(tick.digest && tick.digest->events.size() == 4);
        assert(event_at<EnemyDamaged>(*tick.digest, 0).target.value == 1);
        assert(event_at<EnemyDied>(*tick.digest, 1).id.value == 1);
        assert(event_at<EnemyDamaged>(*tick.digest, 2).target.value == 2);
        assert(event_at<EnemyDied>(*tick.digest, 3).id.value == 2);
        assert(room.enemyCount() == 0);
        assert(room.phase() == RoomPhase::Running);
    }

    void test_a_whiff_is_applied_and_consumes_sequence_and_cooldown()
    {
        Room room = started(boss_room(), PlayerId{.value = 7}, 10);

        const auto whiff = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(0));
        const auto tick = room.handle(RoomSimulationTick{}, at(100));
        const auto early = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 2}, at(999));
        const auto duplicate = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(1000));

        assert(whiff.status == RoomCommandStatus::Applied && !whiff.digest);
        assert(early.status == RoomCommandStatus::SkillOnCooldown);
        assert(duplicate.status == RoomCommandStatus::DuplicateRequest);
        assert(tick.digest);
        const SkillWhiffed& event = event_at<SkillWhiffed>(*tick.digest, 0);
        assert(event.actor == PlayerId{.value = 7});
        assert(event.skill == snf::server::SLASH);
    }

    void test_digest_threshold_flushes_an_atomic_damage_and_death_pair()
    {
        RoomConfig config = wave_room();
        config.wave_count = 1;
        config.minions_per_wave = 1;
        config.max_spawned_enemies = 2;
        config.digest_flush_threshold = 2;
        Room room = started(config, PlayerId{.value = 7}, 10);

        const auto cast = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(0));

        assert(cast.digest && cast.digest->sequence == 2);
        assert(cast.digest->events.size() == 2);
        static_cast<void>(event_at<EnemyDamaged>(*cast.digest, 0));
        static_cast<void>(event_at<EnemyDied>(*cast.digest, 1));
        assert(cast.audience == std::vector<PlayerId>{PlayerId{.value = 7}});
    }

    void test_quiet_ticks_emit_no_digest_and_do_not_advance_its_sequence()
    {
        Room room = started(boss_room());

        const auto quiet = room.handle(RoomSimulationTick{}, at(100));
        const auto spawned = room.handle(RoomSimulationTick{}, at(1000));

        assert(!quiet.digest);
        assert(spawned.digest && spawned.digest->sequence == 1);
    }

    void test_only_killing_the_boss_clears_and_flushes_the_terminal_digest()
    {
        RoomConfig config = boss_room();
        config.boss_health = 20;
        Room room = started(config, PlayerId{.value = 7}, 20);
        const auto spawned = room.handle(RoomSimulationTick{}, at(1000));
        assert(spawned.digest && event_at<EnemySpawned>(*spawned.digest, 0).kind == EnemyKind::Boss);

        const auto cleared = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(1000));

        assert(cleared.phase == RoomPhase::Cleared);
        assert(cleared.outcome == BattleOutcome::Cleared);
        assert(cleared.digest && cleared.digest->sequence == 2);
        assert(cleared.boss_spawned && cleared.boss_health == 0);
        assert((cleared.grants == std::vector<snf::server::StreetExperienceGrant>{{.player = PlayerId{.value = 7}, .experience = 300}}));
        assert(!cleared.tick_after);
    }

    void test_a_cast_at_the_absolute_deadline_expires_the_battle_and_flushes()
    {
        RoomConfig config = boss_room();
        config.battle_duration = 3000ms;
        config.boss_spawn_after = 2000ms;
        Room room = started(config, PlayerId{.value = 7}, 10);
        const auto allowed = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 1}, at(2999));

        const auto expired = room.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = snf::server::SLASH, .request_sequence = 2}, at(3000));

        assert(allowed.status == RoomCommandStatus::Applied);
        assert(allowed.phase == RoomPhase::Running);
        assert(expired.status == RoomCommandStatus::BattleExpired);
        assert(expired.phase == RoomPhase::Failed);
        assert(expired.outcome == BattleOutcome::Failed);
        assert(expired.digest && event_at<SkillWhiffed>(*expired.digest, 0).actor == PlayerId{.value = 7});
        assert(expired.audience == std::vector<PlayerId>{PlayerId{.value = 7}});
        assert(expired.grants.empty());
    }

    void test_a_tick_at_the_deadline_fails_once_and_never_reschedules()
    {
        RoomConfig config = boss_room();
        config.battle_duration = 3000ms;
        config.boss_spawn_after = 2000ms;
        Room room = started(config);

        const auto failed = room.handle(RoomSimulationTick{}, at(3000));
        const auto deadline = room.handle(snf::server::BattleDeadline{}, at(3000));

        assert(failed.outcome == BattleOutcome::Failed);
        assert(!failed.tick_after);
        assert(deadline.status == RoomCommandStatus::WrongPhase);
        assert(!deadline.outcome);
    }

    void test_a_deadline_before_its_absolute_time_is_refused()
    {
        Room room = started(boss_room());

        const auto early = room.handle(snf::server::BattleDeadline{}, at(4999));

        assert(early.status == RoomCommandStatus::WrongPhase);
        assert(room.phase() == RoomPhase::Running);
    }

    void test_leaving_removes_the_participant_from_reward_and_audience()
    {
        RoomConfig config = boss_room();
        config.boss_health = 10;
        Room room{RoomId{.value = 1}, config};
        join(room, PlayerId{.value = 10}, 10);
        join(room, PlayerId{.value = 20}, 10);
        static_cast<void>(room.handle(StartBattle{}, at(0)));
        static_cast<void>(room.handle(snf::server::LeaveRoom{.player = PlayerId{.value = 20}}, at(0)));
        static_cast<void>(room.handle(RoomSimulationTick{}, at(1000)));

        const auto cleared = room.handle(UseSkill{.player = PlayerId{.value = 10}, .skill = snf::server::SLASH, .request_sequence = 1}, at(1000));

        assert(cleared.audience == std::vector<PlayerId>{PlayerId{.value = 10}});
        assert((cleared.grants == std::vector<snf::server::StreetExperienceGrant>{{.player = PlayerId{.value = 10}, .experience = 300}}));
    }
}

void run_room_tests()
{
    test_room_config_rejects_invalid_simulation_bounds();
    test_joining_is_bounded_and_keeps_the_combat_snapshot();
    test_start_spawns_the_first_wave_and_arms_both_timers();
    test_a_late_tick_catches_up_every_due_wave_in_order();
    test_the_next_wave_uses_its_absolute_boundary();
    test_the_boss_spawns_at_its_absolute_time_after_the_minions();
    test_casts_hit_the_first_living_enemy_even_before_cleanup();
    test_a_whiff_is_applied_and_consumes_sequence_and_cooldown();
    test_digest_threshold_flushes_an_atomic_damage_and_death_pair();
    test_quiet_ticks_emit_no_digest_and_do_not_advance_its_sequence();
    test_only_killing_the_boss_clears_and_flushes_the_terminal_digest();
    test_a_cast_at_the_absolute_deadline_expires_the_battle_and_flushes();
    test_a_tick_at_the_deadline_fails_once_and_never_reschedules();
    test_a_deadline_before_its_absolute_time_is_refused();
    test_leaving_removes_the_participant_from_reward_and_audience();
}
