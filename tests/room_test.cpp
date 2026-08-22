#include "snf/game/room.hpp"

#include "snf/game/street_experience_grant.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <vector>

namespace
{
    using snf::server::BattleDeadline;
    using snf::server::JoinRoom;
    using snf::server::LeaveRoom;
    using snf::server::PlayerId;
    using snf::server::Room;
    using snf::server::RoomCommandStatus;
    using snf::server::RoomConfig;
    using snf::server::RoomId;
    using snf::server::RoomPhase;
    using snf::server::SkillId;
    using snf::server::SLASH;
    using snf::server::StartBattle;
    using snf::server::StreetExperienceGrant;
    using snf::server::UseSkill;

    // A fixed origin, so every reading below reads as milliseconds into the battle
    // rather than as a clock value. The Room only ever compares these to each other.
    [[nodiscard]] std::chrono::steady_clock::time_point at(const std::int64_t millis)
    {
        return std::chrono::steady_clock::time_point{} + std::chrono::milliseconds{millis};
    }

    [[nodiscard]] RoomConfig small_room()
    {
        return RoomConfig{
            .battle_duration = std::chrono::milliseconds{5000},
            .max_participants = 2,
            .clear_experience = 300,
            .boss_health = 1000,
        };
    }

    // Two casts of 30 attack kill this boss, and the third would be one too many.
    [[nodiscard]] RoomConfig fragile_boss()
    {
        return RoomConfig{
            .battle_duration = std::chrono::milliseconds{5000},
            .max_participants = 3,
            .clear_experience = 300,
            .boss_health = 50,
        };
    }

    [[nodiscard]] Room started_battle(const RoomConfig config, const PlayerId player, const std::uint64_t attack)
    {
        Room actor{RoomId{.value = 1}, config};
        static_cast<void>(actor.handle(JoinRoom{.player = player, .stats = {.attack = attack, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(StartBattle{}, at(0)));
        return actor;
    }

    void test_a_room_starts_empty_and_waiting()
    {
        const Room actor{RoomId{.value = 1}, small_room()};

        assert(actor.id() == RoomId{.value = 1});
        assert(actor.phase() == RoomPhase::Waiting);
        assert(actor.participantCount() == 0);
        assert(actor.bossHealth() == 1000);
    }

    void test_a_room_refuses_a_duplicate_join()
    {
        Room actor{RoomId{.value = 1}, small_room()};

        const auto first = actor.handle(JoinRoom{.player = PlayerId{.value = 7}}, at(0));
        assert(first.status == RoomCommandStatus::Applied);
        assert(actor.participantCount() == 1);

        const auto again = actor.handle(JoinRoom{.player = PlayerId{.value = 7}}, at(0));
        assert(again.status == RoomCommandStatus::AlreadyJoined);
        assert(again.player == PlayerId{.value = 7});
        assert(actor.participantCount() == 1);
    }

    void test_a_room_refuses_a_join_past_capacity()
    {
        Room actor{RoomId{.value = 1}, small_room()};

        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 1}}, at(0)));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 2}}, at(0)));
        const auto refused = actor.handle(JoinRoom{.player = PlayerId{.value = 3}}, at(0));

        assert(refused.status == RoomCommandStatus::RoomFull);
        assert(actor.participantCount() == 2);
    }

    void test_starting_a_battle_arms_exactly_one_timer()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}, at(0)));

        const auto started = actor.handle(StartBattle{}, at(0));
        assert(started.status == RoomCommandStatus::Applied);
        assert(actor.phase() == RoomPhase::Running);
        assert(started.deadline_after == std::chrono::milliseconds{5000});
        assert(started.grants.empty());
    }

    void test_an_empty_room_cannot_start_a_battle()
    {
        Room actor{RoomId{.value = 1}, small_room()};

        const auto started = actor.handle(StartBattle{}, at(0));
        // Otherwise the room arms a timer and then decides a battle nobody fought.
        assert(started.status == RoomCommandStatus::WrongPhase);
        assert(!started.deadline_after);
        assert(actor.phase() == RoomPhase::Waiting);
    }

    void test_a_second_start_is_refused()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}, at(0)));
        static_cast<void>(actor.handle(StartBattle{}, at(0)));

        const auto again = actor.handle(StartBattle{}, at(0));
        assert(again.status == RoomCommandStatus::WrongPhase);
        // A second timer would fail a battle that the first one already decided.
        assert(!again.deadline_after);
    }

    void test_joining_is_refused_once_the_battle_is_running()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}, at(0)));
        static_cast<void>(actor.handle(StartBattle{}, at(0)));

        const auto late = actor.handle(JoinRoom{.player = PlayerId{.value = 8}}, at(0));
        assert(late.status == RoomCommandStatus::WrongPhase);
        assert(actor.participantCount() == 1);
    }

    void test_a_deadline_before_the_battle_starts_is_refused()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}, at(0)));

        const auto reached = actor.handle(BattleDeadline{}, at(5000));
        assert(reached.status == RoomCommandStatus::WrongPhase);
        assert(reached.audience.empty());
        assert(actor.phase() == RoomPhase::Waiting);
    }

    void test_a_cast_damages_the_boss_by_the_casters_attack()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);

        const auto cast = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0));

        assert(cast.status == RoomCommandStatus::Applied);
        assert(cast.skill);
        assert(cast.skill->actor == PlayerId{.value = 7});
        assert(cast.skill->skill == SLASH);
        // Slash is 100% of attack. The client sent neither number.
        assert(cast.skill->damage == 30);
        assert(cast.boss_health == 970);
        assert(actor.bossHealth() == 970);
        assert(actor.phase() == RoomPhase::Running);
    }

    void test_a_resent_sequence_does_not_damage_the_boss_twice()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0)));

        // Far past the cooldown, so the only thing that can refuse this is the
        // sequence. A resend has to be answered the same way however late it lands.
        const auto resent = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(4000));

        assert(resent.status == RoomCommandStatus::DuplicateRequest);
        assert(!resent.skill);
        assert(resent.boss_health == 970);
        assert(actor.bossHealth() == 970);
    }

    void test_a_sequence_below_the_high_water_mark_is_a_duplicate()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 5}, at(0)));

        // One number rejects every resend, not just the newest one.
        const auto older = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 4}, at(4000));

        assert(older.status == RoomCommandStatus::DuplicateRequest);
        assert(actor.bossHealth() == 970);
    }

    void test_a_cast_before_the_cooldown_expires_is_refused()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0)));

        // Slash is on cooldown for a second, and a new sequence does not shorten it.
        const auto early = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 2}, at(999));

        assert(early.status == RoomCommandStatus::SkillOnCooldown);
        assert(!early.skill);
        assert(actor.bossHealth() == 970);
    }

    void test_a_cast_once_the_cooldown_expires_lands()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0)));

        // The cooldown is a deadline the Room compares against, so it expires with
        // nothing having to visit the participant to decrement it.
        const auto second = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 2}, at(1000));

        assert(second.status == RoomCommandStatus::Applied);
        assert(second.skill->damage == 30);
        assert(actor.bossHealth() == 940);
    }

    void test_a_sequence_refused_for_cooldown_stays_usable()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0)));
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 2}, at(500)));

        // The high-water mark tracks casts that landed. A rejection must not consume
        // the number, or a client whose cast was refused could never retry it.
        const auto retried = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 2}, at(1000));

        assert(retried.status == RoomCommandStatus::Applied);
        assert(actor.bossHealth() == 940);
    }

    void test_an_unknown_skill_is_refused()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);

        const auto refused = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SkillId{.value = 99}, .request_sequence = 1}, at(0));

        // The catalogue is the server's, so an id it does not know is not a cast.
        assert(refused.status == RoomCommandStatus::UnknownSkill);
        assert(!refused.skill);
        assert(actor.bossHealth() == 1000);
    }

    void test_a_cast_outside_a_running_battle_is_refused()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}, .stats = {.attack = 30, .health = 100}}, at(0)));

        const auto waiting = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0));
        assert(waiting.status == RoomCommandStatus::WrongPhase);
        assert(actor.bossHealth() == 1000);
    }

    void test_a_cast_from_a_non_participant_is_refused()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);

        const auto refused = actor.handle(UseSkill{.player = PlayerId{.value = 8}, .skill = SLASH, .request_sequence = 1}, at(0));

        assert(refused.status == RoomCommandStatus::NotJoined);
        assert(actor.bossHealth() == 1000);
    }

    void test_every_participant_hears_about_a_cast()
    {
        Room actor{RoomId{.value = 1}, fragile_boss()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 30}, .stats = {.attack = 10, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 10}, .stats = {.attack = 10, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(StartBattle{}, at(0)));

        const auto cast = actor.handle(UseSkill{.player = PlayerId{.value = 30}, .skill = SLASH, .request_sequence = 1}, at(0));

        // The caster included: telling them apart from an observer is the sink's job,
        // and the Room would have to know about connections to do it here.
        assert((cast.audience == std::vector<PlayerId>{PlayerId{.value = 10}, PlayerId{.value = 30}}));
    }

    void test_a_refused_cast_tells_nobody()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0)));

        const auto refused = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0));

        // Nothing happened in the battle, so there is nothing to broadcast. Only the
        // caster is owed an answer.
        assert(refused.audience.empty());
    }

    void test_the_killing_blow_clears_in_the_same_result()
    {
        Room actor = started_battle(fragile_boss(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0)));

        const auto killing = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 2}, at(1000));

        // One result carries both, so no client can observe a boss at zero health in
        // a battle that is still running.
        assert(killing.status == RoomCommandStatus::Applied);
        assert(killing.phase == RoomPhase::Cleared);
        assert(killing.skill);
        // Clamped to what the boss had left rather than reporting overkill.
        assert(killing.skill->damage == 20);
        assert(killing.boss_health == 0);
        assert(actor.phase() == RoomPhase::Cleared);
        assert(
            (killing.grants ==
             std::vector<StreetExperienceGrant>{
                 {.player = PlayerId{.value = 7}, .experience = 300},
             })
        );
    }

    void test_a_clear_rewards_every_participant_in_player_id_order()
    {
        Room actor{RoomId{.value = 1}, fragile_boss()};
        // Deliberately out of order: the reward order must come from the identity,
        // not from who happened to join first.
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 30}, .stats = {.attack = 50, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 10}, .stats = {.attack = 50, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 20}, .stats = {.attack = 50, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(StartBattle{}, at(0)));

        const auto cleared = actor.handle(UseSkill{.player = PlayerId{.value = 20}, .skill = SLASH, .request_sequence = 1}, at(0));

        assert(cleared.phase == RoomPhase::Cleared);
        // The reward order is the only place the participant order is observable, and
        // it must follow the identity rather than who joined first or who landed the
        // killing blow.
        assert(
            (cleared.grants ==
             std::vector<StreetExperienceGrant>{
                 {.player = PlayerId{.value = 10}, .experience = 300},
                 {.player = PlayerId{.value = 20}, .experience = 300},
                 {.player = PlayerId{.value = 30}, .experience = 300},
             })
        );
    }

    void test_a_clear_pays_out_only_once()
    {
        Room actor = started_battle(fragile_boss(), PlayerId{.value = 7}, 50);
        const auto cleared = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0));
        assert(cleared.grants.size() == 1);

        // A cast that was still in the mailbox when the boss died, and the deadline
        // timer that was armed before it: neither may reward anyone again.
        const auto late_cast = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 2}, at(1000));
        assert(late_cast.status == RoomCommandStatus::WrongPhase);
        assert(late_cast.grants.empty());

        const auto deadline = actor.handle(BattleDeadline{}, at(5000));
        assert(deadline.status == RoomCommandStatus::WrongPhase);
        assert(deadline.grants.empty());
        assert(deadline.audience.empty());
        assert(actor.phase() == RoomPhase::Cleared);
    }

    void test_the_deadline_fails_a_battle_the_boss_survived()
    {
        Room actor{RoomId{.value = 1}, fragile_boss()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 10}, .stats = {.attack = 10, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 20}, .stats = {.attack = 10, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(StartBattle{}, at(0)));
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 10}, .skill = SLASH, .request_sequence = 1}, at(0)));

        const auto failed = actor.handle(BattleDeadline{}, at(5000));

        assert(failed.status == RoomCommandStatus::Applied);
        assert(failed.phase == RoomPhase::Failed);
        assert(actor.phase() == RoomPhase::Failed);
        // A failure pays nobody, and still has to send everybody home: the audience
        // is what the return reads, so it carries every participant.
        assert(failed.grants.empty());
        assert((failed.audience == std::vector<PlayerId>{PlayerId{.value = 10}, PlayerId{.value = 20}}));
        assert(failed.boss_health == 40);
    }

    void test_a_second_deadline_is_refused()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(BattleDeadline{}, at(5000)));

        // A redelivered timer, or one that was still in the mailbox.
        const auto again = actor.handle(BattleDeadline{}, at(5000));
        assert(again.status == RoomCommandStatus::WrongPhase);
        assert(again.audience.empty());
        assert(actor.phase() == RoomPhase::Failed);
    }

    void test_casting_into_a_failed_battle_is_refused()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(BattleDeadline{}, at(5000)));

        const auto refused = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(5001));
        assert(refused.status == RoomCommandStatus::WrongPhase);
        assert(actor.bossHealth() == 1000);
    }

    void test_a_room_keeps_the_stats_each_participant_joined_with()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        const PlayerId weak{.value = 10};
        const PlayerId strong{.value = 20};

        static_cast<void>(actor.handle(JoinRoom{.player = weak, .stats = {.attack = 10, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(JoinRoom{.player = strong, .stats = {.attack = 39, .health = 390}}, at(0)));

        // Fixed at join. The Room never asks the Player again, so what it stored is
        // what the battle runs on.
        assert((actor.statsOf(weak) == snf::server::CombatStats{.attack = 10, .health = 100}));
        assert((actor.statsOf(strong) == snf::server::CombatStats{.attack = 39, .health = 390}));
        assert(!actor.statsOf(PlayerId{.value = 30}));
    }

    void test_each_participant_casts_on_their_own_cooldown()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 10}, .stats = {.attack = 30, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 20}, .stats = {.attack = 40, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(StartBattle{}, at(0)));
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 10}, .skill = SLASH, .request_sequence = 1}, at(0)));

        // One caster's cooldown is not the party's, and neither is their sequence.
        const auto other = actor.handle(UseSkill{.player = PlayerId{.value = 20}, .skill = SLASH, .request_sequence = 1}, at(0));

        assert(other.status == RoomCommandStatus::Applied);
        assert(other.skill->damage == 40);
        assert(actor.bossHealth() == 930);
    }

    void test_leaving_before_the_battle_frees_the_seat()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 1}}, at(0)));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 2}}, at(0)));

        const auto left = actor.handle(LeaveRoom{.player = PlayerId{.value = 1}}, at(0));
        assert(left.status == RoomCommandStatus::Applied);
        assert(left.player == PlayerId{.value = 1});
        assert(actor.participantCount() == 1);
        assert(!actor.statsOf(PlayerId{.value = 1}));

        // The seat is genuinely free again, not merely vacated by a flag.
        const auto joined = actor.handle(JoinRoom{.player = PlayerId{.value = 3}}, at(0));
        assert(joined.status == RoomCommandStatus::Applied);
        assert(actor.participantCount() == 2);
    }

    void test_leaving_a_room_you_are_not_in_is_refused()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 1}}, at(0)));

        const auto refused = actor.handle(LeaveRoom{.player = PlayerId{.value = 2}}, at(0));
        assert(refused.status == RoomCommandStatus::NotJoined);
        assert(refused.phase == RoomPhase::Waiting);
        assert(actor.participantCount() == 1);
    }

    void test_leaving_mid_battle_forfeits_the_reward()
    {
        Room actor{RoomId{.value = 1}, fragile_boss()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 1}, .stats = {.attack = 50, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 2}, .stats = {.attack = 50, .health = 100}}, at(0)));
        static_cast<void>(actor.handle(StartBattle{}, at(0)));

        const auto left = actor.handle(LeaveRoom{.player = PlayerId{.value = 1}}, at(0));
        assert(left.status == RoomCommandStatus::Applied);
        assert(left.phase == RoomPhase::Running);

        // The reward path is never told that anyone left. It pays the participants the
        // Room still holds, which is what makes the removal the whole policy.
        const auto cleared = actor.handle(UseSkill{.player = PlayerId{.value = 2}, .skill = SLASH, .request_sequence = 1}, at(0));
        assert(
            (cleared.grants ==
             std::vector<StreetExperienceGrant>{
                 {.player = PlayerId{.value = 2}, .experience = 300},
             })
        );
    }

    void test_a_participant_who_left_cannot_cast()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 7}, 30);
        static_cast<void>(actor.handle(LeaveRoom{.player = PlayerId{.value = 7}}, at(0)));

        const auto refused = actor.handle(UseSkill{.player = PlayerId{.value = 7}, .skill = SLASH, .request_sequence = 1}, at(0));
        assert(refused.status == RoomCommandStatus::NotJoined);
        assert(actor.bossHealth() == 1000);
    }

    void test_the_last_participant_leaving_a_battle_fails_with_no_grants()
    {
        Room actor = started_battle(small_room(), PlayerId{.value = 1}, 30);
        static_cast<void>(actor.handle(LeaveRoom{.player = PlayerId{.value = 1}}, at(0)));

        // The timer was armed before the room emptied, so the deadline still arrives
        // if the Room is still resident. It has to reach a terminal phase rather than
        // stay Running with nobody in it.
        const auto failed = actor.handle(BattleDeadline{}, at(5000));
        assert(failed.status == RoomCommandStatus::Applied);
        assert(failed.phase == RoomPhase::Failed);
        assert(failed.grants.empty());
        assert(failed.audience.empty());
    }

    void test_leaving_a_decided_room_is_refused()
    {
        Room actor = started_battle(fragile_boss(), PlayerId{.value = 1}, 50);
        static_cast<void>(actor.handle(UseSkill{.player = PlayerId{.value = 1}, .skill = SLASH, .request_sequence = 1}, at(0)));

        // A leave racing the clear must not report that it took a seat back, or the
        // return path would be told to undo an entry that already ended.
        const auto refused = actor.handle(LeaveRoom{.player = PlayerId{.value = 1}}, at(0));
        assert(refused.status == RoomCommandStatus::WrongPhase);
        assert(refused.phase == RoomPhase::Cleared);
    }
}

void run_room_tests()
{
    test_a_room_keeps_the_stats_each_participant_joined_with();
    test_a_room_starts_empty_and_waiting();
    test_a_room_refuses_a_duplicate_join();
    test_a_room_refuses_a_join_past_capacity();
    test_starting_a_battle_arms_exactly_one_timer();
    test_an_empty_room_cannot_start_a_battle();
    test_a_second_start_is_refused();
    test_joining_is_refused_once_the_battle_is_running();
    test_a_deadline_before_the_battle_starts_is_refused();
    test_a_cast_damages_the_boss_by_the_casters_attack();
    test_a_resent_sequence_does_not_damage_the_boss_twice();
    test_a_sequence_below_the_high_water_mark_is_a_duplicate();
    test_a_cast_before_the_cooldown_expires_is_refused();
    test_a_cast_once_the_cooldown_expires_lands();
    test_a_sequence_refused_for_cooldown_stays_usable();
    test_an_unknown_skill_is_refused();
    test_a_cast_outside_a_running_battle_is_refused();
    test_a_cast_from_a_non_participant_is_refused();
    test_every_participant_hears_about_a_cast();
    test_a_refused_cast_tells_nobody();
    test_the_killing_blow_clears_in_the_same_result();
    test_a_clear_rewards_every_participant_in_player_id_order();
    test_a_clear_pays_out_only_once();
    test_the_deadline_fails_a_battle_the_boss_survived();
    test_a_second_deadline_is_refused();
    test_casting_into_a_failed_battle_is_refused();
    test_each_participant_casts_on_their_own_cooldown();
    test_leaving_before_the_battle_frees_the_seat();
    test_leaving_a_room_you_are_not_in_is_refused();
    test_leaving_mid_battle_forfeits_the_reward();
    test_a_participant_who_left_cannot_cast();
    test_the_last_participant_leaving_a_battle_fails_with_no_grants();
    test_leaving_a_decided_room_is_refused();
}
