#include "snf/game/room.hpp"

#include "snf/game/street_experience_grant.hpp"

#include <cassert>
#include <chrono>
#include <vector>

namespace
{
    using snf::server::BattleCompleted;
    using snf::server::JoinRoom;
    using snf::server::PlayerId;
    using snf::server::Room;
    using snf::server::RoomCommandStatus;
    using snf::server::RoomConfig;
    using snf::server::RoomId;
    using snf::server::RoomPhase;
    using snf::server::StartBattle;
    using snf::server::StreetExperienceGrant;

    [[nodiscard]] RoomConfig small_room()
    {
        return RoomConfig{
            .battle_duration = std::chrono::milliseconds{5000},
            .max_participants = 2,
            .clear_experience = 300,
        };
    }

    void test_a_room_starts_empty_and_waiting()
    {
        const Room actor{RoomId{.value = 1}, small_room()};

        assert(actor.id() == RoomId{.value = 1});
        assert(actor.phase() == RoomPhase::Waiting);
        assert(actor.participantCount() == 0);
    }

    void test_a_room_refuses_a_duplicate_join()
    {
        Room actor{RoomId{.value = 1}, small_room()};

        const auto first = actor.handle(JoinRoom{.player = PlayerId{.value = 7}});
        assert(first.status == RoomCommandStatus::Applied);
        assert(actor.participantCount() == 1);

        const auto again = actor.handle(JoinRoom{.player = PlayerId{.value = 7}});
        assert(again.status == RoomCommandStatus::AlreadyJoined);
        assert(again.player == PlayerId{.value = 7});
        assert(actor.participantCount() == 1);
    }

    void test_a_room_refuses_a_join_past_capacity()
    {
        Room actor{RoomId{.value = 1}, small_room()};

        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 1}}));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 2}}));
        const auto refused = actor.handle(JoinRoom{.player = PlayerId{.value = 3}});

        assert(refused.status == RoomCommandStatus::RoomFull);
        assert(actor.participantCount() == 2);
    }

    void test_starting_a_battle_arms_exactly_one_timer()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));

        const auto started = actor.handle(StartBattle{});
        assert(started.status == RoomCommandStatus::Applied);
        assert(actor.phase() == RoomPhase::Running);
        assert(started.complete_after == std::chrono::milliseconds{5000});
        assert(started.grants.empty());
    }

    void test_an_empty_room_cannot_start_a_battle()
    {
        Room actor{RoomId{.value = 1}, small_room()};

        const auto started = actor.handle(StartBattle{});
        // Otherwise the room arms a timer and then clears with nobody to reward.
        assert(started.status == RoomCommandStatus::WrongPhase);
        assert(!started.complete_after);
        assert(actor.phase() == RoomPhase::Waiting);
    }

    void test_a_second_start_is_refused()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));
        static_cast<void>(actor.handle(StartBattle{}));

        const auto again = actor.handle(StartBattle{});
        assert(again.status == RoomCommandStatus::WrongPhase);
        // A second timer would deliver a second completion to the same battle.
        assert(!again.complete_after);
    }

    void test_joining_is_refused_once_the_battle_is_running()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));
        static_cast<void>(actor.handle(StartBattle{}));

        const auto late = actor.handle(JoinRoom{.player = PlayerId{.value = 8}});
        assert(late.status == RoomCommandStatus::WrongPhase);
        assert(actor.participantCount() == 1);
    }

    void test_a_completion_before_the_battle_starts_is_refused()
    {
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));

        const auto completed = actor.handle(BattleCompleted{});
        assert(completed.status == RoomCommandStatus::WrongPhase);
        assert(completed.grants.empty());
        assert(actor.phase() == RoomPhase::Waiting);
    }

    void test_a_clear_rewards_every_participant_in_player_id_order()
    {
        Room actor{
            RoomId{.value = 1},
            RoomConfig{
                .battle_duration = std::chrono::milliseconds{5000},
                .max_participants = 3,
                .clear_experience = 300,
            }
        };
        // Deliberately out of order: the reward order must come from the identity,
        // not from who happened to join first.
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 30}}));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 10}}));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 20}}));
        static_cast<void>(actor.handle(StartBattle{}));

        const auto cleared = actor.handle(BattleCompleted{});
        assert(cleared.status == RoomCommandStatus::Applied);
        assert(actor.phase() == RoomPhase::Cleared);
        // The reward order is the only place the participant order is observable,
        // and it must follow the identity rather than who joined first.
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
        Room actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));
        static_cast<void>(actor.handle(StartBattle{}));

        const auto first = actor.handle(BattleCompleted{});
        assert(first.grants.size() == 1);

        // A duplicate completion -- a redelivered timer, or a mailbox that still held
        // one -- must not grant the reward a second time.
        const auto second = actor.handle(BattleCompleted{});
        assert(second.status == RoomCommandStatus::WrongPhase);
        assert(second.grants.empty());
        assert(actor.phase() == RoomPhase::Cleared);
    }
}

void run_room_tests()
{
    test_a_room_starts_empty_and_waiting();
    test_a_room_refuses_a_duplicate_join();
    test_a_room_refuses_a_join_past_capacity();
    test_starting_a_battle_arms_exactly_one_timer();
    test_an_empty_room_cannot_start_a_battle();
    test_a_second_start_is_refused();
    test_joining_is_refused_once_the_battle_is_running();
    test_a_completion_before_the_battle_starts_is_refused();
    test_a_clear_rewards_every_participant_in_player_id_order();
    test_a_clear_pays_out_only_once();
}
