#include "snf/server/room_actor.hpp"

#include "snf/server/street_experience_grant.hpp"

#include <cassert>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace
{
    using snf::server::BattleCompleted;
    using snf::server::JoinRoom;
    using snf::server::PlayerId;
    using snf::server::RoomActor;
    using snf::server::RoomActorConfig;
    using snf::server::RoomCommandStatus;
    using snf::server::RoomId;
    using snf::server::RoomPhase;
    using snf::server::RoomResult;
    using snf::server::ScheduleTimer;
    using snf::server::StartBattle;
    using snf::server::StreetExperienceGrant;
    using snf::server::TellActor;

    [[nodiscard]] RoomActorConfig small_room()
    {
        return RoomActorConfig{
            .battle_duration = std::chrono::milliseconds{5000},
            .max_participants = 2,
            .clear_experience = 300,
        };
    }

    // The participant list is not exposed, so the reward order a clear emits is the
    // only place its ordering is observable -- which is the property that matters.
    [[nodiscard]] std::vector<std::uint64_t> reward_targets(RoomResult& result)
    {
        std::vector<std::uint64_t> targets;
        for (auto& action : result.follow_ups)
        {
            if (auto* tell = std::get_if<TellActor>(&action))
            {
                assert(tell->target.kind == snf::runtime::ActorKind::Player);
                targets.push_back(tell->target.entity);
            }
        }
        return targets;
    }

    void test_a_room_starts_empty_and_waiting()
    {
        const RoomActor actor{RoomId{.value = 1}, small_room()};

        assert(actor.id() == RoomId{.value = 1});
        assert(actor.phase() == RoomPhase::Waiting);
        assert(actor.participantCount() == 0);
    }

    void test_a_room_refuses_a_duplicate_join()
    {
        RoomActor actor{RoomId{.value = 1}, small_room()};

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
        RoomActor actor{RoomId{.value = 1}, small_room()};

        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 1}}));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 2}}));
        const auto refused = actor.handle(JoinRoom{.player = PlayerId{.value = 3}});

        assert(refused.status == RoomCommandStatus::RoomFull);
        assert(actor.participantCount() == 2);
    }

    void test_starting_a_battle_arms_exactly_one_timer()
    {
        RoomActor actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));

        auto started = actor.handle(StartBattle{});
        assert(started.status == RoomCommandStatus::Applied);
        assert(actor.phase() == RoomPhase::Running);
        assert(started.follow_ups.size() == 1);

        const auto* timer = std::get_if<ScheduleTimer>(&started.follow_ups.front());
        assert(timer && timer->delay == std::chrono::milliseconds{5000});
    }

    void test_an_empty_room_cannot_start_a_battle()
    {
        RoomActor actor{RoomId{.value = 1}, small_room()};

        const auto started = actor.handle(StartBattle{});
        // Otherwise the room arms a timer and then clears with nobody to reward.
        assert(started.status == RoomCommandStatus::WrongPhase);
        assert(started.follow_ups.empty());
        assert(actor.phase() == RoomPhase::Waiting);
    }

    void test_a_second_start_is_refused()
    {
        RoomActor actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));
        static_cast<void>(actor.handle(StartBattle{}));

        const auto again = actor.handle(StartBattle{});
        assert(again.status == RoomCommandStatus::WrongPhase);
        // A second timer would deliver a second completion to the same battle.
        assert(again.follow_ups.empty());
    }

    void test_joining_is_refused_once_the_battle_is_running()
    {
        RoomActor actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));
        static_cast<void>(actor.handle(StartBattle{}));

        const auto late = actor.handle(JoinRoom{.player = PlayerId{.value = 8}});
        assert(late.status == RoomCommandStatus::WrongPhase);
        assert(actor.participantCount() == 1);
    }

    void test_a_completion_before_the_battle_starts_is_refused()
    {
        RoomActor actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));

        const auto completed = actor.handle(BattleCompleted{});
        assert(completed.status == RoomCommandStatus::WrongPhase);
        assert(completed.follow_ups.empty());
        assert(actor.phase() == RoomPhase::Waiting);
    }

    void test_a_clear_rewards_every_participant_in_player_id_order()
    {
        RoomActor actor{RoomId{.value = 1},
                        RoomActorConfig{
                            .battle_duration = std::chrono::milliseconds{5000},
                            .max_participants = 3,
                            .clear_experience = 300,
                        }};
        // Deliberately out of order: the reward order must come from the identity,
        // not from who happened to join first.
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 30}}));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 10}}));
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 20}}));
        static_cast<void>(actor.handle(StartBattle{}));

        auto cleared = actor.handle(BattleCompleted{});
        assert(cleared.status == RoomCommandStatus::Applied);
        assert(actor.phase() == RoomPhase::Cleared);
        assert(cleared.follow_ups.size() == 3);
        assert((reward_targets(cleared) == std::vector<std::uint64_t>{10, 20, 30}));

        auto* tell = std::get_if<TellActor>(&cleared.follow_ups.front());
        assert(tell != nullptr);
        const auto grant = tell->payload.take<StreetExperienceGrant>();
        assert(grant && grant->experience == 300);
    }

    void test_a_clear_pays_out_only_once()
    {
        RoomActor actor{RoomId{.value = 1}, small_room()};
        static_cast<void>(actor.handle(JoinRoom{.player = PlayerId{.value = 7}}));
        static_cast<void>(actor.handle(StartBattle{}));

        const auto first = actor.handle(BattleCompleted{});
        assert(first.follow_ups.size() == 1);

        // A duplicate completion -- a redelivered timer, or a mailbox that still held
        // one -- must not grant the reward a second time.
        const auto second = actor.handle(BattleCompleted{});
        assert(second.status == RoomCommandStatus::WrongPhase);
        assert(second.follow_ups.empty());
        assert(actor.phase() == RoomPhase::Cleared);
    }
}

void run_room_actor_tests()
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
