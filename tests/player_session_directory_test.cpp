#include "snf/server/player_session_directory.hpp"

#include <cassert>

namespace
{
    constexpr snf::net::ConnectionId FIRST_CONNECTION{.descriptor = 10, .generation = 100};
    constexpr snf::net::ConnectionId SECOND_CONNECTION{.descriptor = 11, .generation = 101};
    constexpr snf::server::PlayerId PLAYER{.value = 77};

    void test_enforces_one_live_connection_per_player()
    {
        snf::server::PlayerSessionDirectory sessions;

        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);
        assert(!sessions.locationSnapshotFor(FIRST_CONNECTION).known);
        assert(sessions.playerFor(FIRST_CONNECTION) == PLAYER);
        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::AlreadyAttached);
        assert(sessions.tryAttach(SECOND_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::PlayerConflict);
        assert(sessions.tryAttach(FIRST_CONNECTION, snf::server::PlayerId{.value = 78}) == snf::server::PlayerAttachResult::ConnectionConflict);

        const snf::server::PlayerLocation location{
            .zone = snf::server::ZoneId{.value = 5},
            .position = {.x = -2, .y = 7},
        };
        sessions.noteLocation(FIRST_CONNECTION, location);
        assert(sessions.locationFor(FIRST_CONNECTION) == location);
        const auto snapshot = sessions.locationSnapshotFor(FIRST_CONNECTION);
        assert(snapshot.known);
        assert(snapshot.location == location);
        sessions.noteLocation(FIRST_CONNECTION, std::nullopt);
        const auto empty_snapshot = sessions.locationSnapshotFor(FIRST_CONNECTION);
        assert(empty_snapshot.known);
        assert(!empty_snapshot.location.has_value());
        assert(!sessions.locationFor(SECOND_CONNECTION).has_value());
    }

    void test_provisional_activity_makes_authentication_first_frame_only()
    {
        snf::server::PlayerSessionDirectory sessions;

        assert(sessions.noteProvisionalActivity(FIRST_CONNECTION));
        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::ProvisionalActivity);
        sessions.clearProvisionalActivity(FIRST_CONNECTION);
        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);
    }

    void test_reconnect_waits_for_actor_passivation()
    {
        snf::server::PlayerSessionDirectory sessions;
        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);

        assert(sessions.beginClose(FIRST_CONNECTION));
        assert(sessions.tryAttach(SECOND_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::PlayerConflict);

        sessions.completePassivation(PLAYER, FIRST_CONNECTION);
        assert(!sessions.playerFor(FIRST_CONNECTION).has_value());
        assert(sessions.tryAttach(SECOND_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);
    }

    void test_passivation_requires_the_exact_closing_connection()
    {
        snf::server::PlayerSessionDirectory sessions;
        constexpr snf::net::ConnectionId reused_descriptor{.descriptor = FIRST_CONNECTION.descriptor, .generation = FIRST_CONNECTION.generation + 1};

        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);
        sessions.completePassivation(PLAYER, FIRST_CONNECTION);
        assert(sessions.playerFor(FIRST_CONNECTION) == PLAYER);

        assert(sessions.beginClose(FIRST_CONNECTION));
        sessions.completePassivation(PLAYER, reused_descriptor);
        sessions.completePassivation(PLAYER, SECOND_CONNECTION);
        assert(sessions.playerFor(FIRST_CONNECTION) == PLAYER);

        sessions.completePassivation(PLAYER, FIRST_CONNECTION);
        assert(!sessions.playerFor(FIRST_CONNECTION).has_value());
    }

    void test_late_passivation_cannot_detach_a_new_closing_session()
    {
        snf::server::PlayerSessionDirectory sessions;

        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);
        sessions.abandon(FIRST_CONNECTION);
        assert(sessions.tryAttach(SECOND_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);
        assert(sessions.beginClose(SECOND_CONNECTION));

        sessions.completePassivation(PLAYER, FIRST_CONNECTION);
        assert(sessions.playerFor(SECOND_CONNECTION) == PLAYER);

        sessions.completePassivation(PLAYER, SECOND_CONNECTION);
        assert(!sessions.playerFor(SECOND_CONNECTION).has_value());
    }

    void test_rolls_back_refused_attach_and_close_posts()
    {
        snf::server::PlayerSessionDirectory sessions;
        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);
        sessions.rollbackAttach(FIRST_CONNECTION, PLAYER);
        assert(!sessions.playerFor(FIRST_CONNECTION).has_value());

        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);
        assert(sessions.beginClose(FIRST_CONNECTION));
        sessions.rollbackClose(FIRST_CONNECTION);
        assert(sessions.tryAttach(FIRST_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::AlreadyAttached);
    }
}

void run_player_session_directory_tests()
{
    test_enforces_one_live_connection_per_player();
    test_provisional_activity_makes_authentication_first_frame_only();
    test_reconnect_waits_for_actor_passivation();
    test_passivation_requires_the_exact_closing_connection();
    test_late_passivation_cannot_detach_a_new_closing_session();
    test_rolls_back_refused_attach_and_close_posts();
}
