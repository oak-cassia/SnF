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

        sessions.completePassivation(PLAYER);
        assert(!sessions.playerFor(FIRST_CONNECTION).has_value());
        assert(sessions.tryAttach(SECOND_CONNECTION, PLAYER) == snf::server::PlayerAttachResult::Attached);
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
    test_rolls_back_refused_attach_and_close_posts();
}
