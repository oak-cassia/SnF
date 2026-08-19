#include "snf/server/party_coordinator.hpp"

#include <cassert>

namespace
{
    void test_party_coordinator_bounds_membership_and_keeps_epochs_monotonic()
    {
        snf::server::PartyCoordinator parties{2};
        const snf::server::PartyId party{.value = 5};
        const snf::net::ConnectionId first_connection{.descriptor = 10, .generation = 1};
        const snf::net::ConnectionId second_connection{.descriptor = 11, .generation = 2};
        const snf::net::ConnectionId third_connection{.descriptor = 12, .generation = 3};
        const snf::server::PlayerId first{.value = 1};
        const snf::server::PlayerId second{.value = 2};

        const auto admitted = parties.tryJoin(first_connection, first, party);
        assert(admitted && admitted->created);
        assert(admitted->route.membership_epoch == 1);
        const auto duplicate = parties.tryJoin(first_connection, first, party);
        assert(duplicate && !duplicate->created);
        assert(!parties.tryJoin(second_connection, first, party).has_value());
        assert(parties.tryJoin(second_connection, second, party).has_value());
        const auto full = parties.tryJoin(third_connection, snf::server::PlayerId{.value = 3}, party);
        assert(full && full->capacity_denied && !full->created);
        assert(!parties.routeFor(third_connection).has_value());
        assert(parties.routeCountFor(party) == 2);

        const auto leaving = parties.beginLeave(first_connection);
        assert(leaving && leaving->leaving);
        assert(!parties.tryJoin(first_connection, first, party).has_value());
        parties.rollbackLeave(*leaving);
        assert(!parties.routeFor(first_connection)->leaving);
        const auto leaving_again = parties.beginLeave(first_connection);
        assert(leaving_again && leaving_again->leaving);
        parties.completeLeave(*leaving_again);
        const auto rejoined = parties.tryJoin(third_connection, first, party);
        assert(rejoined && rejoined->route.membership_epoch == 2);
        parties.rollbackJoin(*rejoined);
        assert(!parties.routeFor(third_connection).has_value());
    }
}

void run_party_coordinator_tests()
{
    test_party_coordinator_bounds_membership_and_keeps_epochs_monotonic();
}
