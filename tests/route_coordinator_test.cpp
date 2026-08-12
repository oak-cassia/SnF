#include "snf/server/route_coordinator.hpp"

#include <cassert>

namespace
{
    constexpr snf::net::ConnectionId FIRST_CONNECTION{.descriptor = 10, .generation = 100};
    constexpr snf::net::ConnectionId SECOND_CONNECTION{.descriptor = 11, .generation = 101};
    constexpr snf::server::PlayerId PLAYER{.value = 77};
    constexpr snf::server::ZoneId FIRST_ZONE{.value = 1};
    constexpr snf::server::ZoneId SECOND_ZONE{.value = 2};

    void test_route_coordinator_is_idempotent_and_monotonic()
    {
        snf::server::RouteCoordinator routes;
        const auto first = routes.tryEnter(FIRST_CONNECTION, PLAYER, FIRST_ZONE);
        assert(first.has_value());
        assert(first->created);
        assert(first->route.route_epoch == 1);
        assert(routes.routeFor(FIRST_CONNECTION) == first->route);
        assert(routes.routeCountFor(FIRST_ZONE) == 1);
        assert(routes.routeCountFor(SECOND_ZONE) == 0);

        const auto repeated = routes.tryEnter(FIRST_CONNECTION, PLAYER, FIRST_ZONE);
        assert(repeated.has_value());
        assert(!repeated->created);
        assert(repeated->route == first->route);
        assert(!routes.tryEnter(FIRST_CONNECTION, PLAYER, SECOND_ZONE).has_value());

        routes.completeLeave(first->route);
        assert(!routes.routeFor(FIRST_CONNECTION).has_value());
        assert(routes.routeCountFor(FIRST_ZONE) == 0);
        const auto reentered = routes.tryEnter(SECOND_CONNECTION, PLAYER, FIRST_ZONE);
        assert(reentered.has_value());
        assert(reentered->route.route_epoch == 2);
    }

    void test_route_coordinator_rolls_back_only_the_created_route()
    {
        snf::server::RouteCoordinator routes;
        const auto admission = routes.tryEnter(FIRST_CONNECTION, PLAYER, FIRST_ZONE);
        assert(admission.has_value());
        routes.rollbackEnter(*admission);
        assert(!routes.routeFor(FIRST_CONNECTION).has_value());

        const auto retried = routes.tryEnter(FIRST_CONNECTION, PLAYER, FIRST_ZONE);
        assert(retried.has_value());
        assert(retried->route.route_epoch == 2);
        const auto repeated = routes.tryEnter(FIRST_CONNECTION, PLAYER, FIRST_ZONE);
        assert(repeated.has_value());
        routes.rollbackEnter(*repeated);
        assert(routes.routeFor(FIRST_CONNECTION) == retried->route);
    }
}

void run_route_coordinator_tests()
{
    test_route_coordinator_is_idempotent_and_monotonic();
    test_route_coordinator_rolls_back_only_the_created_route();
}
