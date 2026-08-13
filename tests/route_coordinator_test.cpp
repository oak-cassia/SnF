#include "snf/server/route_coordinator.hpp"

#include <cassert>

namespace
{
    constexpr snf::net::ConnectionId FIRST_CONNECTION{.descriptor = 10, .generation = 100};
    constexpr snf::net::ConnectionId SECOND_CONNECTION{.descriptor = 11, .generation = 101};
    constexpr snf::server::PlayerId PLAYER{.value = 77};
    constexpr snf::server::PlayerId SECOND_PLAYER{.value = 78};
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

    void test_handoff_hides_route_and_restores_with_a_new_epoch()
    {
        snf::server::RouteCoordinator routes{1};
        const auto source = routes.tryEnter(FIRST_CONNECTION, PLAYER, FIRST_ZONE);
        assert(source && source->route.route_epoch == 1);
        assert(routes.tryEnter(SECOND_CONNECTION, SECOND_PLAYER, FIRST_ZONE));

        const auto handoff = routes.tryBeginHandoff(
            FIRST_CONNECTION, PLAYER, SECOND_ZONE, {.x = 10, .y = 20}, {.x = 30, .y = 40}, 99);
        assert(handoff && handoff->target_epoch == 2);
        assert(handoff->step == snf::server::ZoneHandoffStep::LeaveSource);
        assert(!routes.routeFor(FIRST_CONNECTION));
        assert(routes.routeCountFor(FIRST_ZONE) == 2);
        assert(routes.routeCountFor(SECOND_ZONE) == 1);
        assert(!routes.tryBeginHandoff(SECOND_CONNECTION,
                                       SECOND_PLAYER,
                                       SECOND_ZONE,
                                       {.x = 1, .y = 2},
                                       {.x = 3, .y = 4},
                                       100));

        assert(!routes.noteSourceLeft(
            FIRST_CONNECTION, snf::server::ZoneHandoffId{.value = 999}, {.x = 11, .y = 21}));
        assert(!routes.noteSourceLeft(SECOND_CONNECTION, handoff->id, {.x = 11, .y = 21}));
        assert(routes.noteSourceLeft(FIRST_CONNECTION, handoff->id, {.x = 11, .y = 21}));
        assert(routes.handoffFor(FIRST_CONNECTION)->step ==
               snf::server::ZoneHandoffStep::EnterTarget);
        const auto restore_epoch = routes.beginSourceRestore(FIRST_CONNECTION, handoff->id);
        assert(restore_epoch && *restore_epoch == 3);
        const auto restored = routes.completeSourceRestore(FIRST_CONNECTION, handoff->id);
        assert(restored && restored->zone == FIRST_ZONE && restored->route_epoch == 3);
        assert(routes.routeFor(FIRST_CONNECTION) == restored);
        assert(routes.routeCountFor(SECOND_ZONE) == 0);

        const auto second_handoff = routes.tryBeginHandoff(
            FIRST_CONNECTION, PLAYER, SECOND_ZONE, {.x = 11, .y = 21}, {.x = 50, .y = 60}, 101);
        assert(second_handoff && second_handoff->target_epoch == 4);
        assert(routes.noteSourceLeft(FIRST_CONNECTION, second_handoff->id, {.x = 11, .y = 21}));
        const auto target = routes.completeTargetEnter(FIRST_CONNECTION, second_handoff->id);
        assert(target && target->zone == SECOND_ZONE && target->route_epoch == 4);
        assert(routes.routeFor(FIRST_CONNECTION) == target);

        const auto stats = routes.stats();
        assert(stats.handoffs_started == 2);
        assert(stats.handoffs_completed == 1);
        assert(stats.handoffs_restored == 1);
        assert(stats.handoffs_rejected == 1);
        assert(stats.pending_handoffs == 0);
        assert(stats.handoff_high_water_mark == 1);
    }

    void test_handoff_can_rollback_before_source_leave_or_abandon()
    {
        snf::server::RouteCoordinator routes;
        const auto source = routes.tryEnter(FIRST_CONNECTION, PLAYER, FIRST_ZONE);
        assert(source);
        const auto handoff = routes.tryBeginHandoff(
            FIRST_CONNECTION, PLAYER, SECOND_ZONE, {.x = 1, .y = 2}, {.x = 3, .y = 4}, 1);
        assert(handoff && routes.rollbackHandoffBeforeLeave(FIRST_CONNECTION, handoff->id));
        assert(routes.routeFor(FIRST_CONNECTION) == source->route);
        assert(routes.stats().handoffs_rolled_back == 1);

        const auto next = routes.tryBeginHandoff(
            FIRST_CONNECTION, PLAYER, SECOND_ZONE, {.x = 1, .y = 2}, {.x = 3, .y = 4}, 2);
        assert(next);
        routes.abandon(FIRST_CONNECTION);
        assert(!routes.routeFor(FIRST_CONNECTION));
        assert(!routes.handoffFor(FIRST_CONNECTION));
        assert(routes.stats().handoffs_abandoned == 1);
    }
}

void run_route_coordinator_tests()
{
    test_route_coordinator_is_idempotent_and_monotonic();
    test_route_coordinator_rolls_back_only_the_created_route();
    test_handoff_hides_route_and_restores_with_a_new_epoch();
    test_handoff_can_rollback_before_source_leave_or_abandon();
}
