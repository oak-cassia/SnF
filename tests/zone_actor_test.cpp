#include "snf/server/zone_actor.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <limits>
#include <vector>

using namespace std::chrono_literals;

namespace
{
    void test_zone_owns_enter_move_leave_and_deterministic_aoi()
    {
        snf::server::ZoneActor zone{
            snf::server::ZoneId{.value = 1},
            snf::server::ZoneActorConfig{
                .aoi_radius = 10,
                .tick_interval = 50ms,
            },
        };
        const snf::server::PlayerId first{.value = 1};
        const snf::server::PlayerId second{.value = 2};
        const snf::server::PlayerId third{.value = 3};

        const auto second_entered = zone.handle(snf::server::EnterZoneCommand{
            .player = second,
            .route_epoch = 1,
            .position = {.x = 6, .y = 8},
        });
        assert(second_entered.status == snf::server::ZoneCommandStatus::Applied);
        assert(second_entered.timer.has_value());
        assert(second_entered.timer->delay == 50ms);

        const auto entered = zone.handle(snf::server::EnterZoneCommand{
            .player = first,
            .route_epoch = 1,
            .position = {.x = 0, .y = 0},
        });
        assert(entered.status == snf::server::ZoneCommandStatus::Applied);
        assert(!entered.timer.has_value());
        assert(entered.visible_players == std::vector<snf::server::PlayerId>{second});

        assert(zone.handle(snf::server::EnterZoneCommand{
                               .player = third,
                               .route_epoch = 1,
                               .position = {.x = 11, .y = 0},
                           })
                   .status == snf::server::ZoneCommandStatus::Applied);
        assert(zone.visiblePlayers(first) == std::vector<snf::server::PlayerId>{second});
        assert(zone.playerCount() == 3);

        const auto stale_move = zone.handle(snf::server::MoveInZoneCommand{
            .player = second,
            .route_epoch = 0,
            .position = {.x = 20, .y = 20},
        });
        assert(stale_move.status == snf::server::ZoneCommandStatus::StaleRoute);
        assert((zone.positionOf(second) == snf::server::ZonePosition{.x = 6, .y = 8}));

        const auto moved = zone.handle(snf::server::MoveInZoneCommand{
            .player = second,
            .route_epoch = 1,
            .position = {.x = 20, .y = 20},
        });
        assert(moved.status == snf::server::ZoneCommandStatus::Applied);
        assert(zone.visiblePlayers(first).empty());

        const auto stale_leave = zone.handle(snf::server::LeaveZoneCommand{
            .player = second,
            .route_epoch = 2,
        });
        assert(stale_leave.status == snf::server::ZoneCommandStatus::StaleRoute);
        const auto left = zone.handle(snf::server::LeaveZoneCommand{
            .player = second,
            .route_epoch = 1,
        });
        assert(left.status == snf::server::ZoneCommandStatus::Applied);
        assert(!zone.positionOf(second).has_value());
    }

    void test_zone_tick_and_distance_math_reject_stale_or_overflowing_inputs()
    {
        snf::server::ZoneActor zone{
            snf::server::ZoneId{.value = 2},
            snf::server::ZoneActorConfig{
                .aoi_radius = std::numeric_limits<std::int32_t>::max(),
                .tick_interval = 100ms,
            },
        };
        const snf::server::PlayerId low{.value = 1};
        const snf::server::PlayerId high{.value = 2};
        static_cast<void>(zone.handle(snf::server::EnterZoneCommand{
            .player = low,
            .route_epoch = 1,
            .position =
                {
                    .x = std::numeric_limits<std::int32_t>::min(),
                    .y = 0,
                },
        }));
        static_cast<void>(zone.handle(snf::server::EnterZoneCommand{
            .player = high,
            .route_epoch = 1,
            .position =
                {
                    .x = std::numeric_limits<std::int32_t>::max(),
                    .y = 0,
                },
        }));
        assert(zone.visiblePlayers(low).empty());

        assert(zone.lastTick() == 0);
        const auto tick1 = zone.handle(snf::server::ZoneSimulationTick{});
        assert(tick1.status == snf::server::ZoneCommandStatus::Applied);
        assert(tick1.tick == 1);
        assert(zone.lastTick() == 1);
        assert(tick1.timer.has_value());
        assert(tick1.timer->delay == 100ms);

        const auto tick2 = zone.handle(snf::server::ZoneSimulationTick{});
        assert(tick2.status == snf::server::ZoneCommandStatus::Applied);
        assert(tick2.tick == 2);
        assert(zone.lastTick() == 2);

        // Remove all players
        static_cast<void>(
            zone.handle(snf::server::LeaveZoneCommand{.player = low, .route_epoch = 1}));
        static_cast<void>(
            zone.handle(snf::server::LeaveZoneCommand{.player = high, .route_epoch = 1}));
        assert(zone.playerCount() == 0);

        // Tick when zone is empty should not request another timer
        const auto tick3 = zone.handle(snf::server::ZoneSimulationTick{});
        assert(tick3.status == snf::server::ZoneCommandStatus::Applied);
        assert(tick3.tick == 3);
        assert(!tick3.timer.has_value());
    }
}

void run_zone_actor_tests()
{
    test_zone_owns_enter_move_leave_and_deterministic_aoi();
    test_zone_tick_and_distance_math_reject_stale_or_overflowing_inputs();
}
