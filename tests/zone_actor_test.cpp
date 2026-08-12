#include "snf/server/zone_actor.hpp"

#include <cassert>
#include <cstdint>
#include <limits>
#include <vector>

namespace
{
    void test_zone_owns_enter_move_leave_and_deterministic_aoi()
    {
        snf::server::ZoneActor zone{
            snf::server::ZoneId{.value = 1},
            snf::server::ZoneActorConfig{.aoi_radius = 10},
        };
        const snf::server::PlayerId first{.value = 1};
        const snf::server::PlayerId second{.value = 2};
        const snf::server::PlayerId third{.value = 3};

        assert(zone.handle(snf::server::EnterZoneCommand{
                               .player = second,
                               .route_epoch = 1,
                               .position = {.x = 6, .y = 8},
                           })
                   .status == snf::server::ZoneCommandStatus::Applied);
        const auto entered = zone.handle(snf::server::EnterZoneCommand{
            .player = first,
            .route_epoch = 1,
            .position = {.x = 0, .y = 0},
        });
        assert(entered.status == snf::server::ZoneCommandStatus::Applied);
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

        const snf::server::TimerId timer{.value = 10};
        assert(zone.handle(snf::server::ZoneSimulationTick{.timer = timer, .tick = 1}).status ==
               snf::server::ZoneCommandStatus::StaleTimer);
        assert(zone.handle(snf::server::ArmZoneSimulationTimer{.timer = timer}).status ==
               snf::server::ZoneCommandStatus::Applied);
        assert(zone.handle(snf::server::ZoneSimulationTick{.timer = timer, .tick = 1}).status ==
               snf::server::ZoneCommandStatus::Applied);
        assert(zone.handle(snf::server::ZoneSimulationTick{.timer = timer, .tick = 1}).status ==
               snf::server::ZoneCommandStatus::StaleTick);
        assert(zone.handle(snf::server::ZoneSimulationTick{.timer = timer, .tick = 0}).status ==
               snf::server::ZoneCommandStatus::StaleTick);
        assert(zone.lastTick() == 1);

        assert(zone.handle(snf::server::CancelZoneSimulationTimer{.timer = timer}).status ==
               snf::server::ZoneCommandStatus::Applied);
        assert(zone.handle(snf::server::ZoneSimulationTick{.timer = timer, .tick = 2}).status ==
               snf::server::ZoneCommandStatus::StaleTimer);
        const snf::server::TimerId replacement{.value = 11};
        assert(zone.handle(snf::server::ArmZoneSimulationTimer{.timer = replacement}).status ==
               snf::server::ZoneCommandStatus::Applied);
        assert(zone.lastTick() == 0);
        assert(zone.handle(snf::server::ZoneSimulationTick{.timer = timer, .tick = 3}).status ==
               snf::server::ZoneCommandStatus::StaleTimer);
        assert(
            zone.handle(snf::server::ZoneSimulationTick{.timer = replacement, .tick = 1}).status ==
            snf::server::ZoneCommandStatus::Applied);
    }
}

void run_zone_actor_tests()
{
    test_zone_owns_enter_move_leave_and_deterministic_aoi();
    test_zone_tick_and_distance_math_reject_stale_or_overflowing_inputs();
}
