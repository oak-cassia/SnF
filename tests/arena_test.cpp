#include "snf/game/arena.hpp"

#include <cassert>
#include <cstdint>
#include <limits>

namespace
{
    using snf::server::ArenaPosition;
    using snf::server::MoveDirection;

    void test_directional_movement_is_saturating_and_stop_is_valid()
    {
        assert(snf::server::isValidMoveDirection(0));
        assert(snf::server::isValidMoveDirection(8));
        assert(!snf::server::isValidMoveDirection(9));
        assert(snf::server::moveInDirection({.x = 50, .y = 50}, MoveDirection::North, 4, 100, 100) == (ArenaPosition{.x = 50, .y = 46}));
        assert(snf::server::moveInDirection({.x = 50, .y = 50}, MoveDirection::NorthEast, 4, 100, 100) == (ArenaPosition{.x = 54, .y = 46}));
        assert(snf::server::moveInDirection({.x = 50, .y = 50}, MoveDirection::East, 4, 100, 100) == (ArenaPosition{.x = 54, .y = 50}));
        assert(snf::server::moveInDirection({.x = 50, .y = 50}, MoveDirection::SouthEast, 4, 100, 100) == (ArenaPosition{.x = 54, .y = 54}));
        assert(snf::server::moveInDirection({.x = 50, .y = 50}, MoveDirection::South, 4, 100, 100) == (ArenaPosition{.x = 50, .y = 54}));
        assert(snf::server::moveInDirection({.x = 50, .y = 50}, MoveDirection::SouthWest, 4, 100, 100) == (ArenaPosition{.x = 46, .y = 54}));
        assert(snf::server::moveInDirection({.x = 50, .y = 50}, MoveDirection::West, 4, 100, 100) == (ArenaPosition{.x = 46, .y = 50}));
        assert(snf::server::moveInDirection({.x = 50, .y = 50}, MoveDirection::NorthWest, 4, 100, 100) == (ArenaPosition{.x = 46, .y = 46}));
        assert(snf::server::moveInDirection({.x = 1, .y = 1}, MoveDirection::NorthWest, 4, 100, 100) == (ArenaPosition{.x = 0, .y = 0}));
        assert(snf::server::moveInDirection({.x = 98, .y = 98}, MoveDirection::SouthEast, 4, 100, 100) == (ArenaPosition{.x = 99, .y = 99}));
        assert(snf::server::moveInDirection({.x = 50, .y = 50}, MoveDirection::Stop, 4, 100, 100) == (ArenaPosition{.x = 50, .y = 50}));
    }

    void test_targeted_movement_never_overshoots()
    {
        assert(snf::server::moveToward({.x = 10, .y = 20}, {.x = 12, .y = 17}, 4, 100, 100) == (ArenaPosition{.x = 12, .y = 17}));
        assert(snf::server::moveToward({.x = 10, .y = 20}, {.x = 50, .y = 80}, 4, 100, 100) == (ArenaPosition{.x = 14, .y = 24}));
    }

    void test_squared_distance_uses_widened_checked_arithmetic()
    {
        assert(snf::server::squaredDistance({.x = 1, .y = 2}, {.x = 4, .y = 6}) == 25);
        assert(snf::server::isWithinRange({.x = 1, .y = 2}, {.x = 4, .y = 6}, 5));
        assert(!snf::server::isWithinRange({.x = 1, .y = 2}, {.x = 4, .y = 6}, 4));
        assert(
            snf::server::squaredDistance(
                {.x = 0, .y = 0}, {.x = std::numeric_limits<std::uint32_t>::max(), .y = std::numeric_limits<std::uint32_t>::max()}
            ) == std::numeric_limits<std::uint64_t>::max()
        );
    }

    void test_participants_form_a_centered_row()
    {
        assert(snf::server::centeredParticipantPosition(0, 1, 100, 100, 4) == (ArenaPosition{.x = 50, .y = 50}));
        assert(snf::server::centeredParticipantPosition(0, 4, 100, 100, 4) == (ArenaPosition{.x = 44, .y = 50}));
        assert(snf::server::centeredParticipantPosition(3, 4, 100, 100, 4) == (ArenaPosition{.x = 56, .y = 50}));
    }

    void test_enemies_walk_the_square_perimeter_clockwise_from_the_top()
    {
        assert(snf::server::squarePerimeterPosition(0, 8, 100, 100, 25) == (ArenaPosition{.x = 50, .y = 25}));
        assert(snf::server::squarePerimeterPosition(1, 8, 100, 100, 25) == (ArenaPosition{.x = 75, .y = 25}));
        assert(snf::server::squarePerimeterPosition(3, 8, 100, 100, 25) == (ArenaPosition{.x = 75, .y = 75}));
        assert(snf::server::squarePerimeterPosition(5, 8, 100, 100, 25) == (ArenaPosition{.x = 25, .y = 75}));
        assert(snf::server::squarePerimeterPosition(7, 8, 100, 100, 25) == (ArenaPosition{.x = 25, .y = 25}));
        assert(snf::server::bossSpawnPosition(100) == (ArenaPosition{.x = 50, .y = 0}));
    }
}

void run_arena_tests()
{
    test_directional_movement_is_saturating_and_stop_is_valid();
    test_targeted_movement_never_overshoots();
    test_squared_distance_uses_widened_checked_arithmetic();
    test_participants_form_a_centered_row();
    test_enemies_walk_the_square_perimeter_clockwise_from_the_top();
}
