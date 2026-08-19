#include "snf/game/street_progression.hpp"

#include <cassert>
#include <cstdint>
#include <limits>

namespace
{
    using snf::server::CombatStats;
    using snf::server::combatStats;
    using snf::server::streetLevel;

    void test_experience_below_the_first_threshold_stays_at_level_one()
    {
        assert(streetLevel(0) == 1);
        // The point just short of a threshold and the threshold itself are kept
        // adjacent so an off-by-one in the comparison cannot pass.
        assert(streetLevel(999) == 1);
        assert(streetLevel(1000) == 2);
    }

    void test_a_threshold_raises_the_level_exactly_once()
    {
        assert(streetLevel(1999) == 2);
        assert(streetLevel(2000) == 3);
        assert(streetLevel(28999) == 29);
    }

    void test_the_level_stops_at_the_cap()
    {
        assert(streetLevel(29000) == 30);
        assert(streetLevel(29001) == 30);
        // Experience keeps accumulating past the cap rather than being clamped at
        // the source, so an arbitrarily large record still reads as the cap.
        assert(streetLevel(std::numeric_limits<std::uint64_t>::max()) == 30);
    }

    void test_stats_at_level_one_are_the_base_values()
    {
        assert(combatStats(1) == (CombatStats{.attack = 10, .health = 100}));
    }

    void test_each_level_adds_ten_percent_of_the_base()
    {
        assert(combatStats(2) == (CombatStats{.attack = 11, .health = 110}));
        // Ten levels of growth double the base exactly.
        assert(combatStats(11) == (CombatStats{.attack = 20, .health = 200}));
    }

    void test_stats_at_the_cap_are_capped_growth()
    {
        assert(combatStats(30) == (CombatStats{.attack = 39, .health = 390}));
    }

    void test_a_level_outside_the_valid_range_reads_as_the_nearest_one()
    {
        // Level 0 would wrap the growth subtraction; a level above the cap would
        // grow past what the cap is supposed to bound.
        assert(combatStats(0) == combatStats(1));
        assert(combatStats(1000) == combatStats(30));
    }

    void test_stats_derived_from_experience_never_exceed_the_cap()
    {
        const std::uint64_t level = streetLevel(std::numeric_limits<std::uint64_t>::max());
        assert(combatStats(level) == combatStats(30));
    }
}

void run_street_progression_tests()
{
    test_experience_below_the_first_threshold_stays_at_level_one();
    test_a_threshold_raises_the_level_exactly_once();
    test_the_level_stops_at_the_cap();
    test_stats_at_level_one_are_the_base_values();
    test_each_level_adds_ten_percent_of_the_base();
    test_stats_at_the_cap_are_capped_growth();
    test_a_level_outside_the_valid_range_reads_as_the_nearest_one();
    test_stats_derived_from_experience_never_exceed_the_cap();
}
