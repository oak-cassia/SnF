#include "snf/runtime/tell_payload.hpp"

#include <cassert>
#include <memory>
#include <string>
#include <utility>

namespace
{
    struct Reward
    {
        std::uint64_t amount{0};
    };

    struct Greeting
    {
        std::string text;
    };

    void test_a_carrier_returns_its_value_only_to_the_matching_type()
    {
        auto carrier = snf::runtime::TellPayload::of(Reward{.amount = 42});
        assert(!carrier.empty());

        assert(!carrier.take<Greeting>());
        // A refused take leaves the carrier intact, so the owning binding can
        // still claim it after another binding declined.
        assert(!carrier.empty());

        const auto reward = carrier.take<Reward>();
        assert(reward && reward->amount == 42);
    }

    void test_taking_a_value_empties_the_carrier()
    {
        auto carrier = snf::runtime::TellPayload::of(Reward{.amount = 7});

        assert(carrier.take<Reward>());
        assert(carrier.empty());
        // At most once: a delivered tell cannot be claimed twice.
        assert(!carrier.take<Reward>());
    }

    void test_a_default_carrier_holds_nothing()
    {
        snf::runtime::TellPayload carrier;

        assert(carrier.empty());
        assert(!carrier.take<Reward>());
    }

    void test_a_moved_from_carrier_releases_ownership()
    {
        auto source = snf::runtime::TellPayload::of(Greeting{.text = "hello"});
        auto moved = std::move(source);

        const auto greeting = moved.take<Greeting>();
        assert(greeting && greeting->text == "hello");
    }

    void test_a_move_only_payload_survives_the_round_trip()
    {
        auto carrier = snf::runtime::TellPayload::of(std::make_unique<int>(9));

        auto owned = carrier.take<std::unique_ptr<int>>();
        assert(owned && *owned && **owned == 9);
        assert(carrier.empty());
    }
}

void run_tell_payload_tests()
{
    test_a_carrier_returns_its_value_only_to_the_matching_type();
    test_taking_a_value_empties_the_carrier();
    test_a_default_carrier_holds_nothing();
    test_a_moved_from_carrier_releases_ownership();
    test_a_move_only_payload_survives_the_round_trip();
}
