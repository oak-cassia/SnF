#include "snf/net/unique_file_descriptor.hpp"
#include "snf/server/zone_transition_channel.hpp"

#include <cassert>
#include <cstdint>
#include <sys/eventfd.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace
{
    snf::server::ZoneHandoffCompletion completion(const std::uint64_t handoff)
    {
        return snf::server::ZoneHandoffCompletion{
            .handoff_id = snf::server::ZoneHandoffId{.value = handoff},
            .connection = snf::net::ConnectionId{.descriptor = 10, .generation = 20},
            .player = snf::server::PlayerId{.value = 30},
            .zone = snf::server::ZoneId{.value = 40},
            .route_epoch = 50,
            .step = snf::server::ZoneHandoffStep::LeaveSource,
            .status = snf::server::ZoneCommandStatus::Applied,
            .position = snf::server::ZonePosition{.x = 1, .y = 2},
        };
    }

    void test_reservation_guarantees_one_reusable_completion_slot()
    {
        snf::net::UniqueFileDescriptor wake{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
        assert(wake.isValid());
        snf::server::ZoneTransitionChannel channel{2, wake.getDescriptor()};

        const auto first = channel.tryReserve({.value = 1});
        const auto second = channel.tryReserve({.value = 2});
        assert(first && second);
        assert(!channel.tryReserve({.value = 3}));
        assert(channel.publish(*first, completion(1)));
        assert(!channel.publish(*first, completion(99)));
        assert(channel.publish(*second, completion(2)));

        std::uint64_t wake_count = 0;
        assert(::read(wake.getDescriptor(), &wake_count, sizeof(wake_count)) == static_cast<ssize_t>(sizeof(wake_count)));
        assert(wake_count == 2);

        channel.release(*first);
        assert(channel.stats().reservations == 2);
        const auto first_completion = channel.tryPop();
        assert(first_completion && first_completion->handoff_id.value == 1);
        assert(channel.stats().reservations == 1);

        const auto second_completion = channel.tryPop();
        assert(second_completion && second_completion->handoff_id.value == 2);
        assert(!channel.tryPop());
        channel.release(*second);
        assert(channel.stats().reservations == 0);

        const auto reused = channel.tryReserve({.value = 3});
        assert(reused);
        assert(channel.publish(*reused, completion(3)));
        assert(channel.tryPop()->handoff_id.value == 3);
        channel.release(*reused);

        const auto stats = channel.stats();
        assert(stats.reservations_created == 3);
        assert(stats.reservations_rejected == 1);
        assert(stats.completions_published == 3);
        assert(stats.completions_consumed == 3);
        assert(stats.invalid_publishes == 1);
        assert(stats.reservation_high_water_mark == 2);
        assert(stats.queue_high_water_mark == 2);
    }

    void test_cancel_reclaims_queue_and_rejects_future_work()
    {
        snf::net::UniqueFileDescriptor wake{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
        assert(wake.isValid());
        snf::server::ZoneTransitionChannel channel{1, wake.getDescriptor()};
        const auto ticket = channel.tryReserve({.value = 1});
        assert(ticket && channel.publish(*ticket, completion(1)));

        channel.cancel();
        assert(!channel.tryPop());
        assert(!channel.tryReserve({.value = 2}));
        assert(!channel.publish(*ticket, completion(2)));
        const auto stats = channel.stats();
        assert(stats.cancelled);
        assert(stats.reservations == 0);
        assert(stats.queued == 0);
    }

    void test_multiple_workers_publish_reserved_slots_concurrently()
    {
        constexpr std::size_t COUNT = 8;
        snf::net::UniqueFileDescriptor wake{::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC)};
        assert(wake.isValid());
        snf::server::ZoneTransitionChannel channel{COUNT, wake.getDescriptor()};
        std::vector<snf::server::ZoneTransitionTicket> tickets;
        tickets.reserve(COUNT);
        for (std::size_t index = 0; index < COUNT; ++index)
        {
            const auto ticket = channel.tryReserve({.value = index + 1});
            assert(ticket);
            tickets.push_back(*ticket);
        }

        std::vector<std::thread> workers;
        workers.reserve(COUNT);
        for (std::size_t index = 0; index < COUNT; ++index)
        {
            workers.emplace_back([&channel, ticket = tickets[index], index] { assert(channel.publish(ticket, completion(index + 1))); });
        }
        for (std::thread& worker : workers)
        {
            worker.join();
        }

        std::vector<bool> seen(COUNT, false);
        for (std::size_t index = 0; index < COUNT; ++index)
        {
            const auto completed = channel.tryPop();
            assert(completed && completed->handoff_id.value >= 1 && completed->handoff_id.value <= COUNT);
            seen[completed->handoff_id.value - 1] = true;
        }
        for (const bool value : seen)
        {
            assert(value);
        }
        for (const auto ticket : tickets)
        {
            channel.release(ticket);
        }
        assert(channel.stats().reservations == 0);
    }
}

void run_zone_transition_channel_tests()
{
    test_reservation_guarantees_one_reusable_completion_slot();
    test_cancel_reclaims_queue_and_rejects_future_work();
    test_multiple_workers_publish_reserved_slots_concurrently();
}
