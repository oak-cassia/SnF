#include "snf/net/unique_file_descriptor.hpp"
#include "snf/server/room_transition_channel.hpp"

#include <cassert>
#include <cstdint>
#include <thread>
#include <unistd.h>
#include <vector>

#if defined(__linux__)
#include <sys/eventfd.h>
#endif

namespace
{
    snf::server::RoomTransitionCompletion completion(const std::uint64_t entry)
    {
        return snf::server::RoomTransitionCompletion{
            .entry_id = snf::server::RoomEntryId{.value = entry},
            .connection = snf::net::ConnectionId{.descriptor = 10, .generation = 20},
            .player = snf::server::PlayerId{.value = 30},
            .room = snf::server::RoomId{.value = 40},
            .zone = snf::server::ZoneId{.value = 50},
            .route_epoch = 60,
            .step = snf::server::RoomEntryStep::JoinRoom,
            .room_status = snf::server::RoomCommandStatus::Applied,
            .zone_status = snf::server::ZoneCommandStatus::Applied,
            .position = snf::server::ZonePosition{.x = 1, .y = 2},
        };
    }

    int create_wake_fd()
    {
        int fds[2];
        if (::pipe(fds) == -1)
        {
            return -1;
        }
        return fds[1];
    }

    void test_reservation_guarantees_one_reusable_completion_slot()
    {
        const int wake_fd = create_wake_fd();
        assert(wake_fd >= 0);
        snf::server::RoomTransitionChannel channel{2, wake_fd};

        const auto first = channel.tryReserve(snf::server::RoomEntryId{.value = 1});
        const auto second = channel.tryReserve(snf::server::RoomEntryId{.value = 2});
        assert(first && second);
        assert(!channel.tryReserve(snf::server::RoomEntryId{.value = 3}));
        assert(channel.publish(*first, completion(1)));
        assert(!channel.publish(*first, completion(99)));
        assert(channel.publish(*second, completion(2)));

        channel.release(*first);
        assert(channel.stats().reservations == 2);
        const auto first_completion = channel.tryPop();
        assert(first_completion && first_completion->entry_id.value == 1);
        assert(channel.stats().reservations == 1);

        const auto second_completion = channel.tryPop();
        assert(second_completion && second_completion->entry_id.value == 2);
        assert(!channel.tryPop());
        channel.release(*second);
        assert(channel.stats().reservations == 0);

        const auto reused = channel.tryReserve(snf::server::RoomEntryId{.value = 3});
        assert(reused);
        assert(channel.publish(*reused, completion(3)));
        assert(channel.tryPop()->entry_id.value == 3);
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
        const int wake_fd = create_wake_fd();
        assert(wake_fd >= 0);
        snf::server::RoomTransitionChannel channel{1, wake_fd};
        const auto ticket = channel.tryReserve(snf::server::RoomEntryId{.value = 1});
        assert(ticket && channel.publish(*ticket, completion(1)));

        channel.cancel();
        assert(!channel.tryPop());
        assert(!channel.tryReserve(snf::server::RoomEntryId{.value = 2}));
        assert(!channel.publish(*ticket, completion(2)));
        const auto stats = channel.stats();
        assert(stats.cancelled);
        assert(stats.reservations == 0);
        assert(stats.queued == 0);
    }

    void test_multiple_workers_publish_reserved_slots_concurrently()
    {
        constexpr std::size_t COUNT = 8;
        const int wake_fd = create_wake_fd();
        assert(wake_fd >= 0);
        snf::server::RoomTransitionChannel channel{COUNT, wake_fd};
        std::vector<snf::server::RoomTransitionTicket> tickets;
        tickets.reserve(COUNT);
        for (std::size_t index = 0; index < COUNT; ++index)
        {
            const auto ticket = channel.tryReserve(snf::server::RoomEntryId{.value = index + 1});
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
            assert(completed && completed->entry_id.value >= 1 && completed->entry_id.value <= COUNT);
            seen[completed->entry_id.value - 1] = true;
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

    void test_return_reservations_and_correlation()
    {
        const int wake_fd = create_wake_fd();
        assert(wake_fd >= 0);
        snf::server::RoomTransitionChannel channel{2, wake_fd};

        const auto ticket = channel.tryReserve(snf::server::RoomReturnId{.value = 42});
        assert(ticket);

        snf::server::RoomTransitionCompletion comp{
            .entry_id = {},
            .return_id = snf::server::RoomReturnId{.value = 42},
            .connection = {.descriptor = 1, .generation = 1},
            .player = {.value = 10},
            .room = {.value = 20},
            .zone = {.value = 30},
            .route_epoch = 1,
            .step = snf::server::RoomEntryStep::JoinRoom,
            .room_status = snf::server::RoomCommandStatus::Applied,
            .zone_status = snf::server::ZoneCommandStatus::Applied,
            .position = snf::server::ZonePosition{.x = 10, .y = 20},
        };

        auto wrong_comp = comp;
        wrong_comp.return_id.value = 99;
        assert(!channel.publish(*ticket, wrong_comp));

        assert(channel.publish(*ticket, comp));
        const auto popped = channel.tryPop();
        assert(popped && popped->return_id.value == 42);
        channel.release(*ticket);
        assert(channel.stats().reservations == 0);
    }

    void test_return_requests_queue_without_a_reservation()
    {
        const int wake = create_wake_fd();
        assert(wake >= 0);
        snf::net::UniqueFileDescriptor guard{wake};
        snf::server::RoomTransitionChannel channel{2, wake};

        const snf::server::RoomReturnRequest first{
            .room = snf::server::RoomId{.value = 7},
            .player = snf::server::PlayerId{.value = 11},
        };
        const snf::server::RoomReturnRequest second{
            .room = snf::server::RoomId{.value = 7},
            .player = snf::server::PlayerId{.value = 12},
        };

        assert(channel.tryPublishReturnRequest(first));
        assert(channel.tryPublishReturnRequest(second));
        assert(channel.stats().return_requests_queued == 2);
        assert(!channel.drained());

        assert(!channel.tryPublishReturnRequest(first));
        assert(channel.stats().return_requests_rejected == 1);

        const auto popped = channel.tryPopReturnRequest();
        assert(popped && popped->player == snf::server::PlayerId{.value = 11});
        const auto next = channel.tryPopReturnRequest();
        assert(next && next->player == snf::server::PlayerId{.value = 12});
        assert(!channel.tryPopReturnRequest());
        assert(channel.stats().return_requests_consumed == 2);
        assert(channel.drained());

        assert(!channel.tryPublishReturnRequest(snf::server::RoomReturnRequest{.room = {}, .player = snf::server::PlayerId{.value = 11}}));
        assert(!channel.tryPublishReturnRequest(snf::server::RoomReturnRequest{.room = snf::server::RoomId{.value = 7}, .player = {}}));
    }

    void test_cancel_reclaims_return_requests()
    {
        const int wake = create_wake_fd();
        assert(wake >= 0);
        snf::net::UniqueFileDescriptor guard{wake};
        snf::server::RoomTransitionChannel channel{2, wake};

        assert(channel.tryPublishReturnRequest(snf::server::RoomReturnRequest{
            .room = snf::server::RoomId{.value = 7},
            .player = snf::server::PlayerId{.value = 11},
        }));
        channel.cancel();

        assert(!channel.tryPopReturnRequest());
        assert(!channel.tryPublishReturnRequest(snf::server::RoomReturnRequest{
            .room = snf::server::RoomId{.value = 7},
            .player = snf::server::PlayerId{.value = 11},
        }));
        assert(channel.drained());
    }

    void test_a_completion_that_lost_its_correlation_is_refused()
    {
        const int wake = create_wake_fd();
        assert(wake >= 0);
        snf::net::UniqueFileDescriptor guard{wake};
        snf::server::RoomTransitionChannel channel{2, wake};

        const snf::server::RoomEntryContext context{
            .entry_id = {},
            .return_id = snf::server::RoomReturnId{.value = 42},
            .ticket = {},
            .connection = snf::net::ConnectionId{.descriptor = 10, .generation = 20},
            .player = snf::server::PlayerId{.value = 30},
            .step = snf::server::RoomEntryStep::ReturnZone,
        };
        const auto ticket = channel.tryReserve(context.return_id);
        assert(ticket);

        auto forgetful = snf::server::RoomTransitionCompletion{
            .entry_id = context.entry_id,
            .connection = context.connection,
            .player = context.player,
            .step = context.step,
        };
        assert(!channel.publish(*ticket, forgetful));
        assert(channel.stats().invalid_publishes == 1);

        auto whole = snf::server::completionFrom(context);
        whole.zone_status = snf::server::ZoneCommandStatus::Applied;
        assert(channel.publish(*ticket, whole));
        const auto popped = channel.tryPop();
        assert(popped && popped->return_id == context.return_id);
        assert(popped->step == snf::server::RoomEntryStep::ReturnZone);
        assert(popped->connection == context.connection);
        assert(popped->player == context.player);
    }
}

void run_room_transition_channel_tests()
{
    test_reservation_guarantees_one_reusable_completion_slot();
    test_cancel_reclaims_queue_and_rejects_future_work();
    test_multiple_workers_publish_reserved_slots_concurrently();
    test_return_reservations_and_correlation();
    test_return_requests_queue_without_a_reservation();
    test_cancel_reclaims_return_requests();
    test_a_completion_that_lost_its_correlation_is_refused();
}
