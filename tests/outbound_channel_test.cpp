#include "outbound_reservation_test_support.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    using snf::server::OutboundChannel;
    using snf::server::OutboundChannelConfig;
    using snf::server::OutboundReservation;
    using snf::test::make_wake_descriptor;
    using RecordingEndpoint = snf::test::RecordingContinuationEndpoint;
    using TestWaiter = snf::test::ReservationWaiter;

    constexpr snf::net::ConnectionId connection_of(const int descriptor)
    {
        return snf::net::ConnectionId{.descriptor = descriptor, .generation = 1};
    }

    snf::server::OutboundAction send_action(const snf::net::ConnectionId connection, const std::uint32_t request_id)
    {
        return snf::server::SendFrame{
            .connection = connection,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::Pong,
                    .request_id = request_id,
                    .payload = {},
                },
        };
    }

    std::uint32_t request_id_of(const snf::server::PostedOutboundAction& posted)
    {
        return std::get<snf::server::SendFrame>(posted.action).frame.request_id;
    }

    void test_reserve_then_commit_moves_capacity_from_reserved_to_queued()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 4, .max_slots_per_connection = 4}, wake.getDescriptor()};
        const auto connection = connection_of(4);

        auto reservation = channel.tryReserve(connection, 2);
        assert(reservation);
        assert(reservation->valid());
        assert(reservation->remainingSlots() == 2);
        assert(channel.reservedSlotCount() == 2);
        assert(channel.size() == 0);

        assert(channel.commit(*reservation, send_action(connection, 1)));
        assert(channel.commit(*reservation, send_action(connection, 2)));
        assert(reservation->remainingSlots() == 0);
        assert(channel.reservedSlotCount() == 0);
        assert(channel.size() == 2);

        const auto first = channel.tryPop();
        const auto second = channel.tryPop();
        assert(first && request_id_of(*first) == 1);
        assert(second && request_id_of(*second) == 2);
        assert(!channel.tryPop());
        assert(channel.highWaterMark() == 2);
    }

    void test_zero_slot_reservation_is_valid_and_consumes_no_capacity()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1}, wake.getDescriptor()};

        const auto reservation = channel.tryReserve(connection_of(4), 0);
        assert(reservation);
        assert(reservation->valid());
        assert(reservation->remainingSlots() == 0);
        assert(channel.reservedSlotCount() == 0);
    }

    void test_uncommitted_slots_return_when_the_reservation_dies()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 2, .max_slots_per_connection = 2}, wake.getDescriptor()};
        const auto connection = connection_of(4);

        {
            auto reservation = channel.tryReserve(connection, 2);
            assert(reservation);
            assert(channel.commit(*reservation, send_action(connection, 1)));
            assert(channel.reservedSlotCount() == 1);
        }

        assert(channel.reservedSlotCount() == 0);
        assert(channel.size() == 1);
        assert(channel.tryReserve(connection, 2) == std::nullopt);
        assert(channel.tryPop());
        assert(channel.tryReserve(connection, 2));
    }

    void test_a_tracked_connection_keeps_its_accounting_between_commands()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 4, .max_slots_per_connection = 4}, wake.getDescriptor()};
        const auto connection = connection_of(4);
        channel.trackConnection(connection);

        {
            auto reservation = channel.tryReserve(connection, 1);
            assert(reservation);
            assert(channel.commit(*reservation, send_action(connection, 1)));
        }
        assert(channel.tryPop());

        assert(channel.trackedConnectionCount() == 1);

        channel.forgetConnection(connection);
        assert(channel.trackedConnectionCount() == 0);

        channel.trackConnection(connection);
        auto reservation = channel.tryReserve(connection, 1);
        assert(reservation);
        assert(channel.commit(*reservation, send_action(connection, 2)));
        channel.forgetConnection(connection);
        assert(channel.trackedConnectionCount() == 1);
        assert(channel.tryPop());
        assert(channel.trackedConnectionCount() == 0);
    }

    void test_an_untracked_connection_leaves_no_accounting_behind()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 4, .max_slots_per_connection = 4}, wake.getDescriptor()};
        const auto stale = connection_of(4);
        channel.forgetConnection(stale);

        auto reservation = channel.tryReserve(stale, 1);
        assert(reservation);
        assert(channel.trackedConnectionCount() == 1);
        assert(channel.commit(*reservation, send_action(stale, 1)));
        assert(channel.tryPop());
        assert(channel.trackedConnectionCount() == 0);

        const auto other = connection_of(5);
        auto blocking = channel.tryReserve(other, 4);
        assert(blocking);
        assert(channel.tryReserve(connection_of(6), 1) == std::nullopt);
        assert(channel.trackedConnectionCount() == 1);
    }

    void test_reserve_defers_to_a_registered_waiter()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 4, .max_slots_per_connection = 1}, wake.getDescriptor()};
        const auto endpoint = std::make_shared<RecordingEndpoint>();
        const auto blocked = connection_of(4);

        auto reservation = channel.tryReserve(blocked, 1);
        assert(reservation);
        assert(channel.commit(*reservation, send_action(blocked, 1)));

        TestWaiter waiter{endpoint, 1};
        const auto ticket = channel.registerWaiter(blocked, 1, std::move(waiter.producer));
        assert(ticket.valid());

        assert(channel.tryReserve(connection_of(5), 1) == std::nullopt);
        assert(channel.pendingWaiterCount() == 1);
    }

    void test_per_connection_limit_does_not_block_another_connection()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 8, .max_slots_per_connection = 1}, wake.getDescriptor()};
        const auto endpoint = std::make_shared<RecordingEndpoint>();
        const auto blocked = connection_of(4);
        const auto other = connection_of(5);

        auto queued = channel.tryReserve(blocked, 1);
        assert(queued);
        assert(channel.commit(*queued, send_action(blocked, 1)));

        TestWaiter blocked_waiter{endpoint, 1};
        TestWaiter other_waiter{endpoint, 2};
        static_cast<void>(channel.registerWaiter(blocked, 1, std::move(blocked_waiter.producer)));
        static_cast<void>(channel.registerWaiter(other, 1, std::move(other_waiter.producer)));

        assert(channel.grantPending() == 1);
        assert(other_waiter.state->outcome() == snf::runtime::AsyncOperationOutcome::Completed);
        assert(blocked_waiter.isPending());
        assert(channel.pendingWaiterCount() == 1);

        assert(channel.tryPop());
        assert(channel.grantPending() == 1);
        assert(blocked_waiter.state->outcome() == snf::runtime::AsyncOperationOutcome::Completed);
        assert(channel.pendingWaiterCount() == 0);

        auto granted = blocked_waiter.state->takeResult();
        assert(granted.valid());
        assert(granted.remainingSlots() == 1);
        assert(granted.connection() == blocked);
    }

    void test_global_capacity_blocks_a_waiter_until_a_pop_frees_a_slot()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1}, wake.getDescriptor()};
        const auto endpoint = std::make_shared<RecordingEndpoint>();
        const auto first = connection_of(4);

        auto reservation = channel.tryReserve(first, 1);
        assert(reservation);
        assert(channel.commit(*reservation, send_action(first, 1)));

        TestWaiter waiter{endpoint, 1};
        static_cast<void>(channel.registerWaiter(connection_of(5), 1, std::move(waiter.producer)));
        assert(channel.grantPending() == 0);
        assert(waiter.isPending());

        assert(channel.tryPop());
        assert(channel.grantPending() == 1);
        assert(!waiter.isPending());
        assert(endpoint->published.size() == 1);
    }

    void test_grant_pass_is_bounded_per_turn()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 8, .max_slots_per_connection = 8, .max_grants_per_turn = 2}, wake.getDescriptor()};
        const auto endpoint = std::make_shared<RecordingEndpoint>();

        std::vector<std::unique_ptr<TestWaiter>> waiters;
        for (std::uint64_t index = 0; index < 3; ++index)
        {
            auto waiter = std::make_unique<TestWaiter>(endpoint, index + 1);
            static_cast<void>(channel.registerWaiter(connection_of(static_cast<int>(index) + 4), 1, std::move(waiter->producer)));
            waiters.push_back(std::move(waiter));
        }

        assert(channel.grantPending() == 2);
        assert(channel.pendingWaiterCount() == 1);
        assert(channel.grantPending() == 1);
        assert(channel.pendingWaiterCount() == 0);
    }

    void test_award_that_lost_the_terminal_claim_returns_its_slots()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 2, .max_slots_per_connection = 2}, wake.getDescriptor()};
        const auto endpoint = std::make_shared<RecordingEndpoint>();
        const auto connection = connection_of(4);

        TestWaiter waiter{endpoint, 1};
        static_cast<void>(channel.registerWaiter(connection, 2, std::move(waiter.producer)));

        assert(waiter.state->claimCancelled());

        assert(channel.grantPending() == 1);
        assert(endpoint->published.empty());
        assert(endpoint->rejected_count == 1);
        assert(endpoint->last_rejection == snf::runtime::ContinuationRejection::AlreadyCancelled);
        assert(channel.reservedSlotCount() == 0);
        assert(channel.tryReserve(connection, 2));
    }

    void test_withdrawn_waiter_is_never_granted()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1}, wake.getDescriptor()};
        const auto endpoint = std::make_shared<RecordingEndpoint>();
        const auto connection = connection_of(4);

        auto reservation = channel.tryReserve(connection, 1);
        assert(reservation);
        assert(channel.commit(*reservation, send_action(connection, 1)));

        TestWaiter waiter{endpoint, 1};
        const auto ticket = channel.registerWaiter(connection, 1, std::move(waiter.producer));
        channel.withdrawWaiter(ticket);
        assert(channel.pendingWaiterCount() == 0);

        assert(channel.tryPop());
        assert(channel.grantPending() == 0);
        assert(waiter.isPending());
        channel.withdrawWaiter(ticket);
    }

    void test_cancel_releases_every_waiter_with_an_invalid_reservation()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 4, .max_slots_per_connection = 2}, wake.getDescriptor()};
        const auto endpoint = std::make_shared<RecordingEndpoint>();
        const auto connection = connection_of(4);

        auto queued = channel.tryReserve(connection, 2);
        assert(queued);
        assert(channel.commit(*queued, send_action(connection, 1)));

        TestWaiter first{endpoint, 1};
        TestWaiter second{endpoint, 2};
        static_cast<void>(channel.registerWaiter(connection, 2, std::move(first.producer)));
        static_cast<void>(channel.registerWaiter(connection_of(5), 2, std::move(second.producer)));

        assert(channel.cancel() == 1);
        assert(channel.isCancelled());
        assert(channel.pendingWaiterCount() == 0);

        for (const TestWaiter* waiter : {&first, &second})
        {
            assert(waiter->state->outcome() == snf::runtime::AsyncOperationOutcome::Completed);
            auto cancelled = waiter->state->takeResult();
            assert(!cancelled.valid());
        }

        assert(channel.tryReserve(connection, 1) == std::nullopt);
        assert(!channel.commit(*queued, send_action(connection, 2)));
        assert(channel.grantPending() == 0);
    }

    void test_registering_on_a_cancelled_channel_completes_the_producer()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1}, wake.getDescriptor()};
        const auto endpoint = std::make_shared<RecordingEndpoint>();
        static_cast<void>(channel.cancel());

        TestWaiter waiter{endpoint, 1};
        const auto ticket = channel.registerWaiter(connection_of(4), 1, std::move(waiter.producer));
        assert(!ticket.valid());
        assert(waiter.state->outcome() == snf::runtime::AsyncOperationOutcome::Completed);
        assert(!waiter.state->takeResult().valid());
    }

    void test_an_unsatisfiable_request_is_refused_without_throwing()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 4, .max_slots_per_connection = 2}, wake.getDescriptor()};
        const auto connection = connection_of(4);

        assert(!channel.canEverReserve(3));
        assert(channel.tryReserve(connection, 3) == std::nullopt);
        assert(channel.trackedConnectionCount() == 0);

        assert(channel.canEverReserve(2));
        assert(channel.tryReserve(connection, 2));
    }

    void test_admission_failures_collapse_and_overflow_uses_the_fail_safe()
    {
        const auto wake = make_wake_descriptor();
        OutboundChannel channel{OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1, .max_pending_admission_failures = 2}, wake.getDescriptor()};

        channel.reportAdmissionFailure(connection_of(4));
        channel.reportAdmissionFailure(connection_of(4));
        channel.reportAdmissionFailure(connection_of(4));
        channel.reportAdmissionFailure(connection_of(5));
        assert(channel.pendingAdmissionFailureCount() == 2);

        std::vector<snf::net::ConnectionId> failures;
        assert(!channel.takePendingAdmissionFailures(failures));
        assert(failures.size() == 2);
        assert(channel.pendingAdmissionFailureCount() == 0);

        failures.clear();
        assert(!channel.takePendingAdmissionFailures(failures));
        assert(failures.empty());

        channel.reportAdmissionFailure(connection_of(4));
        channel.reportAdmissionFailure(connection_of(5));
        channel.reportAdmissionFailure(connection_of(6));

        assert(channel.pendingAdmissionFailureCount() == 2);
        failures.clear();
        assert(channel.takePendingAdmissionFailures(failures));
        assert(failures.size() == 2);

        failures.clear();
        assert(!channel.takePendingAdmissionFailures(failures));
        assert(failures.empty());
    }

    void test_rejects_an_unsatisfiable_configuration()
    {
        const auto wake = make_wake_descriptor();
        bool rejected = false;
        try
        {
            OutboundChannel channel{OutboundChannelConfig{.capacity = 2, .max_slots_per_connection = 4}, wake.getDescriptor()};
            static_cast<void>(channel.capacity());
        }
        catch (const std::invalid_argument&)
        {
            rejected = true;
        }

        assert(rejected);
    }
}

void run_outbound_channel_tests()
{
    test_reserve_then_commit_moves_capacity_from_reserved_to_queued();
    test_zero_slot_reservation_is_valid_and_consumes_no_capacity();
    test_uncommitted_slots_return_when_the_reservation_dies();
    test_a_tracked_connection_keeps_its_accounting_between_commands();
    test_an_untracked_connection_leaves_no_accounting_behind();
    test_an_unsatisfiable_request_is_refused_without_throwing();
    test_admission_failures_collapse_and_overflow_uses_the_fail_safe();
    test_reserve_defers_to_a_registered_waiter();
    test_per_connection_limit_does_not_block_another_connection();
    test_global_capacity_blocks_a_waiter_until_a_pop_frees_a_slot();
    test_grant_pass_is_bounded_per_turn();
    test_award_that_lost_the_terminal_claim_returns_its_slots();
    test_withdrawn_waiter_is_never_granted();
    test_cancel_releases_every_waiter_with_an_invalid_reservation();
    test_registering_on_a_cancelled_channel_completes_the_producer();
    test_rejects_an_unsatisfiable_configuration();
}
