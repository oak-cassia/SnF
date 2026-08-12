#include "outbound_reservation_test_support.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/outbound_sink.hpp"

#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <unistd.h>
#include <utility>
#include <variant>

namespace
{
    using snf::test::make_wake_descriptor;
    using RecordingEndpoint = snf::test::RecordingContinuationEndpoint;

    constexpr snf::net::ConnectionId CONNECTION{.descriptor = 42, .generation = 7};

    std::uint64_t read_wakeup_count(const int descriptor)
    {
        std::uint64_t wakeup_count = 0;
        assert(::read(descriptor, &wakeup_count, sizeof(wakeup_count)) ==
               static_cast<ssize_t>(sizeof(wakeup_count)));
        return wakeup_count;
    }

    snf::server::OutboundAction pong_action(const std::uint32_t request_id)
    {
        return snf::server::SendFrame{
            .connection = CONNECTION,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::Pong,
                    .request_id = request_id,
                    .payload = {},
                },
        };
    }

    void test_outbound_sink_hides_queue_and_wakeup()
    {
        const auto wake = make_wake_descriptor();
        snf::server::OutboundChannel channel{
            snf::server::OutboundChannelConfig{.capacity = 2, .max_slots_per_connection = 2},
            wake.getDescriptor()};
        snf::server::ChannelOutboundSink sink{channel};

        auto reservation = sink.tryReserve(CONNECTION, 1);
        assert(reservation);
        assert(sink.commit(*reservation, pong_action(9)));

        assert(read_wakeup_count(wake.getDescriptor()) == 1);
        const auto posted = channel.tryPop();
        assert(posted.has_value());
        // The channel stamps the commit instant; the game runtime never sees it.
        assert(posted->posted_at.time_since_epoch().count() != 0);
        const auto* send = std::get_if<snf::server::SendFrame>(&posted->action);
        assert(send != nullptr);
        assert(send->connection.generation == 7);
        assert(send->frame.request_id == 9);
    }

    void test_runtime_completion_is_authoritative_and_independent_from_outbound_capacity()
    {
        const auto outbound_wake = make_wake_descriptor();
        snf::server::OutboundChannel full_outbound{
            snf::server::OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1},
            outbound_wake.getDescriptor()};
        snf::server::ChannelOutboundSink sink{full_outbound};
        auto reservation = sink.tryReserve(CONNECTION, 1);
        assert(reservation);
        assert(sink.commit(*reservation, pong_action(1)));

        const auto event = make_wake_descriptor();
        constexpr std::uint64_t required =
            snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic);
        snf::runtime::RuntimeCompletionCoordinator completion{required, event.getDescriptor()};

        completion.notifyDrained(snf::runtime::RuntimeId::Logic);
        assert(completion.allRequiredRuntimesDrained());
        assert(!completion.anyRuntimeFailed());

        completion.notifyFailed(snf::runtime::RuntimeId::Logic);
        completion.notifyFailed(snf::runtime::RuntimeId::Logic);
        assert(completion.anyRuntimeFailed());
        assert(read_wakeup_count(event.getDescriptor()) == 3);
        assert(full_outbound.size() == 1);
    }

    void test_outbound_saturation_defers_instead_of_blocking()
    {
        const auto wake = make_wake_descriptor();
        snf::server::OutboundChannel channel{
            snf::server::OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1},
            wake.getDescriptor()};
        snf::server::ChannelOutboundSink sink{channel};
        const auto endpoint = std::make_shared<RecordingEndpoint>();

        auto queued = sink.tryReserve(CONNECTION, 1);
        assert(queued);
        assert(sink.commit(*queued, pong_action(1)));

        // A saturated channel refuses without waiting: nothing parks a Worker, and the
        // caller is left to await capacity through a waiter instead.
        assert(sink.tryReserve(CONNECTION, 1) == std::nullopt);

        snf::test::ReservationWaiter waiter{endpoint, 1};
        const auto ticket = sink.registerWaiter(CONNECTION, 1, std::move(waiter.producer));
        assert(ticket.valid());
        assert(waiter.isPending());

        assert(channel.tryPop().has_value());
        assert(channel.grantPending() == 1);
        assert(waiter.isCompleted());
        auto granted = waiter.state->takeResult();
        assert(granted.valid());
        assert(sink.commit(granted, pong_action(2)));
        assert(channel.size() == 1);
    }

    void test_cancelling_outbound_releases_waiters_so_a_worker_can_drain()
    {
        const auto wake = make_wake_descriptor();
        snf::server::OutboundChannel channel{
            snf::server::OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1},
            wake.getDescriptor()};
        snf::server::ChannelOutboundSink sink{channel};
        const auto endpoint = std::make_shared<RecordingEndpoint>();

        auto queued = sink.tryReserve(CONNECTION, 1);
        assert(queued);
        assert(sink.commit(*queued, pong_action(1)));

        snf::test::ReservationWaiter waiter{endpoint, 1};
        static_cast<void>(sink.registerWaiter(CONNECTION, 1, std::move(waiter.producer)));
        assert(waiter.isPending());

        // A stopped reactor can no longer grant. Cancelling is what turns that into a
        // terminal outcome instead of a Worker waiting forever.
        assert(channel.cancel() == 1);
        assert(waiter.isCompleted());
        assert(!waiter.state->takeResult().valid());
        assert(channel.pendingWaiterCount() == 0);
    }

    void test_rejects_invalid_boundary_configuration()
    {
        bool invalid_outbound_descriptor_rejected = false;
        try
        {
            [[maybe_unused]] snf::server::OutboundChannel channel{
                snf::server::OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1},
                -1};
        }
        catch (const std::invalid_argument&)
        {
            invalid_outbound_descriptor_rejected = true;
        }
        assert(invalid_outbound_descriptor_rejected);

        const auto event = make_wake_descriptor();
        bool empty_required_mask_rejected = false;
        try
        {
            [[maybe_unused]] snf::runtime::RuntimeCompletionCoordinator completion{
                0, event.getDescriptor()};
        }
        catch (const std::invalid_argument&)
        {
            empty_required_mask_rejected = true;
        }
        assert(empty_required_mask_rejected);
    }
}

void run_runtime_boundary_tests()
{
    test_outbound_sink_hides_queue_and_wakeup();
    test_runtime_completion_is_authoritative_and_independent_from_outbound_capacity();
    test_outbound_saturation_defers_instead_of_blocking();
    test_cancelling_outbound_releases_waiters_so_a_worker_can_drain();
    test_rejects_invalid_boundary_configuration();
}
