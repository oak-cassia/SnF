#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/runtime/async_operation.hpp"
#include "snf/server/outbound_action.hpp"
#include "snf/server/outbound_reservation.hpp"
#include "snf/server/outbound_sink.hpp"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace snf::server
{
    struct OutboundChannelConfig
    {
        std::size_t capacity{4096};
        std::size_t max_slots_per_connection{64};
        std::size_t max_grants_per_turn{64};
        std::size_t max_waiters{4096};
        std::size_t max_pending_admission_failures{4096};
    };

    class OutboundChannel final : public OutboundSink
    {
    public:
        OutboundChannel(const OutboundChannelConfig& config, int wake_descriptor);

        OutboundChannel(const OutboundChannel&) = delete;
        OutboundChannel& operator=(const OutboundChannel&) = delete;

        [[nodiscard]] bool canEverReserve(std::size_t slots) const noexcept override;

        [[nodiscard]] std::optional<OutboundReservation> tryReserve(snf::net::ConnectionId connection, std::size_t slots) override;

        [[nodiscard]] ReservationTicket registerWaiter(snf::net::ConnectionId connection, std::size_t slots, snf::runtime::AsyncOperationProducer<OutboundReservation> producer) override;

        void withdrawWaiter(const ReservationTicket& ticket) noexcept override;

        [[nodiscard]] bool commit(OutboundReservation& reservation, OutboundAction action) override;

        [[nodiscard]] std::optional<PostedOutboundAction> tryPop();
        void drainInto(std::vector<PostedOutboundAction>& actions, std::size_t max_actions);
        std::size_t grantPending();
        void trackConnection(snf::net::ConnectionId connection);
        void forgetConnection(snf::net::ConnectionId connection);
        [[nodiscard]] bool takePendingAdmissionFailures(std::vector<snf::net::ConnectionId>& failures);

        void reportAdmissionFailure(snf::net::ConnectionId connection) noexcept override;

        std::size_t cancel();

        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] std::size_t capacity() const noexcept;
        [[nodiscard]] std::size_t highWaterMark() const;
        [[nodiscard]] std::size_t reservedSlotCount() const;
        [[nodiscard]] std::size_t pendingWaiterCount() const;
        [[nodiscard]] std::size_t trackedConnectionCount() const;
        [[nodiscard]] std::size_t pendingAdmissionFailureCount() const;
        [[nodiscard]] bool isCancelled() const;

    private:
        struct Waiter
        {
            std::uint64_t ticket{0};
            std::size_t slots{0};
            snf::runtime::AsyncOperationProducer<OutboundReservation> producer;
        };

        struct ConnectionUsage
        {
            std::size_t queued{0};
            std::size_t reserved{0};
            std::deque<Waiter> waiters;
            bool queued_for_grant{false};
            bool erase_when_idle{true};
        };

        struct QueuedAction
        {
            PostedOutboundAction posted;
            snf::net::ConnectionId connection{};
        };

        struct Award
        {
            snf::runtime::AsyncOperationProducer<OutboundReservation> producer;
            OutboundReservation reservation;
        };

        void returnSlots(snf::net::ConnectionId connection, std::size_t slots) noexcept;

        [[nodiscard]] std::optional<PostedOutboundAction> takeFront();
        [[nodiscard]] bool fits(const ConnectionUsage& usage, std::size_t slots) const;
        void markGrantable(snf::net::ConnectionId connection, ConnectionUsage& usage);
        void eraseUsageIfIdle(snf::net::ConnectionId connection);
        void signalWakeUp() const noexcept;

        const std::size_t _capacity;
        const std::size_t _max_slots_per_connection;
        const std::size_t _max_grants_per_turn;
        const std::size_t _max_waiters;
        const std::size_t _max_pending_admission_failures;
        const int _wake_descriptor;

        mutable std::mutex _mutex;
        std::deque<QueuedAction> _items;
        std::unordered_map<snf::net::ConnectionId, ConnectionUsage, snf::net::ConnectionIdHash> _connections;
        std::deque<snf::net::ConnectionId> _grant_order;
        std::unordered_set<snf::net::ConnectionId, snf::net::ConnectionIdHash> _admission_failures;
        std::size_t _reserved_slots{0};
        std::size_t _waiter_count{0};
        std::size_t _high_water_mark{0};
        std::uint64_t _next_ticket{1};
        bool _admission_failure_overflowed{false};
        bool _cancelled{false};

        friend class OutboundReservation;
    };
}
