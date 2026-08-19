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
        // Bounds how much of the shared capacity one connection may hold, so a
        // single slow client cannot occupy all of it.
        std::size_t max_slots_per_connection{64};
        // Bounds both the waiters examined and the awards made in one grant pass,
        // for the same reason the pending lifecycle retry is bounded per turn: the
        // cost lands in the reactor's own turn latency.
        std::size_t max_grants_per_turn{64};
        // At least the sum of every Worker's in-flight operation budget. A Worker
        // reserves an in-flight slot before it registers, so registration is never
        // the binding constraint and a full registry means a broken invariant.
        std::size_t max_waiters{4096};
        // Covers both the backend's maximum concurrent connections and commands that
        // were already outstanding when their connection disappeared. Failures are
        // keyed by connection. Exceeding the bound uses a no-throw reactor fail-safe;
        // it never discards the condition on the Worker.
        std::size_t max_pending_admission_failures{4096};
    };

    // Owns outbound storage, its capacity accounting and the reactor wake-up behind
    // one synchronisation boundary. Keeping the reservation accounting in a separate
    // object would let queued and reserved drift apart in the window between a pop
    // and a commit.
    //
    // The reactor is the only granter. A Logic Worker that frees capacity signals
    // the wake-up rather than granting, which keeps grant work bounded per reactor
    // turn and keeps the ordering rules on one thread.
    //
    // It is the current OutboundSink, without an adapter in between. The interface is
    // what narrows the domain's view -- a binding holding OutboundSink& cannot drain or
    // grant -- so a forwarding class would add a layer and narrow nothing further. A
    // second backend implements the interface itself.
    class OutboundChannel final : public OutboundSink
    {
    public:
        OutboundChannel(const OutboundChannelConfig& config, int wake_descriptor);

        OutboundChannel(const OutboundChannel&) = delete;
        OutboundChannel& operator=(const OutboundChannel&) = delete;

        // Whether a request of this size could ever be granted. A request above the
        // per-connection limit is unsatisfiable no matter how much capacity frees, so a
        // caller has to distinguish it from saturation before it waits for a grant that
        // can never come. Const and lock-free.
        [[nodiscard]] bool canEverReserve(std::size_t slots) const noexcept override;

        // Non-blocking. std::nullopt means the caller must register a waiter or give
        // up; it never means "retry in a loop". A request that canEverReserve rejects
        // also returns std::nullopt, so callers check that first.
        //
        // While any waiter is registered this refuses even a satisfiable request, so
        // a late arrival cannot barge ahead of an actor that is already suspended.
        // Outside saturation there are no waiters and this is the only path taken,
        // which is why the common case starts no async operation at all.
        [[nodiscard]] std::optional<OutboundReservation> tryReserve(snf::net::ConnectionId connection, std::size_t slots) override;

        // The producer receives a valid reservation once capacity is granted, and an
        // invalid one if the channel is cancelled first. An invalid ticket means the
        // channel was already cancelled and the producer has been completed.
        [[nodiscard]] ReservationTicket registerWaiter(snf::net::ConnectionId connection, std::size_t slots, snf::runtime::AsyncOperationProducer<OutboundReservation> producer) override;

        // Owning Worker only, and only after it has claimed the operation cancelled:
        // withdrawing destroys the producer, so no completion will ever arrive for
        // that waiter. A ticket whose waiter was already granted withdraws nothing.
        void withdrawWaiter(const ReservationTicket& ticket) noexcept override;

        // Consumes one reserved slot. false means the channel was cancelled; a
        // reserved slot is otherwise guaranteed to exist, so this never fails for
        // capacity.
        [[nodiscard]] bool commit(OutboundReservation& reservation, OutboundAction action) override;

        // Reactor only.
        [[nodiscard]] std::optional<PostedOutboundAction> tryPop();
        // One lock for a whole batch. Draining item by item multiplies lock traffic
        // with the committing Workers exactly when a burst makes that contention
        // expensive. Actions are appended, so the caller controls the buffer.
        void drainInto(std::vector<PostedOutboundAction>& actions, std::size_t max_actions);
        std::size_t grantPending();
        // Reactor only, when a connection is accepted. A connection's accounting
        // outlives its individual commands, so the entry is created here rather than per
        // command: allocating a map node inside the lock on every command lengthens the
        // critical section for everyone.
        void trackConnection(snf::net::ConnectionId connection);
        // Reactor only, when a connection is gone. The entry is dropped once whatever it
        // still holds has drained. An entry created for a connection the backend never
        // tracked -- a late command for a session that is already closed -- is dropped
        // as soon as it is idle, because no forgetConnection will ever arrive for it.
        void forgetConnection(snf::net::ConnectionId connection);
        // Moves the recorded connections to the caller. true means the fixed record
        // budget was exceeded (or recording allocated unsuccessfully), so the reactor
        // must apply the fail-safe policy to every current session: the one record that
        // could not be retained must never turn into a silently missing response.
        [[nodiscard]] bool takePendingAdmissionFailures(std::vector<snf::net::ConnectionId>& failures);

        // Callable from any thread. Records a connection whose outbound admission
        // failed before any capacity was reserved, so the reactor can close it under
        // the same overflow policy the inbound path uses instead of dropping the
        // response silently.
        //
        // Keyed by connection, so repeated reports for one connection collapse and a
        // single connection cannot crowd out other connections' closes. This is a Worker
        // failure path and must not throw. If the record budget is nevertheless exceeded,
        // a preallocated flag asks the reactor to close all current sessions rather than
        // dropping this close or failing the Worker.
        void reportAdmissionFailure(snf::net::ConnectionId connection) noexcept override;

        // Abandons queued actions and releases every waiter with the cancelled
        // outcome. Required whenever the reactor stops consuming, which would
        // otherwise leave a Logic Worker waiting for a grant that cannot arrive.
        std::size_t cancel();

        [[nodiscard]] std::size_t size() const;
        [[nodiscard]] std::size_t capacity() const noexcept;
        [[nodiscard]] std::size_t highWaterMark() const;
        [[nodiscard]] std::size_t reservedSlotCount() const;
        [[nodiscard]] std::size_t pendingWaiterCount() const;
        // Connections with live accounting. Entries survive between a connection's
        // commands, so this is the gauge that shows the bound holding: it must track
        // live connections and not the command rate.
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
            // Kept in step with waiters being non-empty, so the grant order holds at
            // most one entry per connection.
            bool queued_for_grant{false};
            // A tracked connection keeps its entry through an idle moment, because its
            // next command would only have to allocate it again. Everything else --
            // a connection the backend has dropped, or one it never tracked because the
            // command arrived after the session closed -- is dropped as soon as it holds
            // nothing. Defaulting to true is what keeps connection churn from
            // accumulating entries nobody will ever forget.
            bool erase_when_idle{true};
        };

        // The connection travels with the action so a pop does not have to inspect
        // the variant to find whose accounting to release.
        struct QueuedAction
        {
            PostedOutboundAction posted;
            snf::net::ConnectionId connection{};
        };

        // One award, moved out of the lock before it is published: a completion that
        // loses its terminal claim destroys the reservation inline, and that
        // destructor takes this same mutex.
        struct Award
        {
            snf::runtime::AsyncOperationProducer<OutboundReservation> producer;
            OutboundReservation reservation;
        };

        // Called by OutboundReservation's destructor, from any thread.
        void returnSlots(snf::net::ConnectionId connection, std::size_t slots) noexcept;

        // Caller holds the lock.
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
