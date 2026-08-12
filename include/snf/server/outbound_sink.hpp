#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/runtime/async_operation.hpp"
#include "snf/server/outbound_action.hpp"
#include "snf/server/outbound_channel.hpp"

#include <cstddef>
#include <optional>

namespace snf::server
{
    // The outbound capacity protocol as domain code sees it. Actor bindings reserve
    // before they emit, so a saturated backend suspends one actor instead of blocking
    // the Worker that owns it. Which backend holds the queue, and how that backend is
    // awakened, stays behind this boundary.
    class OutboundSink
    {
    public:
        virtual ~OutboundSink() = default;

        // Non-blocking. std::nullopt means capacity must be awaited through
        // registerWaiter; it is not an invitation to retry in a loop.
        [[nodiscard]] virtual std::optional<OutboundReservation>
        tryReserve(snf::net::ConnectionId connection, std::size_t slots) = 0;

        // The producer is completed with a valid reservation once capacity is
        // granted, and with an invalid one if the backend is cancelled first. Either
        // way the terminal outcome the awaiting Worker is owed always arrives.
        [[nodiscard]] virtual ReservationTicket
        registerWaiter(snf::net::ConnectionId connection,
                       std::size_t slots,
                       snf::runtime::AsyncOperationProducer<OutboundReservation> producer) = 0;

        // Owning Worker only, after it has claimed the operation cancelled. Withdrawing
        // is what keeps a cancelled await from leaving a waiter behind.
        virtual void withdrawWaiter(const ReservationTicket& ticket) noexcept = 0;

        // Consumes one reserved slot. false means the backend was cancelled; a
        // reserved slot is otherwise guaranteed, so this never fails for capacity.
        [[nodiscard]] virtual bool commit(OutboundReservation& reservation,
                                          OutboundAction action) = 0;

        // Records a connection whose emission could not even be admitted, so the
        // backend closes it rather than dropping its response silently.
        virtual void reportAdmissionFailure(snf::net::ConnectionId connection) noexcept = 0;
    };

    // Current adapter over the reactor's outbound channel. A second network backend
    // supplies its own wake-up descriptor to a channel of its own without the game
    // runtime seeing either.
    class ChannelOutboundSink final : public OutboundSink
    {
    public:
        explicit ChannelOutboundSink(OutboundChannel& channel) noexcept;

        [[nodiscard]] std::optional<OutboundReservation>
        tryReserve(snf::net::ConnectionId connection, std::size_t slots) override;

        [[nodiscard]] ReservationTicket
        registerWaiter(snf::net::ConnectionId connection,
                       std::size_t slots,
                       snf::runtime::AsyncOperationProducer<OutboundReservation> producer) override;

        void withdrawWaiter(const ReservationTicket& ticket) noexcept override;

        [[nodiscard]] bool commit(OutboundReservation& reservation, OutboundAction action) override;

        void reportAdmissionFailure(snf::net::ConnectionId connection) noexcept override;

    private:
        OutboundChannel& _channel;
    };
}
