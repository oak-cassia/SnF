#include "snf/server/outbound_sink.hpp"

#include <utility>

namespace snf::server
{
    ChannelOutboundSink::ChannelOutboundSink(OutboundChannel& channel) noexcept
        : _channel(channel)
    {
    }

    bool ChannelOutboundSink::canEverReserve(const std::size_t slots) const noexcept
    {
        return _channel.canEverReserve(slots);
    }

    std::optional<OutboundReservation>
    ChannelOutboundSink::tryReserve(const snf::net::ConnectionId connection,
                                    const std::size_t slots)
    {
        return _channel.tryReserve(connection, slots);
    }

    ReservationTicket ChannelOutboundSink::registerWaiter(
        const snf::net::ConnectionId connection,
        const std::size_t slots,
        snf::runtime::AsyncOperationProducer<OutboundReservation> producer)
    {
        return _channel.registerWaiter(connection, slots, std::move(producer));
    }

    void ChannelOutboundSink::withdrawWaiter(const ReservationTicket& ticket) noexcept
    {
        _channel.withdrawWaiter(ticket);
    }

    bool ChannelOutboundSink::commit(OutboundReservation& reservation, OutboundAction action)
    {
        return _channel.commit(reservation, std::move(action));
    }

    void
    ChannelOutboundSink::reportAdmissionFailure(const snf::net::ConnectionId connection) noexcept
    {
        _channel.reportAdmissionFailure(connection);
    }
}
