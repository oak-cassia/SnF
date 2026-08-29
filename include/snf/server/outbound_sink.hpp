#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/runtime/async_operation.hpp"
#include "snf/server/outbound_action.hpp"
#include "snf/server/outbound_reservation.hpp"

#include <cstddef>
#include <optional>
#include <utility>

namespace snf::server
{
    class OutboundSink
    {
    public:
        virtual ~OutboundSink() = default;

        [[nodiscard]] virtual bool canEverReserve(std::size_t slots) const noexcept = 0;

        [[nodiscard]] virtual std::optional<OutboundReservation> tryReserve(snf::net::ConnectionId connection, std::size_t slots) = 0;

        [[nodiscard]] virtual ReservationTicket registerWaiter(
            snf::net::ConnectionId connection,
            std::size_t slots,
            snf::runtime::AsyncOperationProducer<OutboundReservation> producer
        ) = 0;

        virtual void withdrawWaiter(const ReservationTicket& ticket) noexcept = 0;

        [[nodiscard]] virtual bool commit(OutboundReservation& reservation, OutboundAction action) = 0;

        virtual void reportAdmissionFailure(snf::net::ConnectionId connection) noexcept = 0;
    };

    [[nodiscard]] inline bool send_one_frame(OutboundSink& outbound, const snf::net::ConnectionId connection, snf::protocol::Frame frame)
    {
        auto reservation = outbound.tryReserve(connection, 1);
        if (!reservation)
        {
            outbound.reportAdmissionFailure(connection);
            return false;
        }

        if (!outbound.commit(
                *reservation,
                SendFrame{
                    .connection = connection,
                    .frame = std::move(frame),
                }
            ))
        {
            outbound.reportAdmissionFailure(connection);
            return false;
        }

        return true;
    }
}
