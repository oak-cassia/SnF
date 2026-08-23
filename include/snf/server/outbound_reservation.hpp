#pragma once

#include "snf/net/connection_id.hpp"

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace snf::server
{
    class OutboundChannel;

    class OutboundReservation final
    {
    public:
        OutboundReservation() noexcept = default;
        ~OutboundReservation();

        OutboundReservation(const OutboundReservation&) = delete;
        OutboundReservation& operator=(const OutboundReservation&) = delete;
        OutboundReservation(OutboundReservation&& other) noexcept;
        OutboundReservation& operator=(OutboundReservation&& other) noexcept;

        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] std::size_t remainingSlots() const noexcept;
        [[nodiscard]] snf::net::ConnectionId connection() const noexcept;

    private:
        OutboundReservation(OutboundChannel& channel, snf::net::ConnectionId connection, std::size_t slots) noexcept;

        void returnRemainingSlots() noexcept;

        OutboundChannel* _channel{nullptr};
        snf::net::ConnectionId _connection{};
        std::size_t _slots{0};

        friend class OutboundChannel;
    };

    static_assert(std::is_nothrow_move_constructible_v<OutboundReservation>,
                  "A reservation is an async operation result, so a producer stores it inside a "
                  "noexcept window that a waiting Worker depends on.");

    struct ReservationTicket
    {
        std::uint64_t value{0};
        snf::net::ConnectionId connection{};

        [[nodiscard]] bool valid() const noexcept
        {
            return value != 0;
        }
    };
}
