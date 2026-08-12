#pragma once

#include "snf/net/connection_id.hpp"

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace snf::server
{
    class OutboundChannel;

    // A granted slice of outbound capacity. Move-only, and the destructor returns
    // whatever was never committed. That is what makes every abandoned path -- an
    // award that lost the terminal claim to a cancellation, a destroyed coroutine
    // frame, a handler emitting fewer actions than it reserved -- return capacity
    // without the channel having to observe that path at all.
    //
    // An invalid reservation is the cancelled outcome. A waiter released by cancel()
    // receives one instead of an exception, so an awaiting handler has a single
    // shape to interpret.
    //
    // It lives apart from the channel that mints it so the domain-facing headers can
    // name a reservation without pulling in the channel's storage and locking.
    class OutboundReservation final
    {
    public:
        OutboundReservation() noexcept = default;
        ~OutboundReservation();

        OutboundReservation(const OutboundReservation&) = delete;
        OutboundReservation& operator=(const OutboundReservation&) = delete;
        OutboundReservation(OutboundReservation&& other) noexcept;
        OutboundReservation& operator=(OutboundReservation&& other) noexcept;

        // Zero remaining slots is still valid: a fully committed reservation and a
        // reservation taken for a command with no effect are both legal.
        [[nodiscard]] bool valid() const noexcept;
        [[nodiscard]] std::size_t remainingSlots() const noexcept;
        [[nodiscard]] snf::net::ConnectionId connection() const noexcept;

    private:
        OutboundReservation(OutboundChannel& channel,
                            snf::net::ConnectionId connection,
                            std::size_t slots) noexcept;

        void returnRemainingSlots() noexcept;

        OutboundChannel* _channel{nullptr};
        snf::net::ConnectionId _connection{};
        std::size_t _slots{0};

        friend class OutboundChannel;
    };

    static_assert(std::is_nothrow_move_constructible_v<OutboundReservation>,
                  "A reservation is an async operation result, so a producer stores it inside a "
                  "noexcept window that a waiting Worker depends on.");

    // Identifies one registered waiter so its owning Worker can withdraw it. The
    // value is unique for the channel's lifetime, so a ticket whose waiter was
    // already granted cannot withdraw a later waiter for the same connection.
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
