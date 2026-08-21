#include "snf/server/room_transition_channel.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <unistd.h>
#include <utility>

namespace snf::server
{
    RoomTransitionChannel::RoomTransitionChannel(const std::size_t capacity, const int wake_descriptor)
        : _capacity(capacity)
        , _wake_descriptor(wake_descriptor)
        , _slots(capacity)
        , _return_requests(capacity)
    {
        if (_capacity == 0 || _wake_descriptor < 0)
        {
            throw std::invalid_argument{"Room transition channel configuration is invalid"};
        }
        _reservations.reserve(_capacity);
    }

    std::optional<RoomTransitionTicket> RoomTransitionChannel::tryReserve(const RoomEntryId entry)
    {
        std::lock_guard lock{_mutex};
        if (_cancelled || entry.value == 0 || _reservations.size() == _capacity || _next_ticket == std::numeric_limits<std::uint64_t>::max())
        {
            ++_reservations_rejected;
            return std::nullopt;
        }

        const RoomTransitionTicket ticket{.value = _next_ticket++};
        _reservations.emplace(ticket.value, Reservation{.correlation_id = entry.value});
        ++_reservations_created;
        _reservation_high_water_mark = std::max(_reservation_high_water_mark, _reservations.size());
        return ticket;
    }

    std::optional<RoomTransitionTicket> RoomTransitionChannel::tryReserve(const RoomReturnId return_id)
    {
        std::lock_guard lock{_mutex};
        if (_cancelled || return_id.value == 0 || _reservations.size() == _capacity || _next_ticket == std::numeric_limits<std::uint64_t>::max())
        {
            ++_reservations_rejected;
            return std::nullopt;
        }

        const RoomTransitionTicket ticket{.value = _next_ticket++};
        _reservations.emplace(ticket.value, Reservation{.correlation_id = return_id.value});
        ++_reservations_created;
        _reservation_high_water_mark = std::max(_reservation_high_water_mark, _reservations.size());
        return ticket;
    }

    bool RoomTransitionChannel::publish(const RoomTransitionTicket ticket, RoomTransitionCompletion completion) noexcept
    {
        try
        {
            {
                std::lock_guard lock{_mutex};
                const auto reservation = _reservations.find(ticket.value);
                const std::uint64_t expected_id = completion.return_id.valid() ? completion.return_id.value : completion.entry_id.value;
                if (_cancelled || !ticket.valid() || reservation == _reservations.end() || reservation->second.correlation_id != expected_id || reservation->second.queued || _queued == _capacity ||
                    _slots[_tail])
                {
                    ++_invalid_publishes;
                    return false;
                }

                _slots[_tail].emplace(QueuedCompletion{
                    .ticket = ticket,
                    .completion = std::move(completion),
                    .posted_at = std::chrono::steady_clock::now(),
                });
                _tail = (_tail + 1) % _capacity;
                ++_queued;
                reservation->second.queued = true;
                ++_completions_published;
                _queue_high_water_mark = std::max(_queue_high_water_mark, _queued);
            }
            signalWakeUp();
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    std::optional<RoomTransitionCompletion> RoomTransitionChannel::tryPop()
    {
        std::lock_guard lock{_mutex};
        if (_queued == 0)
        {
            return std::nullopt;
        }

        std::optional<QueuedCompletion>& slot = _slots[_head];
        if (!slot)
        {
            throw std::logic_error{"Room transition queue accounting diverged"};
        }
        QueuedCompletion queued = std::move(*slot);
        slot.reset();
        _head = (_head + 1) % _capacity;
        --_queued;
        ++_completions_consumed;

        const auto reservation = _reservations.find(queued.ticket.value);
        if (reservation == _reservations.end() || !reservation->second.queued)
        {
            throw std::logic_error{"Room transition reservation accounting diverged"};
        }
        reservation->second.queued = false;
        if (reservation->second.release_when_consumed)
        {
            _reservations.erase(reservation);
        }
        _queue_wait_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - queued.posted_at));
        return std::move(queued.completion);
    }

    bool RoomTransitionChannel::tryPublishReturnRequest(RoomReturnRequest request) noexcept
    {
        try
        {
            {
                std::lock_guard lock{_mutex};
                if (_cancelled || request.room.value == 0 || request.player.value == 0 || _return_queued == _capacity || _return_requests[_return_tail])
                {
                    ++_return_requests_rejected;
                    return false;
                }

                _return_requests[_return_tail].emplace(request);
                _return_tail = (_return_tail + 1) % _capacity;
                ++_return_queued;
                ++_return_requests_published;
                _return_request_high_water_mark = std::max(_return_request_high_water_mark, _return_queued);
            }
            signalWakeUp();
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    std::optional<RoomReturnRequest> RoomTransitionChannel::tryPopReturnRequest()
    {
        std::lock_guard lock{_mutex};
        if (_return_queued == 0)
        {
            return std::nullopt;
        }

        std::optional<RoomReturnRequest>& slot = _return_requests[_return_head];
        if (!slot)
        {
            throw std::logic_error{"Room return request queue accounting diverged"};
        }
        const RoomReturnRequest request = *slot;
        slot.reset();
        _return_head = (_return_head + 1) % _capacity;
        --_return_queued;
        ++_return_requests_consumed;
        return request;
    }

    void RoomTransitionChannel::release(const RoomTransitionTicket ticket) noexcept
    {
        try
        {
            std::lock_guard lock{_mutex};
            const auto reservation = _reservations.find(ticket.value);
            if (reservation == _reservations.end())
            {
                return;
            }
            if (reservation->second.queued)
            {
                reservation->second.release_when_consumed = true;
            }
            else
            {
                _reservations.erase(reservation);
            }
        }
        catch (...)
        {
        }
    }

    void RoomTransitionChannel::wakeIfPending() const noexcept
    {
        bool pending = false;
        try
        {
            std::lock_guard lock{_mutex};
            pending = _queued != 0 || _return_queued != 0;
        }
        catch (...)
        {
            return;
        }
        if (pending)
        {
            signalWakeUp();
        }
    }

    void RoomTransitionChannel::cancel() noexcept
    {
        try
        {
            std::lock_guard lock{_mutex};
            for (std::optional<QueuedCompletion>& slot : _slots)
            {
                slot.reset();
            }
            for (std::optional<RoomReturnRequest>& slot : _return_requests)
            {
                slot.reset();
            }
            _reservations.clear();
            _head = 0;
            _tail = 0;
            _queued = 0;
            _return_head = 0;
            _return_tail = 0;
            _return_queued = 0;
            _cancelled = true;
        }
        catch (...)
        {
        }
        signalWakeUp();
    }

    RoomTransitionChannelStats RoomTransitionChannel::stats() const
    {
        std::lock_guard lock{_mutex};
        return RoomTransitionChannelStats{
            .queue_wait_nanoseconds = _queue_wait_nanoseconds.snapshot(),
            .reservations_created = _reservations_created,
            .reservations_rejected = _reservations_rejected,
            .completions_published = _completions_published,
            .completions_consumed = _completions_consumed,
            .invalid_publishes = _invalid_publishes,
            .reservations = _reservations.size(),
            .queued = _queued,
            .reservation_high_water_mark = _reservation_high_water_mark,
            .queue_high_water_mark = _queue_high_water_mark,
            .return_requests_published = _return_requests_published,
            .return_requests_consumed = _return_requests_consumed,
            .return_requests_rejected = _return_requests_rejected,
            .return_requests_queued = _return_queued,
            .return_request_high_water_mark = _return_request_high_water_mark,
            .cancelled = _cancelled,
        };
    }

    std::size_t RoomTransitionChannel::capacity() const noexcept
    {
        return _capacity;
    }

    bool RoomTransitionChannel::drained() const noexcept
    {
        try
        {
            std::lock_guard lock{_mutex};
            return _queued == 0 && _return_queued == 0 && _reservations.empty();
        }
        catch (...)
        {
            return false;
        }
    }

    void RoomTransitionChannel::signalWakeUp() const noexcept
    {
        constexpr std::uint64_t wakeup_value = 1;
        while (::write(_wake_descriptor, &wakeup_value, sizeof(wakeup_value)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return;
        }
    }
}
