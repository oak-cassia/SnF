#include "snf/server/zone_transition_channel.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <limits>
#include <stdexcept>
#include <unistd.h>
#include <utility>

namespace snf::server
{
    ZoneTransitionChannel::ZoneTransitionChannel(const std::size_t capacity,
                                                 const int wake_descriptor)
        : _capacity(capacity)
        , _wake_descriptor(wake_descriptor)
        , _slots(capacity)
    {
        if (_capacity == 0 || _wake_descriptor < 0)
        {
            throw std::invalid_argument{"Zone transition channel configuration is invalid"};
        }
        _reservations.reserve(_capacity);
    }

    std::optional<ZoneTransitionTicket>
    ZoneTransitionChannel::tryReserve(const ZoneHandoffId handoff)
    {
        std::lock_guard lock{_mutex};
        if (_cancelled || handoff.value == 0 || _reservations.size() == _capacity ||
            _next_ticket == std::numeric_limits<std::uint64_t>::max())
        {
            ++_reservations_rejected;
            return std::nullopt;
        }

        const ZoneTransitionTicket ticket{.value = _next_ticket++};
        _reservations.emplace(ticket.value, Reservation{.handoff = handoff});
        ++_reservations_created;
        _reservation_high_water_mark = std::max(_reservation_high_water_mark, _reservations.size());
        return ticket;
    }

    bool ZoneTransitionChannel::publish(const ZoneTransitionTicket ticket,
                                        ZoneHandoffCompletion completion) noexcept
    {
        try
        {
            {
                std::lock_guard lock{_mutex};
                const auto reservation = _reservations.find(ticket.value);
                if (_cancelled || !ticket.valid() || reservation == _reservations.end() ||
                    reservation->second.handoff != completion.handoff_id ||
                    reservation->second.queued || _queued == _capacity || _slots[_tail])
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

    std::optional<ZoneHandoffCompletion> ZoneTransitionChannel::tryPop()
    {
        std::lock_guard lock{_mutex};
        if (_queued == 0)
        {
            return std::nullopt;
        }

        std::optional<QueuedCompletion>& slot = _slots[_head];
        if (!slot)
        {
            throw std::logic_error{"Zone transition queue accounting diverged"};
        }
        QueuedCompletion queued = std::move(*slot);
        slot.reset();
        _head = (_head + 1) % _capacity;
        --_queued;
        ++_completions_consumed;

        const auto reservation = _reservations.find(queued.ticket.value);
        if (reservation == _reservations.end() || !reservation->second.queued)
        {
            throw std::logic_error{"Zone transition reservation accounting diverged"};
        }
        reservation->second.queued = false;
        if (reservation->second.release_when_consumed)
        {
            _reservations.erase(reservation);
        }
        _queue_wait_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - queued.posted_at));
        return std::move(queued.completion);
    }

    void ZoneTransitionChannel::release(const ZoneTransitionTicket ticket) noexcept
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

    void ZoneTransitionChannel::wakeIfPending() const noexcept
    {
        bool pending = false;
        try
        {
            std::lock_guard lock{_mutex};
            pending = _queued != 0;
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

    void ZoneTransitionChannel::cancel() noexcept
    {
        try
        {
            std::lock_guard lock{_mutex};
            for (std::optional<QueuedCompletion>& slot : _slots)
            {
                slot.reset();
            }
            _reservations.clear();
            _head = 0;
            _tail = 0;
            _queued = 0;
            _cancelled = true;
        }
        catch (...)
        {
        }
        signalWakeUp();
    }

    ZoneTransitionChannelStats ZoneTransitionChannel::stats() const
    {
        std::lock_guard lock{_mutex};
        return ZoneTransitionChannelStats{
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
            .cancelled = _cancelled,
        };
    }

    std::size_t ZoneTransitionChannel::capacity() const noexcept
    {
        return _capacity;
    }

    bool ZoneTransitionChannel::drained() const noexcept
    {
        try
        {
            std::lock_guard lock{_mutex};
            return _queued == 0 && _reservations.empty();
        }
        catch (...)
        {
            return false;
        }
    }

    void ZoneTransitionChannel::signalWakeUp() const noexcept
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
