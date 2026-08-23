#include "snf/server/outbound_channel.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <stdexcept>
#include <unistd.h>
#include <utility>

namespace snf::server
{
    OutboundReservation::OutboundReservation(OutboundChannel& channel, const snf::net::ConnectionId connection, const std::size_t slots) noexcept
        : _channel(&channel)
        , _connection(connection)
        , _slots(slots)
    {
    }

    OutboundReservation::~OutboundReservation()
    {
        returnRemainingSlots();
    }

    OutboundReservation::OutboundReservation(OutboundReservation&& other) noexcept
        : _channel(std::exchange(other._channel, nullptr))
        , _connection(other._connection)
        , _slots(std::exchange(other._slots, 0))
    {
    }

    OutboundReservation& OutboundReservation::operator=(OutboundReservation&& other) noexcept
    {
        if (this != &other)
        {
            returnRemainingSlots();
            _channel = std::exchange(other._channel, nullptr);
            _connection = other._connection;
            _slots = std::exchange(other._slots, 0);
        }

        return *this;
    }

    bool OutboundReservation::valid() const noexcept
    {
        return _channel != nullptr;
    }

    std::size_t OutboundReservation::remainingSlots() const noexcept
    {
        return _slots;
    }

    snf::net::ConnectionId OutboundReservation::connection() const noexcept
    {
        return _connection;
    }

    void OutboundReservation::returnRemainingSlots() noexcept
    {
        if (_channel != nullptr && _slots != 0)
        {
            _channel->returnSlots(_connection, _slots);
        }

        _channel = nullptr;
        _slots = 0;
    }

    OutboundChannel::OutboundChannel(const OutboundChannelConfig& config, const int wake_descriptor)
        : _capacity(config.capacity)
        , _max_slots_per_connection(config.max_slots_per_connection)
        , _max_grants_per_turn(config.max_grants_per_turn)
        , _max_waiters(config.max_waiters)
        , _max_pending_admission_failures(config.max_pending_admission_failures)
        , _wake_descriptor(wake_descriptor)
    {
        if (_capacity == 0 || _max_grants_per_turn == 0 || _max_waiters == 0 || _max_pending_admission_failures == 0 || _wake_descriptor < 0)
        {
            throw std::invalid_argument{"Invalid outbound channel configuration"};
        }

        if (_max_slots_per_connection == 0 || _max_slots_per_connection > _capacity)
        {
            throw std::invalid_argument{"Outbound per-connection limit must fit within the shared capacity"};
        }
    }

    bool OutboundChannel::canEverReserve(const std::size_t slots) const noexcept
    {
        return slots <= _max_slots_per_connection;
    }

    std::optional<OutboundReservation> OutboundChannel::tryReserve(const snf::net::ConnectionId connection, const std::size_t slots)
    {
        if (!canEverReserve(slots))
        {
            return std::nullopt;
        }

        std::lock_guard lock{_mutex};
        if (_cancelled)
        {
            return std::nullopt;
        }

        if (slots == 0)
        {
            return OutboundReservation{*this, connection, 0};
        }

        if (_waiter_count != 0)
        {
            return std::nullopt;
        }

        ConnectionUsage& usage = _connections[connection];
        if (!fits(usage, slots))
        {
            eraseUsageIfIdle(connection);
            return std::nullopt;
        }

        usage.reserved += slots;
        _reserved_slots += slots;
        return OutboundReservation{*this, connection, slots};
    }

    ReservationTicket OutboundChannel::registerWaiter(const snf::net::ConnectionId connection, const std::size_t slots, snf::runtime::AsyncOperationProducer<OutboundReservation> producer)
    {
        if (slots == 0 || !canEverReserve(slots))
        {
            throw std::logic_error{"Outbound waiter slot count is outside the reservable range"};
        }

        ReservationTicket ticket{};
        {
            std::lock_guard lock{_mutex};
            if (!_cancelled)
            {
                if (_waiter_count == _max_waiters)
                {
                    throw std::logic_error{"Outbound reservation waiter registry is full"};
                }

                ConnectionUsage& usage = _connections[connection];
                ticket = ReservationTicket{.value = _next_ticket++, .connection = connection};
                usage.waiters.push_back(Waiter{
                    .ticket = ticket.value,
                    .slots = slots,
                    .producer = std::move(producer),
                });
                ++_waiter_count;
                markGrantable(connection, usage);
            }
        }

        if (!ticket.valid())
        {
            producer.complete(OutboundReservation{});
            return ticket;
        }

        signalWakeUp();
        return ticket;
    }

    void OutboundChannel::withdrawWaiter(const ReservationTicket& ticket) noexcept
    {
        if (!ticket.valid())
        {
            return;
        }

        try
        {
            std::lock_guard lock{_mutex};
            const auto usage_iterator = _connections.find(ticket.connection);
            if (usage_iterator == _connections.end())
            {
                return;
            }

            std::deque<Waiter>& waiters = usage_iterator->second.waiters;
            const auto waiter_iterator = std::find_if(waiters.begin(), waiters.end(), [&ticket](const Waiter& waiter) { return waiter.ticket == ticket.value; });
            if (waiter_iterator == waiters.end())
            {
                return;
            }

            waiters.erase(waiter_iterator);
            --_waiter_count;
            eraseUsageIfIdle(ticket.connection);
        }
        catch (...)
        {
        }
    }

    bool OutboundChannel::commit(OutboundReservation& reservation, OutboundAction action)
    {
        if (reservation._channel != this || reservation._slots == 0)
        {
            throw std::logic_error{"Outbound commit without a reserved slot"};
        }

        {
            std::lock_guard lock{_mutex};
            if (_cancelled)
            {
                return false;
            }

            const auto usage_iterator = _connections.find(reservation._connection);
            if (usage_iterator == _connections.end() || usage_iterator->second.reserved == 0)
            {
                throw std::logic_error{"Outbound commit lost its per-connection reservation"};
            }

            _items.push_back(QueuedAction{
                .posted =
                    PostedOutboundAction{
                        .action = std::move(action),
                        .posted_at = std::chrono::steady_clock::now(),
                    },
                .connection = reservation._connection,
            });

            ConnectionUsage& usage = usage_iterator->second;
            --usage.reserved;
            ++usage.queued;
            --_reserved_slots;
            --reservation._slots;

            if (_items.size() > _high_water_mark)
            {
                _high_water_mark = _items.size();
            }
        }

        signalWakeUp();
        return true;
    }

    std::optional<PostedOutboundAction> OutboundChannel::tryPop()
    {
        std::lock_guard lock{_mutex};
        return takeFront();
    }

    void OutboundChannel::drainInto(std::vector<PostedOutboundAction>& actions, const std::size_t max_actions)
    {
        std::lock_guard lock{_mutex};
        for (std::size_t drained = 0; drained < max_actions; ++drained)
        {
            auto posted = takeFront();
            if (!posted)
            {
                return;
            }

            actions.push_back(std::move(*posted));
        }
    }

    void OutboundChannel::trackConnection(const snf::net::ConnectionId connection)
    {
        std::lock_guard lock{_mutex};
        _connections[connection].erase_when_idle = false;
    }

    void OutboundChannel::forgetConnection(const snf::net::ConnectionId connection)
    {
        std::lock_guard lock{_mutex};
        const auto usage_iterator = _connections.find(connection);
        if (usage_iterator == _connections.end())
        {
            return;
        }

        usage_iterator->second.erase_when_idle = true;
        eraseUsageIfIdle(connection);
    }

    std::size_t OutboundChannel::grantPending()
    {
        std::vector<Award> awards;
        awards.reserve(_max_grants_per_turn);

        {
            std::lock_guard lock{_mutex};
            if (_cancelled)
            {
                return 0;
            }

            std::size_t remaining_rotations = _grant_order.size();
            std::size_t examined = 0;
            while (awards.size() < _max_grants_per_turn && examined < _max_grants_per_turn && remaining_rotations != 0 && !_grant_order.empty())
            {
                --remaining_rotations;
                ++examined;
                const snf::net::ConnectionId connection = _grant_order.front();
                _grant_order.pop_front();

                const auto usage_iterator = _connections.find(connection);
                if (usage_iterator == _connections.end())
                {
                    continue;
                }

                ConnectionUsage& usage = usage_iterator->second;
                usage.queued_for_grant = false;
                if (usage.waiters.empty())
                {
                    eraseUsageIfIdle(connection);
                    continue;
                }

                Waiter& head = usage.waiters.front();
                if (!fits(usage, head.slots))
                {
                    usage.queued_for_grant = true;
                    _grant_order.push_back(connection);
                    continue;
                }

                usage.reserved += head.slots;
                _reserved_slots += head.slots;
                awards.push_back(Award{
                    .producer = std::move(head.producer),
                    .reservation = OutboundReservation{*this, connection, head.slots},
                });
                usage.waiters.pop_front();
                --_waiter_count;

                if (!usage.waiters.empty())
                {
                    usage.queued_for_grant = true;
                    _grant_order.push_back(connection);
                }
            }
        }

        for (Award& award : awards)
        {
            award.producer.complete(std::move(award.reservation));
        }

        return awards.size();
    }

    bool OutboundChannel::takePendingAdmissionFailures(std::vector<snf::net::ConnectionId>& failures)
    {
        std::lock_guard lock{_mutex};
        failures.reserve(failures.size() + _admission_failures.size());
        failures.insert(failures.end(), _admission_failures.begin(), _admission_failures.end());
        _admission_failures.clear();
        return std::exchange(_admission_failure_overflowed, false);
    }

    void OutboundChannel::reportAdmissionFailure(const snf::net::ConnectionId connection) noexcept
    {
        try
        {
            {
                std::lock_guard lock{_mutex};
                if (_admission_failures.contains(connection))
                {
                }
                else if (_admission_failures.size() == _max_pending_admission_failures)
                {
                    _admission_failure_overflowed = true;
                }
                else
                {
                    _admission_failures.insert(connection);
                }
            }
        }
        catch (...)
        {
            try
            {
                std::lock_guard lock{_mutex};
                _admission_failure_overflowed = true;
            }
            catch (...)
            {
            }
        }

        signalWakeUp();
    }

    std::size_t OutboundChannel::cancel()
    {
        std::vector<snf::runtime::AsyncOperationProducer<OutboundReservation>> producers;
        std::size_t discarded_action_count = 0;
        {
            std::lock_guard lock{_mutex};
            _cancelled = true;
            discarded_action_count = _items.size();
            _items.clear();
            _grant_order.clear();

            for (auto& [connection, usage] : _connections)
            {
                static_cast<void>(connection);
                for (Waiter& waiter : usage.waiters)
                {
                    producers.push_back(std::move(waiter.producer));
                }

                usage.waiters.clear();
                usage.queued = 0;
                usage.queued_for_grant = false;
            }

            _waiter_count = 0;
        }

        for (auto& producer : producers)
        {
            producer.complete(OutboundReservation{});
        }

        signalWakeUp();
        return discarded_action_count;
    }

    std::size_t OutboundChannel::size() const
    {
        std::lock_guard lock{_mutex};
        return _items.size();
    }

    std::size_t OutboundChannel::capacity() const noexcept
    {
        return _capacity;
    }

    std::size_t OutboundChannel::highWaterMark() const
    {
        std::lock_guard lock{_mutex};
        return _high_water_mark;
    }

    std::size_t OutboundChannel::reservedSlotCount() const
    {
        std::lock_guard lock{_mutex};
        return _reserved_slots;
    }

    std::size_t OutboundChannel::pendingWaiterCount() const
    {
        std::lock_guard lock{_mutex};
        return _waiter_count;
    }

    std::size_t OutboundChannel::trackedConnectionCount() const
    {
        std::lock_guard lock{_mutex};
        return _connections.size();
    }

    std::size_t OutboundChannel::pendingAdmissionFailureCount() const
    {
        std::lock_guard lock{_mutex};
        return _admission_failures.size();
    }

    bool OutboundChannel::isCancelled() const
    {
        std::lock_guard lock{_mutex};
        return _cancelled;
    }

    void OutboundChannel::returnSlots(const snf::net::ConnectionId connection, const std::size_t slots) noexcept
    {
        try
        {
            {
                std::lock_guard lock{_mutex};
                _reserved_slots = _reserved_slots >= slots ? _reserved_slots - slots : 0;

                const auto usage_iterator = _connections.find(connection);
                if (usage_iterator != _connections.end())
                {
                    ConnectionUsage& usage = usage_iterator->second;
                    usage.reserved = usage.reserved >= slots ? usage.reserved - slots : 0;
                    markGrantable(connection, usage);
                    eraseUsageIfIdle(connection);
                }
            }

            signalWakeUp();
        }
        catch (...)
        {
        }
    }

    std::optional<PostedOutboundAction> OutboundChannel::takeFront()
    {
        if (_items.empty())
        {
            return std::nullopt;
        }

        QueuedAction queued = std::move(_items.front());
        _items.pop_front();

        const auto usage_iterator = _connections.find(queued.connection);
        if (usage_iterator != _connections.end())
        {
            ConnectionUsage& usage = usage_iterator->second;
            if (usage.queued != 0)
            {
                --usage.queued;
            }

            markGrantable(queued.connection, usage);
            eraseUsageIfIdle(queued.connection);
        }

        return std::move(queued.posted);
    }

    bool OutboundChannel::fits(const ConnectionUsage& usage, const std::size_t slots) const
    {
        return _items.size() + _reserved_slots + slots <= _capacity && usage.queued + usage.reserved + slots <= _max_slots_per_connection;
    }

    void OutboundChannel::markGrantable(const snf::net::ConnectionId connection, ConnectionUsage& usage)
    {
        if (usage.waiters.empty() || usage.queued_for_grant)
        {
            return;
        }

        usage.queued_for_grant = true;
        _grant_order.push_back(connection);
    }

    void OutboundChannel::eraseUsageIfIdle(const snf::net::ConnectionId connection)
    {
        const auto usage_iterator = _connections.find(connection);
        if (usage_iterator == _connections.end())
        {
            return;
        }

        const ConnectionUsage& usage = usage_iterator->second;
        if (usage.erase_when_idle && usage.queued == 0 && usage.reserved == 0 && usage.waiters.empty() && !usage.queued_for_grant)
        {
            _connections.erase(usage_iterator);
        }
    }

    void OutboundChannel::signalWakeUp() const noexcept
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
