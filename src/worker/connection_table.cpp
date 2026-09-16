#include "snf/worker/connection_table.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

namespace
{
    [[nodiscard]] std::size_t validatedConnectionCapacity(const snf::worker::ConnectionTableConfig& config)
    {
        const auto& limits = config.limits;
        if (config.capacity == 0 || config.capacity > static_cast<std::size_t>(snf::worker::MAX_POLL_INDEX) + 1 ||
            limits.max_read_buffer_bytes == 0 || limits.write_soft_watermark_bytes == 0 || limits.write_hard_limit_bytes == 0 ||
            limits.write_soft_watermark_bytes > limits.write_hard_limit_bytes || limits.close_drain_deadline < std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Invalid connection table configuration"};
        }
        return config.capacity;
    }
}

namespace snf::worker
{
    ConnectionTable::Reservation::Reservation(ConnectionTable& table, const std::size_t index, const ConnectionHandle handle) noexcept
        : _table(&table)
        , _index(index)
        , _handle(handle)
    {
    }

    ConnectionTable::Reservation::~Reservation()
    {
        rollback();
    }

    ConnectionTable::Reservation::Reservation(Reservation&& other) noexcept
        : _table(std::exchange(other._table, nullptr))
        , _index(other._index)
        , _handle(other._handle)
        , _committed(std::exchange(other._committed, false))
    {
    }

    ConnectionTable::Reservation& ConnectionTable::Reservation::operator=(Reservation&& other) noexcept
    {
        if (this != &other)
        {
            rollback();
            _table = std::exchange(other._table, nullptr);
            _index = other._index;
            _handle = other._handle;
            _committed = std::exchange(other._committed, false);
        }
        return *this;
    }

    ConnectionHandle ConnectionTable::Reservation::handle() const noexcept
    {
        return _handle;
    }

    ConnectionRef ConnectionTable::Reservation::reference() const noexcept
    {
        return _table->_slots[_index].connection->reference();
    }

    ConnectionSlot& ConnectionTable::Reservation::slot() noexcept
    {
        return *_table->_slots[_index].connection;
    }

    const ConnectionSlot& ConnectionTable::Reservation::slot() const noexcept
    {
        return *_table->_slots[_index].connection;
    }

    void ConnectionTable::Reservation::commit() noexcept
    {
        if (_table == nullptr || _committed)
        {
            return;
        }

        ConnectionTable::Slot& slot = _table->_slots[_index];
        slot.reserved = false;
        slot.committed = true;
        --_table->_reserved_count;
        ++_table->_active_count;
        _committed = true;
    }

    void ConnectionTable::Reservation::rollback() noexcept
    {
        if (_table == nullptr || _committed)
        {
            return;
        }
        _table->rollback(_index);
        _table = nullptr;
    }

    ConnectionTable::ConnectionTable(const ConnectionTableConfig& config)
        : _slots(validatedConnectionCapacity(config))
        , _limits(config.limits)
    {
    }

    std::optional<ConnectionTable::Reservation> ConnectionTable::tryReserve(snf::net::UniqueFileDescriptor socket, const WorkerId owner)
    {
        if (!socket.isValid())
        {
            throw std::invalid_argument{"A connection reservation requires a valid socket"};
        }

        if (!hasCapacity())
        {
            return std::nullopt;
        }

        for (std::size_t index = 0; index < _slots.size(); ++index)
        {
            Slot& slot = _slots[index];
            if (slot.reserved || slot.committed)
            {
                continue;
            }

            const ConnectionGeneration generation = nextGeneration();
            const ConnectionRef reference{
                .id = ConnectionId{static_cast<std::uint32_t>(index)},
                .generation = generation,
                .owner = owner,
            };

            slot.connection.emplace(std::move(socket), reference, _limits);
            slot.reserved = true;
            slot.committed = false;
            ++_reserved_count;
            return std::optional<Reservation>{
                std::in_place,
                *this,
                index,
                ConnectionHandle{
                    .id = reference.id,
                    .generation = reference.generation,
                }
            };
        }

        return std::nullopt;
    }

    ConnectionSlot* ConnectionTable::find(const ConnectionHandle handle) noexcept
    {
        if (!handle.isValid() || handle.id.value >= _slots.size())
        {
            return nullptr;
        }

        Slot& slot = _slots[handle.id.value];
        if (!slot.committed || !slot.connection || slot.connection->handle() != handle)
        {
            return nullptr;
        }
        return &*slot.connection;
    }

    const ConnectionSlot* ConnectionTable::find(const ConnectionHandle handle) const noexcept
    {
        if (!handle.isValid() || handle.id.value >= _slots.size())
        {
            return nullptr;
        }

        const Slot& slot = _slots[handle.id.value];
        if (!slot.committed || !slot.connection || slot.connection->handle() != handle)
        {
            return nullptr;
        }
        return &*slot.connection;
    }

    bool ConnectionTable::release(const ConnectionHandle handle) noexcept
    {
        if (!handle.isValid() || handle.id.value >= _slots.size())
        {
            return false;
        }

        Slot& slot = _slots[handle.id.value];
        if (!slot.committed || !slot.connection || slot.connection->handle() != handle)
        {
            return false;
        }

        slot.connection.reset();
        slot.committed = false;
        --_active_count;
        return true;
    }

    bool ConnectionTable::hasCapacity() const noexcept
    {
        return _reserved_count + _active_count < _slots.size();
    }

    std::size_t ConnectionTable::capacity() const noexcept
    {
        return _slots.size();
    }

    std::size_t ConnectionTable::reservedCount() const noexcept
    {
        return _reserved_count;
    }

    std::size_t ConnectionTable::activeCount() const noexcept
    {
        return _active_count;
    }

    std::size_t ConnectionTable::availableCount() const noexcept
    {
        return _slots.size() - _reserved_count - _active_count;
    }

    std::vector<ConnectionHandle> ConnectionTable::activeHandles() const
    {
        std::vector<ConnectionHandle> handles;
        handles.reserve(_active_count);
        for (const Slot& slot : _slots)
        {
            if (slot.committed && slot.connection)
            {
                handles.push_back(slot.connection->handle());
            }
        }
        return handles;
    }

    ConnectionGeneration ConnectionTable::nextGeneration()
    {
        if (_generation_source.last().value == std::numeric_limits<std::uint64_t>::max())
        {
            throw std::overflow_error{"Connection generation exhausted"};
        }
        return _generation_source.next();
    }

    void ConnectionTable::rollback(const std::size_t index) noexcept
    {
        Slot& slot = _slots[index];
        if (!slot.reserved || slot.committed)
        {
            return;
        }
        slot.connection.reset();
        slot.reserved = false;
        --_reserved_count;
    }
}
