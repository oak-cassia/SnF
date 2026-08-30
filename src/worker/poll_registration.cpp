#include "snf/worker/poll_registration.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

namespace
{
    [[nodiscard]] std::size_t validatedPollRegistrationCapacity(const std::size_t capacity)
    {
        if (capacity == 0 || capacity > static_cast<std::size_t>(snf::worker::MAX_POLL_INDEX) + 1)
        {
            throw std::invalid_argument{"Invalid poll registration capacity"};
        }
        return capacity;
    }
}

namespace snf::worker
{
    PollRegistrationTable::Reservation::Reservation(
        PollRegistrationTable& table,
        const std::size_t index,
        const PollRegistrationHandle handle
    ) noexcept
        : _table(&table)
        , _index(index)
        , _handle(handle)
    {
    }

    PollRegistrationTable::Reservation::~Reservation()
    {
        rollback();
    }

    PollRegistrationTable::Reservation::Reservation(Reservation&& other) noexcept
        : _table(std::exchange(other._table, nullptr))
        , _index(other._index)
        , _handle(other._handle)
        , _committed(std::exchange(other._committed, false))
    {
    }

    PollRegistrationTable::Reservation& PollRegistrationTable::Reservation::operator=(Reservation&& other) noexcept
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

    PollRegistrationHandle PollRegistrationTable::Reservation::handle() const noexcept
    {
        return _handle;
    }

    PollRegistrationView PollRegistrationTable::Reservation::view() const
    {
        if (_table == nullptr)
        {
            throw std::logic_error{"An empty poll registration reservation has no view"};
        }
        return _table->viewFor(_index);
    }

    PollToken PollRegistrationTable::Reservation::token() const noexcept
    {
        if (_table == nullptr)
        {
            return {};
        }
        return _table->_slots[_index].token;
    }

    void PollRegistrationTable::Reservation::commit() noexcept
    {
        if (_table == nullptr || _committed)
        {
            return;
        }

        PollRegistrationTable::Slot& slot = _table->_slots[_index];
        slot.reserved = false;
        slot.committed = true;
        --_table->_reserved_count;
        ++_table->_active_count;
        _committed = true;
    }

    void PollRegistrationTable::Reservation::rollback() noexcept
    {
        if (_table == nullptr || _committed)
        {
            return;
        }
        _table->rollback(_index);
        _table = nullptr;
    }

    PollRegistrationTable::PollRegistrationTable(const std::size_t capacity)
        : _slots(validatedPollRegistrationCapacity(capacity))
    {
    }

    std::optional<PollRegistrationTable::Reservation> PollRegistrationTable::tryReserve(
        const int descriptor,
        const PollTargetKind kind,
        const std::optional<ConnectionHandle> connection
    )
    {
        if (descriptor < 0 || kind == PollTargetKind::Invalid || kind == PollTargetKind::Wakeup ||
            (kind == PollTargetKind::ClientConnection && !connection.has_value()) ||
            (kind != PollTargetKind::ClientConnection && connection.has_value()))
        {
            throw std::invalid_argument{"Invalid client poll registration"};
        }

        for (std::size_t index = 0; index < _slots.size(); ++index)
        {
            Slot& slot = _slots[index];
            if (slot.reserved || slot.committed)
            {
                continue;
            }

            if (_next_generation >= static_cast<std::uint64_t>(MAX_POLL_GENERATION))
            {
                throw std::overflow_error{"Poll registration generation exhausted"};
            }

            slot.generation = ++_next_generation;
            slot.descriptor = descriptor;
            slot.token = PollToken{
                .kind = kind,
                .index = static_cast<std::uint32_t>(index),
                .generation = static_cast<std::uint32_t>(slot.generation),
            };
            slot.connection = connection;
            slot.reserved = true;
            slot.committed = false;
            ++_reserved_count;
            return std::optional<Reservation>{
                std::in_place,
                *this,
                index,
                PollRegistrationHandle{
                    .index = static_cast<std::uint32_t>(index),
                    .generation = PollRegistrationGeneration{slot.generation},
                }
            };
        }

        return std::nullopt;
    }

    std::optional<PollRegistrationView> PollRegistrationTable::lookup(const PollToken token) const noexcept
    {
        if (token.kind == PollTargetKind::Invalid || token.index >= _slots.size())
        {
            return std::nullopt;
        }

        const Slot& slot = _slots[token.index];
        if (!slot.committed || slot.token.kind != token.kind || slot.token.generation != token.generation)
        {
            return std::nullopt;
        }
        return viewFor(token.index);
    }

    std::optional<PollRegistrationView> PollRegistrationTable::lookup(const PollRegistrationHandle handle) const noexcept
    {
        if (!handle.isValid() || handle.index >= _slots.size())
        {
            return std::nullopt;
        }

        const Slot& slot = _slots[handle.index];
        if (!slot.committed || slot.generation != handle.generation.value)
        {
            return std::nullopt;
        }
        return viewFor(handle.index);
    }

    bool PollRegistrationTable::release(const PollRegistrationHandle handle) noexcept
    {
        if (!handle.isValid() || handle.index >= _slots.size())
        {
            return false;
        }

        Slot& slot = _slots[handle.index];
        if (!slot.committed || slot.generation != handle.generation.value)
        {
            return false;
        }

        slot.generation = 0;
        slot.descriptor = -1;
        slot.token = {};
        slot.connection.reset();
        slot.reserved = false;
        slot.committed = false;
        --_active_count;
        return true;
    }

    bool PollRegistrationTable::hasCapacity() const noexcept
    {
        return _reserved_count + _active_count < _slots.size();
    }

    std::size_t PollRegistrationTable::capacity() const noexcept
    {
        return _slots.size();
    }

    std::size_t PollRegistrationTable::reservedCount() const noexcept
    {
        return _reserved_count;
    }

    std::size_t PollRegistrationTable::activeCount() const noexcept
    {
        return _active_count;
    }

    PollRegistrationView PollRegistrationTable::viewFor(const std::size_t index) const noexcept
    {
        const Slot& slot = _slots[index];
        return PollRegistrationView{
            .handle =
                PollRegistrationHandle{
                    .index = static_cast<std::uint32_t>(index),
                    .generation = PollRegistrationGeneration{slot.generation},
                },
            .descriptor = slot.descriptor,
            .token = slot.token,
            .connection = slot.connection,
        };
    }

    void PollRegistrationTable::rollback(const std::size_t index) noexcept
    {
        Slot& slot = _slots[index];
        if (!slot.reserved || slot.committed)
        {
            return;
        }
        slot.generation = 0;
        slot.descriptor = -1;
        slot.token = {};
        slot.connection.reset();
        slot.reserved = false;
        slot.committed = false;
        --_reserved_count;
    }
}
