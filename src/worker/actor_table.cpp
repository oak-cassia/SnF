#include "snf/worker/actor_table.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

namespace snf::worker
{
    ActorSlot* ActorTable::activeAt(const std::size_t index) noexcept
    {
        return index < _slots.size() && _slots[index].committed ? &*_slots[index].actor : nullptr;
    }

    const ActorSlot* ActorTable::activeAt(const std::size_t index) const noexcept
    {
        return index < _slots.size() && _slots[index].committed ? &*_slots[index].actor : nullptr;
    }
    ActorSlot::ActorSlot(const ActorKey key, const ActorIncarnation incarnation, const std::size_t slot_index) noexcept
        : _key(key)
        , _incarnation(incarnation)
        , _slot_index(slot_index)
    {
    }

    ReadyActorQueue::ReadyActorQueue(const std::size_t capacity)
        : _capacity(capacity)
        , _buffer(capacity)
    {
        if (capacity == 0)
        {
            throw std::invalid_argument{"ReadyActorQueue capacity must be > 0"};
        }
    }

    void ReadyActorQueue::push(const ActorHandle handle)
    {
        if (full())
        {
            throw std::logic_error{"ReadyActorQueue invariant violation: queue is full"};
        }
        _buffer[_tail] = handle;
        _tail = (_tail + 1) % _capacity;
        ++_size;
    }

    ActorHandle ReadyActorQueue::pop()
    {
        if (empty())
        {
            throw std::logic_error{"ReadyActorQueue underflow"};
        }
        const ActorHandle handle = _buffer[_head];
        _head = (_head + 1) % _capacity;
        --_size;
        return handle;
    }

    void ReadyActorQueue::clear() noexcept
    {
        _head = 0;
        _tail = 0;
        _size = 0;
    }

    ActorTable::Reservation::Reservation(ActorTable& table, const std::size_t index, const ActorHandle handle) noexcept
        : _table(&table)
        , _index(index)
        , _handle(handle)
    {
    }

    ActorTable::Reservation::~Reservation()
    {
        rollback();
    }

    ActorTable::Reservation::Reservation(Reservation&& other) noexcept
        : _table(std::exchange(other._table, nullptr))
        , _index(other._index)
        , _handle(other._handle)
        , _committed(std::exchange(other._committed, false))
    {
    }

    ActorTable::Reservation& ActorTable::Reservation::operator=(Reservation&& other) noexcept
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

    ActorHandle ActorTable::Reservation::handle() const noexcept
    {
        return _handle;
    }

    ActorKey ActorTable::Reservation::key() const noexcept
    {
        return _table->_slots[_index].actor->key();
    }

    ActorSlot& ActorTable::Reservation::slot() noexcept
    {
        return *_table->_slots[_index].actor;
    }

    const ActorSlot& ActorTable::Reservation::slot() const noexcept
    {
        return *_table->_slots[_index].actor;
    }

    void ActorTable::Reservation::commit() noexcept
    {
        if (_table == nullptr || _committed)
        {
            return;
        }

        ActorTable::Slot& slot = _table->_slots[_index];
        slot.reserved = false;
        slot.committed = true;
        --_table->_reserved_count;
        ++_table->_active_count;
        _committed = true;
    }

    void ActorTable::Reservation::rollback() noexcept
    {
        if (_table == nullptr || _committed)
        {
            return;
        }
        _table->rollback(_index);
        _table = nullptr;
    }

    ActorTable::ActorTable(const std::size_t capacity)
        : _slots(capacity)
    {
        if (capacity == 0)
        {
            throw std::invalid_argument{"ActorTable capacity must be > 0"};
        }

        _free_indices.reserve(capacity);
        for (std::size_t index = capacity; index > 0; --index)
        {
            _free_indices.push_back(index - 1);
        }
        _key_to_slot.reserve(capacity);
    }

    std::optional<ActorTable::Reservation> ActorTable::tryReserve(const ActorKey key)
    {
        if (!hasCapacity() || _free_indices.empty())
        {
            return std::nullopt;
        }

        if (_key_to_slot.find(key) != _key_to_slot.end())
        {
            return std::nullopt;
        }

        const std::size_t index = _free_indices.back();
        const ActorIncarnation incarnation = nextIncarnation();

        // Allocate the hash node before mutating the slot/free-list. If the
        // allocation throws, the table remains unchanged and there is no
        // not-yet-returned Reservation that would need to roll it back.
        const auto [key_it, inserted] = _key_to_slot.emplace(key, index);
        if (!inserted)
        {
            return std::nullopt;
        }
        static_cast<void>(key_it);

        _free_indices.pop_back();
        Slot& slot = _slots[index];
        slot.actor.emplace(key, incarnation, index);
        slot.reserved = true;
        slot.committed = false;
        ++_reserved_count;

        return std::optional<Reservation>{
            std::in_place,
            *this,
            index,
            ActorHandle{
                .slot_index = static_cast<std::uint32_t>(index),
                .incarnation = incarnation,
            }
        };
    }

    ActorSlot* ActorTable::find(const ActorKey key) noexcept
    {
        const auto it = _key_to_slot.find(key);
        if (it == _key_to_slot.end())
        {
            return nullptr;
        }

        Slot& slot = _slots[it->second];
        if (!slot.committed || !slot.actor)
        {
            return nullptr;
        }
        return &*slot.actor;
    }

    const ActorSlot* ActorTable::find(const ActorKey key) const noexcept
    {
        const auto it = _key_to_slot.find(key);
        if (it == _key_to_slot.end())
        {
            return nullptr;
        }

        const Slot& slot = _slots[it->second];
        if (!slot.committed || !slot.actor)
        {
            return nullptr;
        }
        return &*slot.actor;
    }

    ActorSlot* ActorTable::find(const ActorHandle handle) noexcept
    {
        if (!handle.isValid() || handle.slot_index >= _slots.size())
        {
            return nullptr;
        }

        Slot& slot = _slots[handle.slot_index];
        if (!slot.committed || !slot.actor || slot.actor->incarnation() != handle.incarnation)
        {
            return nullptr;
        }
        return &*slot.actor;
    }

    const ActorSlot* ActorTable::find(const ActorHandle handle) const noexcept
    {
        if (!handle.isValid() || handle.slot_index >= _slots.size())
        {
            return nullptr;
        }

        const Slot& slot = _slots[handle.slot_index];
        if (!slot.committed || !slot.actor || slot.actor->incarnation() != handle.incarnation)
        {
            return nullptr;
        }
        return &*slot.actor;
    }

    bool ActorTable::release(const ActorHandle handle) noexcept
    {
        if (!handle.isValid() || handle.slot_index >= _slots.size())
        {
            return false;
        }

        Slot& slot = _slots[handle.slot_index];
        if (!slot.committed || !slot.actor || slot.actor->incarnation() != handle.incarnation)
        {
            return false;
        }

        _key_to_slot.erase(slot.actor->key());
        slot.actor.reset();
        slot.committed = false;
        --_active_count;
        _free_indices.push_back(handle.slot_index);
        return true;
    }

    bool ActorTable::hasCapacity() const noexcept
    {
        return _reserved_count + _active_count < _slots.size();
    }

    std::size_t ActorTable::capacity() const noexcept
    {
        return _slots.size();
    }

    std::size_t ActorTable::reservedCount() const noexcept
    {
        return _reserved_count;
    }

    std::size_t ActorTable::activeCount() const noexcept
    {
        return _active_count;
    }

    std::size_t ActorTable::availableCount() const noexcept
    {
        return _slots.size() - _reserved_count - _active_count;
    }

    std::vector<ActorHandle> ActorTable::activeHandles() const
    {
        std::vector<ActorHandle> handles;
        handles.reserve(_active_count);
        for (const Slot& slot : _slots)
        {
            if (slot.committed && slot.actor)
            {
                handles.push_back(slot.actor->handle());
            }
        }
        return handles;
    }

    ActorIncarnation ActorTable::nextIncarnation()
    {
        if (_incarnation_source.last().value == std::numeric_limits<std::uint64_t>::max())
        {
            throw std::overflow_error{"Actor incarnation exhausted"};
        }
        return _incarnation_source.next();
    }

    void ActorTable::rollback(const std::size_t index) noexcept
    {
        Slot& slot = _slots[index];
        if (!slot.reserved || slot.committed)
        {
            return;
        }
        if (slot.actor)
        {
            _key_to_slot.erase(slot.actor->key());
        }
        slot.actor.reset();
        slot.reserved = false;
        --_reserved_count;
        _free_indices.push_back(index);
    }
}
