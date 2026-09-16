#include "snf/worker/connection_work_queue.hpp"

#include <stdexcept>

namespace snf::worker
{
    ConnectionWorkQueue::ConnectionWorkQueue(const std::size_t capacity)
        : _items(capacity)
    {
        if (capacity == 0)
        {
            throw std::invalid_argument{"A connection work queue capacity must be positive"};
        }
    }

    bool ConnectionWorkQueue::tryPush(const ConnectionHandle handle) noexcept
    {
        if (_size == _items.size())
        {
            return false;
        }

        _items[_tail] = handle;
        _tail = (_tail + 1) % _items.size();
        ++_size;
        return true;
    }

    std::optional<ConnectionHandle> ConnectionWorkQueue::tryPop() noexcept
    {
        if (_size == 0)
        {
            return std::nullopt;
        }

        const ConnectionHandle handle = _items[_head];
        _head = (_head + 1) % _items.size();
        --_size;
        return handle;
    }

    bool ConnectionWorkQueue::empty() const noexcept
    {
        return _size == 0;
    }

    std::size_t ConnectionWorkQueue::size() const noexcept
    {
        return _size;
    }

    std::size_t ConnectionWorkQueue::capacity() const noexcept
    {
        return _items.size();
    }

    void ConnectionWorkQueue::clear() noexcept
    {
        _head = 0;
        _tail = 0;
        _size = 0;
    }
}
