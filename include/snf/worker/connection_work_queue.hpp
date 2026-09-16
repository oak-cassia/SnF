#pragma once

#include "snf/worker/connection.hpp"

#include <cstddef>
#include <optional>
#include <vector>

namespace snf::worker
{
    // Worker-local, owner-thread-only ring. The queue deliberately contains
    // only value handles; all mutable connection state stays in ConnectionTable.
    class ConnectionWorkQueue final
    {
    public:
        explicit ConnectionWorkQueue(std::size_t capacity);

        ConnectionWorkQueue(const ConnectionWorkQueue&) = delete;
        ConnectionWorkQueue& operator=(const ConnectionWorkQueue&) = delete;

        [[nodiscard]] bool tryPush(ConnectionHandle handle) noexcept;
        [[nodiscard]] std::optional<ConnectionHandle> tryPop() noexcept;
        [[nodiscard]] bool empty() const noexcept;
        [[nodiscard]] std::size_t size() const noexcept;
        [[nodiscard]] std::size_t capacity() const noexcept;
        void clear() noexcept;

    private:
        std::vector<ConnectionHandle> _items;
        std::size_t _head{0};
        std::size_t _tail{0};
        std::size_t _size{0};
    };

    using ReadWorkQueue = ConnectionWorkQueue;
    using WriteWorkQueue = ConnectionWorkQueue;
}
