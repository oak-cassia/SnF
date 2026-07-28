#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>

namespace snf::runtime
{
    // A bounded FIFO queue with two distinct shutdown modes. close() preserves
    // accepted items for consumers, while cancel() abandons them and wakes every waiter.
    template <typename T>
    class BoundedQueue
    {
    public:
        explicit BoundedQueue(const std::size_t capacity)
            : _capacity(capacity)
        {
            if (_capacity == 0)
            {
                throw std::invalid_argument{"A bounded queue capacity must be positive"};
            }
        }

        BoundedQueue(const BoundedQueue&) = delete;
        BoundedQueue& operator=(const BoundedQueue&) = delete;

        template <typename U>
        [[nodiscard]] bool tryPush(U&& item)
        {
            {
                std::lock_guard lock{_mutex};
                if (_closed || _cancelled || _items.size() == _capacity)
                {
                    return false;
                }

                _items.emplace_back(std::forward<U>(item));
                updateHighWaterMark();
            }

            _not_empty.notify_one();
            return true;
        }

        // Waits only on a full queue. The producer is released when capacity becomes
        // available or when close()/cancel() rejects the item.
        template <typename U>
        [[nodiscard]] bool push(U&& item)
        {
            {
                std::unique_lock lock{_mutex};
                _not_full.wait(lock,
                               [this]
                               {
                                   return _cancelled || _closed ||
                                          _items.size() < _capacity;
                               });

                if (_closed || _cancelled)
                {
                    return false;
                }

                _items.emplace_back(std::forward<U>(item));
                updateHighWaterMark();
            }

            _not_empty.notify_one();
            return true;
        }

        [[nodiscard]] std::optional<T> pop()
        {
            std::unique_lock lock{_mutex};
            _not_empty.wait(lock,
                            [this]
                            {
                                return _cancelled || !_items.empty() || _closed;
                            });

            if (_cancelled || _items.empty())
            {
                return std::nullopt;
            }

            std::optional<T> item{std::in_place, std::move(_items.front())};
            _items.pop_front();
            lock.unlock();
            _not_full.notify_one();
            return item;
        }

        [[nodiscard]] std::optional<T> tryPop()
        {
            std::lock_guard lock{_mutex};
            if (_cancelled || _items.empty())
            {
                return std::nullopt;
            }

            std::optional<T> item{std::in_place, std::move(_items.front())};
            _items.pop_front();
            _not_full.notify_one();
            return item;
        }

        // New producers are rejected, but consumers continue draining accepted items.
        void close()
        {
            {
                std::lock_guard lock{_mutex};
                _closed = true;
            }

            _not_empty.notify_all();
            _not_full.notify_all();
        }

        // Used only when graceful shutdown has expired. It abandons queued work and
        // releases every blocked producer and consumer.
        void cancel()
        {
            {
                std::lock_guard lock{_mutex};
                _cancelled = true;
                _items.clear();
            }

            _not_empty.notify_all();
            _not_full.notify_all();
        }

        [[nodiscard]] std::size_t size() const
        {
            std::lock_guard lock{_mutex};
            return _items.size();
        }

        [[nodiscard]] std::size_t capacity() const noexcept
        {
            return _capacity;
        }

        [[nodiscard]] std::size_t highWaterMark() const
        {
            std::lock_guard lock{_mutex};
            return _high_water_mark;
        }

        [[nodiscard]] bool isClosed() const
        {
            std::lock_guard lock{_mutex};
            return _closed;
        }

        [[nodiscard]] bool isCancelled() const
        {
            std::lock_guard lock{_mutex};
            return _cancelled;
        }

    private:
        void updateHighWaterMark() noexcept
        {
            if (_items.size() > _high_water_mark)
            {
                _high_water_mark = _items.size();
            }
        }

        const std::size_t _capacity;
        mutable std::mutex _mutex;
        std::condition_variable _not_empty;
        std::condition_variable _not_full;
        std::deque<T> _items;
        std::size_t _high_water_mark{0};
        bool _closed{false};
        bool _cancelled{false};
    };
}
