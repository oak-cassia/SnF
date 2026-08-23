#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <utility>

namespace snf::runtime
{
    template <typename T> class BoundedQueue
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

        template <typename U> [[nodiscard]] bool tryPush(U&& item)
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

        template <typename U> [[nodiscard]] bool push(U&& item)
        {
            {
                std::unique_lock lock{_mutex};
                _not_full.wait(lock, [this] { return _cancelled || _closed || _items.size() < _capacity; });

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

        template <typename U> [[nodiscard]] bool push(U&& item, const std::stop_token stop_token)
        {
            {
                std::unique_lock lock{_mutex};
                const bool can_push = _not_full.wait(lock, stop_token, [this] { return _cancelled || _closed || _items.size() < _capacity; });

                if (!can_push || stop_token.stop_requested() || _closed || _cancelled)
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
            _not_empty.wait(lock, [this] { return _cancelled || !_items.empty() || _closed; });

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

        void close()
        {
            {
                std::lock_guard lock{_mutex};
                _closed = true;
            }

            _not_empty.notify_all();
            _not_full.notify_all();
        }

        std::size_t cancel()
        {
            std::size_t discarded_count = 0;
            {
                std::lock_guard lock{_mutex};
                _cancelled = true;
                discarded_count = _items.size();
                _items.clear();
            }

            _not_empty.notify_all();
            _not_full.notify_all();
            return discarded_count;
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
        std::condition_variable_any _not_full;
        std::deque<T> _items;
        std::size_t _high_water_mark{0};
        bool _closed{false};
        bool _cancelled{false};
    };
}
