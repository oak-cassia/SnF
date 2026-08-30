#pragma once

#include "snf/worker/budget.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/identity.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <functional>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace snf::worker
{
    struct AwaitTimeout
    {
        AwaitKey key;

        [[nodiscard]] bool operator==(const AwaitTimeout&) const noexcept = default;
    };

    struct ConnectionCloseDeadline
    {
        ConnectionHandle connection;

        [[nodiscard]] bool operator==(const ConnectionCloseDeadline&) const noexcept = default;
    };

    using TimerPayload = std::variant<AwaitTimeout, ConnectionCloseDeadline>;

    static_assert(std::is_nothrow_move_constructible_v<TimerPayload>);

    struct ExpireResult
    {
        std::size_t expired{0};
        bool due_items_remain{false}; // now 이전 deadline이 아직 남았다
        bool budget_exhausted{false};
    };

    // TimerQueue 계약:
    // - trySchedule(), commitReserved(), tryReserve(), expire() 모두 owner Worker thread 전용이다.
    // - 외부 thread가 실행 중인 Worker의 TimerQueue에 직접 접근하지 않는다.
    // - 동일한 deadline 사이의 만료 순서는 보장하지 않는다 (§9.3).
    //
    // Reservation no-fail 계약:
    // tryReserve() == true -> 뒤따르는 commitReserved()는 capacity 또는 allocation 때문에 실패하지 않는다.
    // 이를 위해 생성자에서 _heap.reserve(CAPACITY)로 메모리를 선확보한다.
    class TimerQueue
    {
    public:
        static constexpr std::size_t CAPACITY = 65536;
        using TimePoint = std::chrono::steady_clock::time_point;

        TimerQueue()
        {
            _heap.reserve(CAPACITY);
        }

        [[nodiscard]] bool trySchedule(const TimePoint deadline, TimerPayload payload)
        {
            if (_heap.size() + _reserved >= CAPACITY)
            {
                return false;
            }
            _heap.push_back(Entry{deadline, std::move(payload)});
            std::push_heap(_heap.begin(), _heap.end(), std::greater<Entry>{});
            return true;
        }

        [[nodiscard]] bool tryReserve() noexcept
        {
            if (_heap.size() + _reserved >= CAPACITY)
            {
                return false;
            }
            ++_reserved;
            return true;
        }

        void releaseReservation() noexcept
        {
            assert(_reserved > 0);
            --_reserved;
        }

        void commitReserved(const TimePoint deadline, TimerPayload payload)
        {
            assert(_reserved > 0);
            --_reserved;
            _heap.push_back(Entry{deadline, std::move(payload)});
            std::push_heap(_heap.begin(), _heap.end(), std::greater<Entry>{});
        }

        [[nodiscard]] std::optional<TimePoint> nextDeadline() const noexcept
        {
            if (_heap.empty())
            {
                return std::nullopt;
            }
            return _heap.front().deadline;
        }

        [[nodiscard]] std::size_t size() const noexcept
        {
            return _heap.size();
        }

        template <class Handler> [[nodiscard]] ExpireResult expire(const TimePoint now, const CountTimeBudget& budget, Handler&& handler)
        {
            ExpireResult result{};
            const auto start_time = std::chrono::steady_clock::now();

            while (!_heap.empty() && _heap.front().deadline <= now)
            {
                if (result.expired >= budget.max_count)
                {
                    result.budget_exhausted = true;
                    result.due_items_remain = true;
                    break;
                }

                std::pop_heap(_heap.begin(), _heap.end(), std::greater<Entry>{});
                Entry entry = std::move(_heap.back());
                _heap.pop_back();

                handler(std::move(entry.payload));
                ++result.expired;

                if ((result.expired & 63U) == 0)
                {
                    const auto current_time = std::chrono::steady_clock::now();
                    if (current_time - start_time >= budget.max_duration)
                    {
                        result.budget_exhausted = true;
                        if (!_heap.empty() && _heap.front().deadline <= now)
                        {
                            result.due_items_remain = true;
                        }
                        break;
                    }
                }
            }

            if (!_heap.empty() && _heap.front().deadline <= now)
            {
                result.due_items_remain = true;
            }

            return result;
        }

    private:
        struct Entry
        {
            TimePoint deadline;
            TimerPayload payload;

            bool operator>(const Entry& other) const noexcept
            {
                return deadline > other.deadline;
            }
        };

        std::vector<Entry> _heap;
        std::size_t _reserved{0};
    };
}
