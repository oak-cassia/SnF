#pragma once

#include "snf/worker/actor_envelope.hpp"
#include "snf/worker/budget.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/identity.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
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

    struct ApplicationTimer
    {
        ActivationRef target;
        ActorEnvelope message;
        std::uint64_t charged_bytes{0};
    };

    using TimerPayload = std::variant<AwaitTimeout, ConnectionCloseDeadline, ApplicationTimer>;

    static_assert(std::is_nothrow_move_constructible_v<TimerPayload>);

    struct TimerEntry
    {
        using TimePoint = std::chrono::steady_clock::time_point;

        TimePoint deadline;
        TimerPayload payload;

        TimerEntry(const TimePoint d, TimerPayload p) noexcept
            : deadline(d)
            , payload(std::move(p))
        {
        }

        TimerEntry(TimerEntry&&) noexcept = default;
        TimerEntry& operator=(TimerEntry&&) noexcept = default;

        TimerEntry(const TimerEntry&) = delete;
        TimerEntry& operator=(const TimerEntry&) = delete;

        [[nodiscard]] bool operator>(const TimerEntry& other) const noexcept
        {
            return deadline > other.deadline;
        }
    };

    static_assert(std::is_nothrow_move_constructible_v<TimerEntry>);
    static_assert(std::is_nothrow_swappable_v<TimerEntry>);

    struct ExpireResult
    {
        std::size_t expired{0};
        bool due_items_remain{false};
        bool budget_exhausted{false};
    };

    class TimerAdmission;

    class TimerReservation final
    {
    public:
        TimerReservation() noexcept = default;
        ~TimerReservation() noexcept;

        TimerReservation(const TimerReservation&) = delete;
        TimerReservation& operator=(const TimerReservation&) = delete;

        TimerReservation(TimerReservation&& other) noexcept
            : _admission(other._admission)
            , _charged_bytes(other._charged_bytes)
            , _turn_id(other._turn_id)
        {
            other._admission = nullptr;
            other._charged_bytes = 0;
            other._turn_id = 0;
        }

        TimerReservation& operator=(TimerReservation&& other) noexcept
        {
            if (this != &other)
            {
                reset();
                _admission = other._admission;
                _charged_bytes = other._charged_bytes;
                _turn_id = other._turn_id;
                other._admission = nullptr;
                other._charged_bytes = 0;
                other._turn_id = 0;
            }
            return *this;
        }

        [[nodiscard]] bool isValid() const noexcept
        {
            return _admission != nullptr;
        }

        [[nodiscard]] explicit operator bool() const noexcept
        {
            return isValid();
        }

        [[nodiscard]] std::uint64_t chargedBytes() const noexcept
        {
            return _charged_bytes;
        }

        [[nodiscard]] std::uint64_t turnId() const noexcept
        {
            return _turn_id;
        }

        [[nodiscard]] TimerAdmission* admission() const noexcept
        {
            return _admission;
        }

        void reset() noexcept;

        TimerReservation(TimerAdmission* admission, const std::uint64_t charged_bytes, const std::uint64_t turn_id) noexcept
            : _admission(admission)
            , _charged_bytes(charged_bytes)
            , _turn_id(turn_id)
        {
        }

    private:
        friend class TimerAdmission;
        friend class Worker;

        TimerAdmission* _admission{nullptr};
        std::uint64_t _charged_bytes{0};
        std::uint64_t _turn_id{0};
    };

    static_assert(std::is_nothrow_move_constructible_v<TimerReservation>);
    static_assert(!std::is_copy_constructible_v<TimerReservation>);
    static_assert(!std::is_copy_assignable_v<TimerReservation>);

    class TimerAdmission
    {
    public:
        virtual ~TimerAdmission() = default;
        [[nodiscard]] virtual std::optional<TimerReservation> tryReserve(std::uint64_t charged_bytes, std::uint64_t turn_id) noexcept = 0;
        virtual void releaseReservation(std::uint64_t charged_bytes) noexcept = 0;
    };

    inline void TimerReservation::reset() noexcept
    {
        if (_admission != nullptr)
        {
            _admission->releaseReservation(_charged_bytes);
            _admission = nullptr;
            _charged_bytes = 0;
            _turn_id = 0;
        }
    }

    inline TimerReservation::~TimerReservation() noexcept
    {
        reset();
    }

    class TimerQueue
    {
    public:
        static constexpr std::size_t CAPACITY = 65536;
        using TimePoint = std::chrono::steady_clock::time_point;
        using Entry = TimerEntry;

        explicit TimerQueue(const std::uint64_t max_application_timer_bytes = 64 * 1024 * 1024)
            : _max_application_timer_bytes(max_application_timer_bytes)
        {
            _heap.reserve(CAPACITY);
        }

        void setMaxApplicationTimerBytes(const std::uint64_t max_bytes) noexcept
        {
            _max_application_timer_bytes = max_bytes;
        }

        [[nodiscard]] bool trySchedule(const TimePoint deadline, TimerPayload payload)
        {
            if (_heap.size() + _reserved_entries >= CAPACITY)
            {
                return false;
            }
            if (std::holds_alternative<ApplicationTimer>(payload))
            {
                const auto charge = std::get<ApplicationTimer>(payload).charged_bytes;
                if (_application_timer_bytes + _reserved_application_timer_bytes + charge > _max_application_timer_bytes)
                {
                    return false;
                }
                _application_timer_bytes += charge;
            }
            _heap.push_back(Entry{deadline, std::move(payload)});
            std::push_heap(_heap.begin(), _heap.end(), std::greater<Entry>{});
            return true;
        }

        [[nodiscard]] bool tryReserve() noexcept
        {
            if (_heap.size() + _reserved_entries >= CAPACITY)
            {
                return false;
            }
            ++_reserved_entries;
            return true;
        }

        void releaseReservation() noexcept
        {
            assert(_reserved_entries > 0);
            --_reserved_entries;
        }

        void commitReserved(const TimePoint deadline, TimerPayload payload) noexcept
        {
            assert(_reserved_entries > 0);
            --_reserved_entries;
            if (std::holds_alternative<ApplicationTimer>(payload))
            {
                _application_timer_bytes += std::get<ApplicationTimer>(payload).charged_bytes;
            }
            _heap.push_back(Entry{deadline, std::move(payload)});
            std::push_heap(_heap.begin(), _heap.end(), std::greater<Entry>{});
        }

        [[nodiscard]] bool tryReserveApplicationTimer(const std::uint64_t charged_bytes) noexcept
        {
            if (_heap.size() + _reserved_entries >= CAPACITY)
            {
                return false;
            }
            if (_application_timer_bytes + _reserved_application_timer_bytes + charged_bytes > _max_application_timer_bytes)
            {
                return false;
            }
            ++_reserved_entries;
            _reserved_application_timer_bytes += charged_bytes;
            return true;
        }

        void releaseApplicationTimerReservation(const std::uint64_t charged_bytes) noexcept
        {
            assert(_reserved_entries > 0);
            assert(_reserved_application_timer_bytes >= charged_bytes);
            --_reserved_entries;
            _reserved_application_timer_bytes -= charged_bytes;
        }

        void commitReservedApplicationTimer(
            const TimePoint deadline,
            const ActivationRef target,
            ActorEnvelope message,
            const std::uint64_t charged_bytes
        ) noexcept
        {
            assert(_reserved_entries > 0);
            assert(_reserved_application_timer_bytes >= charged_bytes);
            --_reserved_entries;
            _reserved_application_timer_bytes -= charged_bytes;
            _application_timer_bytes += charged_bytes;
            _heap.push_back(Entry{deadline, ApplicationTimer{target, std::move(message), charged_bytes}});
            std::push_heap(_heap.begin(), _heap.end(), std::greater<Entry>{});
        }

        [[nodiscard]] bool tryScheduleApplicationTimer(
            const TimePoint deadline,
            const ActivationRef target,
            ActorEnvelope message
        )
        {
            const std::uint64_t charge = message.chargedBytes();
            if (_heap.size() + _reserved_entries >= CAPACITY)
            {
                return false;
            }
            if (_application_timer_bytes + _reserved_application_timer_bytes + charge > _max_application_timer_bytes)
            {
                return false;
            }
            _application_timer_bytes += charge;
            _heap.push_back(Entry{deadline, ApplicationTimer{target, std::move(message), charge}});
            std::push_heap(_heap.begin(), _heap.end(), std::greater<Entry>{});
            return true;
        }

        [[nodiscard]] std::size_t cancelApplicationTimers() noexcept
        {
            std::size_t count = 0;
            auto it = _heap.begin();
            while (it != _heap.end())
            {
                if (std::holds_alternative<ApplicationTimer>(it->payload))
                {
                    const auto charge = std::get<ApplicationTimer>(it->payload).charged_bytes;
                    assert(_application_timer_bytes >= charge);
                    _application_timer_bytes -= charge;
                    ++count;
                    it = _heap.erase(it);
                }
                else
                {
                    ++it;
                }
            }
            if (count > 0)
            {
                std::make_heap(_heap.begin(), _heap.end(), std::greater<Entry>{});
            }
            return count;
        }

        [[nodiscard]] std::size_t applicationTimerCount() const noexcept
        {
            std::size_t count = 0;
            for (const auto& entry : _heap)
            {
                if (std::holds_alternative<ApplicationTimer>(entry.payload))
                {
                    ++count;
                }
            }
            return count;
        }

        [[nodiscard]] std::uint64_t applicationTimerBytes() const noexcept
        {
            return _application_timer_bytes;
        }

        [[nodiscard]] std::uint64_t reservedApplicationTimerBytes() const noexcept
        {
            return _reserved_application_timer_bytes;
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

        template <class Handler>
        [[nodiscard]] ExpireResult expire(const TimePoint now, const CountTimeBudget& budget, Handler&& handler)
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

                if (std::holds_alternative<ApplicationTimer>(entry.payload))
                {
                    const auto charge = std::get<ApplicationTimer>(entry.payload).charged_bytes;
                    assert(_application_timer_bytes >= charge);
                    _application_timer_bytes -= charge;
                }

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
        std::vector<Entry> _heap;
        std::size_t _reserved_entries{0};
        std::uint64_t _max_application_timer_bytes{64 * 1024 * 1024};
        std::uint64_t _application_timer_bytes{0};
        std::uint64_t _reserved_application_timer_bytes{0};
    };
}
