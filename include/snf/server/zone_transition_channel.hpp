#pragma once

#include "snf/runtime/distribution.hpp"
#include "snf/server/zone_handoff.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace snf::server
{
    struct ZoneTransitionChannelStats
    {
        snf::runtime::DistributionSnapshot queue_wait_nanoseconds;
        std::uint64_t reservations_created{0};
        std::uint64_t reservations_rejected{0};
        std::uint64_t completions_published{0};
        std::uint64_t completions_consumed{0};
        std::uint64_t invalid_publishes{0};
        std::size_t reservations{0};
        std::size_t queued{0};
        std::size_t reservation_high_water_mark{0};
        std::size_t queue_high_water_mark{0};
        bool cancelled{false};
    };

    class ZoneTransitionChannel final
    {
    public:
        ZoneTransitionChannel(std::size_t capacity, int wake_descriptor);

        ZoneTransitionChannel(const ZoneTransitionChannel&) = delete;
        ZoneTransitionChannel& operator=(const ZoneTransitionChannel&) = delete;

        [[nodiscard]] std::optional<ZoneTransitionTicket> tryReserve(ZoneHandoffId handoff);
        [[nodiscard]] bool publish(ZoneTransitionTicket ticket, ZoneHandoffCompletion completion) noexcept;
        [[nodiscard]] std::optional<ZoneHandoffCompletion> tryPop();
        void release(ZoneTransitionTicket ticket) noexcept;
        void wakeIfPending() const noexcept;
        void cancel() noexcept;

        [[nodiscard]] ZoneTransitionChannelStats stats() const;
        [[nodiscard]] std::size_t capacity() const noexcept;
        [[nodiscard]] bool drained() const noexcept;

    private:
        struct Reservation
        {
            ZoneHandoffId handoff;
            bool queued{false};
            bool release_when_consumed{false};
        };

        struct QueuedCompletion
        {
            ZoneTransitionTicket ticket;
            ZoneHandoffCompletion completion;
            std::chrono::steady_clock::time_point posted_at;
        };

        void signalWakeUp() const noexcept;

        const std::size_t _capacity;
        const int _wake_descriptor;
        mutable std::mutex _mutex;
        std::vector<std::optional<QueuedCompletion>> _slots;
        std::unordered_map<std::uint64_t, Reservation> _reservations;
        std::size_t _head{0};
        std::size_t _tail{0};
        std::size_t _queued{0};
        std::size_t _reservation_high_water_mark{0};
        std::size_t _queue_high_water_mark{0};
        std::uint64_t _next_ticket{1};
        std::uint64_t _reservations_created{0};
        std::uint64_t _reservations_rejected{0};
        std::uint64_t _completions_published{0};
        std::uint64_t _completions_consumed{0};
        std::uint64_t _invalid_publishes{0};
        snf::runtime::Distribution _queue_wait_nanoseconds;
        bool _cancelled{false};
    };
}
