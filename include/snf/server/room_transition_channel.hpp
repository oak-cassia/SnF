#pragma once

#include "snf/game/zone_result.hpp"
#include "snf/runtime/distribution.hpp"
#include "snf/server/room_entry.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace snf::server
{
    struct RoomTransitionCompletion
    {
        RoomEntryId entry_id{};
        RoomReturnId return_id{};
        snf::net::ConnectionId connection{};
        PlayerId player{};
        RoomId room{};
        ZoneId zone{};
        std::uint64_t route_epoch{0};
        RoomEntryStep step{RoomEntryStep::JoinRoom};
        RoomCommandStatus room_status{RoomCommandStatus::Applied};
        ZoneCommandStatus zone_status{ZoneCommandStatus::Applied};
        std::optional<ZonePosition> position{};
    };

    [[nodiscard]] inline RoomTransitionCompletion completionFrom(const RoomEntryContext& context) noexcept
    {
        return RoomTransitionCompletion{
            .entry_id = context.entry_id,
            .return_id = context.return_id,
            .connection = context.connection,
            .player = context.player,
            .step = context.step,
        };
    }

    struct RoomReturnRequest
    {
        RoomId room{};
        PlayerId player{};
    };

    struct RoomTransitionChannelStats
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
        std::uint64_t return_requests_published{0};
        std::uint64_t return_requests_consumed{0};
        std::uint64_t return_requests_rejected{0};
        std::size_t return_requests_queued{0};
        std::size_t return_request_high_water_mark{0};
        bool cancelled{false};
    };

    class RoomTransitionChannel final
    {
    public:
        RoomTransitionChannel(std::size_t capacity, int wake_descriptor);

        RoomTransitionChannel(const RoomTransitionChannel&) = delete;
        RoomTransitionChannel& operator=(const RoomTransitionChannel&) = delete;

        [[nodiscard]] std::optional<RoomTransitionTicket> tryReserve(RoomEntryId entry);
        [[nodiscard]] std::optional<RoomTransitionTicket> tryReserve(RoomReturnId return_id);
        [[nodiscard]] bool publish(RoomTransitionTicket ticket, RoomTransitionCompletion completion) noexcept;
        [[nodiscard]] std::optional<RoomTransitionCompletion> tryPop();
        [[nodiscard]] bool tryPublishReturnRequest(RoomReturnRequest request) noexcept;
        [[nodiscard]] std::optional<RoomReturnRequest> tryPopReturnRequest();
        void release(RoomTransitionTicket ticket) noexcept;
        void wakeIfPending() const noexcept;
        void cancel() noexcept;

        [[nodiscard]] RoomTransitionChannelStats stats() const;
        [[nodiscard]] std::size_t capacity() const noexcept;
        [[nodiscard]] bool drained() const noexcept;

    private:
        struct Reservation
        {
            std::uint64_t correlation_id{0};
            bool queued{false};
            bool release_when_consumed{false};
        };

        struct QueuedCompletion
        {
            RoomTransitionTicket ticket;
            RoomTransitionCompletion completion;
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
        std::vector<std::optional<RoomReturnRequest>> _return_requests;
        std::size_t _return_head{0};
        std::size_t _return_tail{0};
        std::size_t _return_queued{0};
        std::size_t _return_request_high_water_mark{0};
        std::uint64_t _return_requests_published{0};
        std::uint64_t _return_requests_consumed{0};
        std::uint64_t _return_requests_rejected{0};
        snf::runtime::Distribution _queue_wait_nanoseconds;
        bool _cancelled{false};
    };
}
