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

    // The saga identity, copied whole. Both publishing sites used to spell it out field
    // by field, and one of them omitted return_id -- which made every return completion
    // correlate against zero, fail the ticket check, and take the server down with it.
    // The outcome fields are the caller's to fill in; the identity is not.
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

    // What a cleared Room hands the reactor: this participant is in no Zone and has to
    // be put back. It carries no ticket, because a clear is not a step of a saga that
    // reserved one, so this queue is bounded on its own.
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

    // A reactor admission reserves one ticket for a room entry handoff's whole lifetime.
    // Only one step is in flight, so that ticket guarantees one allocation-free
    // Worker completion slot at a time.
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
        // The Worker side of a clear. It only states the fact; starting the return is
        // the reactor's, because the route state the return moves is the reactor's. A
        // refusal is a logic failure for the caller to report, like a refused publish:
        // dropping it would leave a player in no Zone with nothing to put them back.
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
        // Sized like the completion ring. A player can only be in a Room after an entry
        // admitted them, and both are bounded by the same connection capacity.
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
