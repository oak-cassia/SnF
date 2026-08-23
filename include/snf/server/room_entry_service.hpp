#pragma once

#include "snf/runtime/distribution.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/frame_ingress.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_session_directory.hpp"
#include "snf/server/protocol_zone_result_sink.hpp"
#include "snf/server/room_entry.hpp"
#include "snf/server/room_transition_channel.hpp"
#include "snf/server/route_coordinator.hpp"
#include "snf/server/routed_command_ingress.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <unordered_map>

namespace snf::server
{
    struct RoomEntryStats
    {
        snf::runtime::DistributionSnapshot transition_nanoseconds;
        std::uint64_t failures_before_source_leave{0};
        std::uint64_t source_leave_failures{0};
        std::uint64_t return_failures{0};
        std::uint64_t compensated{0};
        std::uint64_t transition_busy_replies{0};
        std::uint64_t stale_completions{0};
        std::uint64_t disconnect_cleanups{0};
        std::uint64_t shutdown_cancels{0};
        std::size_t pending{0};
    };

    class RoomEntryService
    {
    public:
        RoomEntryService(RoutedCommandIngress& commands,
                         PlayerSessionDirectory& sessions,
                         RouteCoordinator& routes,
                         RoomTransitionChannel& room_transitions,
                         CommandLifecycleSink& lifecycle,
                         OutboundSink& outbound,
                         ProtocolZoneResultSink& zone_results,
                         std::size_t max_completions_per_turn);

        [[nodiscard]] FramePostResult
        tryStart(snf::net::ConnectionId connection, std::uint32_t request_id, PlayerId player, RoomId room);

        [[nodiscard]] bool tryReplyRoomBusy(snf::net::ConnectionId connection, std::uint32_t request_id, RoomReplyKind kind);

        [[nodiscard]] bool tryReplyZoneBlockedByRoom(snf::net::ConnectionId connection, std::uint32_t request_id, ZoneReplyKind kind);

        [[nodiscard]] bool noteDisconnect(snf::net::ConnectionId connection) noexcept;

        void startReturn(snf::net::ConnectionId connection, RoomId room);

        void drain();
        [[nodiscard]] bool drained() const noexcept;
        [[nodiscard]] RoomEntryStats stats() const noexcept;
        void close() noexcept;
        void cancel() noexcept;

    private:
        struct ActiveEntry
        {
            snf::net::ConnectionId connection;
            RoomTransitionTicket ticket;
            CommandReleaseToken release;
            RoomEntryId entry_id;
            PlayerId player;
            RoomId room;
            ZoneId source_zone;
            std::uint64_t source_epoch{0};
            std::uint32_t request_id{0};
            bool disconnecting{false};
            std::chrono::steady_clock::time_point started_at;
        };

        struct ActiveReturn
        {
            snf::net::ConnectionId connection;
            RoomTransitionTicket ticket;
            RoomReturnId return_id;
            PlayerId player;
            RoomId room;
            ZoneId return_zone;
            std::uint64_t return_epoch{0};
            ZonePosition return_position;
            bool disconnecting{false};
            std::chrono::steady_clock::time_point started_at;
        };

        void handleCompletion(RoomTransitionCompletion completion);
        void handleJoinRoomCompletion(const RoomTransitionCompletion& completion, ActiveEntry& active);
        void handleLeaveSourceCompletion(const RoomTransitionCompletion& completion, ActiveEntry& active);
        void handleReturnZoneCompletion(const RoomTransitionCompletion& completion, ActiveReturn& active_return);

        void replyRoomJoined(snf::net::ConnectionId connection, std::uint32_t request_id, RoomId room, RoomCommandStatus status, RoomPhase phase);
        void replyReturnedToZone(snf::net::ConnectionId connection, ZoneId zone, ZonePosition position);
        [[nodiscard]] bool sendFrame(snf::net::ConnectionId connection, snf::protocol::Frame frame);

        void failEntryBeforeSourceLeave(snf::net::ConnectionId connection, RoomId room, std::uint32_t request_id, RoomCommandStatus status);
        void compensateFailedSourceLeave(ActiveEntry& active);

        RoutedCommandIngress& _commands;
        PlayerSessionDirectory& _sessions;
        RouteCoordinator& _routes;
        RoomTransitionChannel& _room_transitions;
        CommandLifecycleSink& _lifecycle;
        OutboundSink& _outbound;
        ProtocolZoneResultSink& _zone_results;
        std::size_t _max_completions_per_turn;

        std::unordered_map<snf::net::ConnectionId, ActiveEntry, snf::net::ConnectionIdHash> _active_entries;
        std::unordered_map<snf::net::ConnectionId, ActiveReturn, snf::net::ConnectionIdHash> _active_returns;

        snf::runtime::Distribution _transition_nanoseconds;
        std::uint64_t _failures_before_source_leave{0};
        std::uint64_t _source_leave_failures{0};
        std::uint64_t _return_failures{0};
        std::uint64_t _compensated{0};
        std::uint64_t _transition_busy_replies{0};
        std::uint64_t _stale_completions{0};
        std::uint64_t _disconnect_cleanups{0};
        std::uint64_t _shutdown_cancels{0};
        bool _admission_closed{false};
    };
}
