#pragma once

#include "snf/server/command_terminal.hpp"
#include "snf/server/frame_ingress.hpp"
#include "snf/server/player_session_directory.hpp"
#include "snf/server/protocol_zone_result_sink.hpp"
#include "snf/server/route_coordinator.hpp"
#include "snf/server/routed_command_ingress.hpp"
#include "snf/server/zone_transition_channel.hpp"

#include "snf/runtime/distribution.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <unordered_map>

namespace snf::server
{
    struct ZoneHandoffStats
    {
        snf::runtime::DistributionSnapshot transition_nanoseconds;
        std::uint64_t failures_before_source_leave{0};
        std::uint64_t target_failures{0};
        std::uint64_t compensated{0};
        std::uint64_t fatal{0};
        std::uint64_t transition_busy_replies{0};
        std::uint64_t stale_completions{0};
        std::uint64_t disconnect_cleanups{0};
        std::uint64_t shutdown_cancels{0};
        std::size_t pending{0};
    };

    // The cross-zone move, as a state machine of its own. It used to live inside
    // ProtocolGateway, which meant one class owned both frame decoding and a
    // multi-step saga with compensation; the gateway now starts a handoff and asks
    // about one, and nothing more.
    //
    // Reactor-owned, like the coordinators it drives: every method here runs on the
    // reactor thread, which is why none of this state is synchronised.
    class ZoneHandoffService
    {
    public:
        ZoneHandoffService(RoutedCommandIngress& commands,
                           PlayerSessionDirectory& sessions,
                           RouteCoordinator& routes,
                           ZoneTransitionChannel& zone_transitions,
                           CommandLifecycleSink& lifecycle,
                           ProtocolZoneResultSink& zone_results,
                           std::size_t max_completions_per_turn);

        [[nodiscard]] FramePostResult
        tryStart(snf::net::ConnectionId connection, std::uint32_t request_id, PlayerId player, ZoneId target_zone, ZonePosition requested_position, const SessionRoute& source);

        // Answers a Zone request that arrived while a handoff for the same connection
        // is still in flight, and reports whether it did. The reply describes the
        // handoff's target, since that is where the player is going.
        [[nodiscard]] bool tryReplyTransitionInProgress(snf::net::ConnectionId connection, std::uint32_t request_id, ZoneReplyKind kind);

        // True when a handoff is still in flight for this connection, in which case
        // the close cannot be applied yet and the caller retries after cleanup.
        [[nodiscard]] bool noteDisconnect(snf::net::ConnectionId connection) noexcept;

        void drain();
        [[nodiscard]] bool drained() const noexcept;
        [[nodiscard]] ZoneHandoffStats stats() const noexcept;
        void close() noexcept;
        void cancel() noexcept;

    private:
        struct ActiveHandoff
        {
            ZoneTransitionTicket ticket;
            CommandReleaseToken release;
            bool disconnecting{false};
            std::chrono::steady_clock::time_point started_at;
        };

        [[nodiscard]] PostResult postStage(const ZoneHandoff& handoff, ZoneHandoffStep step);
        void handleCompletion(ZoneHandoffCompletion completion);
        void failHandoffBeforeSourceLeave(snf::net::ConnectionId connection, ZoneHandoffId handoff, ZoneCommandStatus status);
        void beginSourceRestore(const ZoneHandoff& handoff);
        void finishSourceRestore(const ZoneHandoff& handoff, ZonePosition position);
        void beginDisconnectCleanup(const ZoneHandoff& handoff, ZoneHandoffStep cleanup_step);
        void finishDisconnectedHandoff(const ZoneHandoff& handoff);
        void finishFatalHandoff(const ZoneHandoff& handoff);
        void finishActiveHandoff(snf::net::ConnectionId connection);
        [[nodiscard]] bool isValidCompletion(const ZoneHandoff& handoff, const ZoneHandoffCompletion& completion) const noexcept;

        RoutedCommandIngress& _commands;
        PlayerSessionDirectory& _sessions;
        RouteCoordinator& _routes;
        ZoneTransitionChannel& _zone_transitions;
        CommandLifecycleSink& _lifecycle;
        ProtocolZoneResultSink& _zone_results;
        std::size_t _max_completions_per_turn;
        std::unordered_map<snf::net::ConnectionId, ActiveHandoff, snf::net::ConnectionIdHash> _active;
        snf::runtime::Distribution _zone_transition_nanoseconds;
        std::uint64_t _handoff_failures_before_source_leave{0};
        std::uint64_t _handoff_target_failures{0};
        std::uint64_t _handoffs_compensated{0};
        std::uint64_t _fatal_handoffs{0};
        std::uint64_t _transition_busy_replies{0};
        std::uint64_t _stale_handoff_completions{0};
        std::uint64_t _disconnect_handoff_cleanups{0};
        std::uint64_t _shutdown_handoff_cancels{0};
        bool _admission_closed{false};
    };
}
