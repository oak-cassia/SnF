#pragma once

#include "snf/server/command_terminal.hpp"
#include "snf/server/frame_ingress.hpp"
#include "snf/server/message_dispatcher.hpp"
#include "snf/server/party_coordinator.hpp"
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
    struct ZoneHandoffGatewayStats
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

    class ProtocolGateway final : public FrameIngress
    {
    public:
        explicit ProtocolGateway(RoutedCommandIngress& commands);
        ProtocolGateway(RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes);
        ProtocolGateway(RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes,
                        PartyCoordinator& parties,
                        ZoneTransitionChannel& zone_transitions,
                        CommandLifecycleSink& lifecycle,
                        ProtocolZoneResultSink& zone_results,
                        std::size_t max_zone_completions_per_turn);
        ProtocolGateway(MessageDispatcher dispatcher, RoutedCommandIngress& commands);
        ProtocolGateway(MessageDispatcher dispatcher,
                        RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes);
        ProtocolGateway(MessageDispatcher dispatcher,
                        RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes,
                        PartyCoordinator& parties);

        [[nodiscard]] FramePostResult tryPost(FrameEnvelope envelope) override;
        [[nodiscard]] PostResult tryPostConnectionClosed(ConnectionClosed closed) override;
        void drainZoneTransitions();
        [[nodiscard]] bool zoneTransitionsDrained() const noexcept;
        [[nodiscard]] ZoneHandoffGatewayStats zoneHandoffStats() const noexcept;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        struct ActiveZoneHandoff
        {
            ZoneTransitionTicket ticket;
            CommandReleaseToken release;
            bool disconnecting{false};
            std::chrono::steady_clock::time_point started_at;
        };

        [[nodiscard]] FramePostResult tryStartZoneHandoff(const FrameEnvelope& envelope,
                                                          PlayerId player,
                                                          ZoneId target_zone,
                                                          ZonePosition requested_position,
                                                          const SessionRoute& source);
        [[nodiscard]] PostResult postZoneHandoffStage(const ZoneHandoff& handoff,
                                                      ZoneHandoffStep step);
        void handleZoneHandoffCompletion(ZoneHandoffCompletion completion);
        void failHandoffBeforeSourceLeave(snf::net::ConnectionId connection,
                                          ZoneHandoffId handoff,
                                          ZoneCommandStatus status);
        void beginSourceRestore(const ZoneHandoff& handoff);
        void finishSourceRestore(const ZoneHandoff& handoff, ZonePosition position);
        void beginDisconnectCleanup(const ZoneHandoff& handoff, ZoneHandoffStep cleanup_step);
        void finishDisconnectedHandoff(const ZoneHandoff& handoff);
        void finishFatalHandoff(const ZoneHandoff& handoff);
        void finishActiveHandoff(snf::net::ConnectionId connection);
        void replyZoneStatus(snf::net::ConnectionId connection,
                             PlayerId player,
                             ZoneId zone,
                             std::uint64_t route_epoch,
                             ZonePosition position,
                             std::uint32_t request_id,
                             ZoneReplyKind kind,
                             ZoneCommandStatus status);
        [[nodiscard]] bool
        isValidCompletion(const ZoneHandoff& handoff,
                          const ZoneHandoffCompletion& completion) const noexcept;

        MessageDispatcher _dispatcher;
        RoutedCommandIngress& _commands;
        PlayerSessionDirectory _owned_sessions;
        PlayerSessionDirectory& _sessions;
        RouteCoordinator _owned_routes;
        RouteCoordinator& _routes;
        PartyCoordinator _owned_parties;
        PartyCoordinator& _parties;
        ZoneTransitionChannel* _zone_transitions{nullptr};
        CommandLifecycleSink* _lifecycle{nullptr};
        ProtocolZoneResultSink* _zone_results{nullptr};
        std::size_t _max_zone_completions_per_turn{0};
        std::unordered_map<snf::net::ConnectionId, ActiveZoneHandoff, snf::net::ConnectionIdHash>
            _active_zone_handoffs;
        snf::runtime::Distribution _zone_transition_nanoseconds;
        std::uint64_t _handoff_failures_before_source_leave{0};
        std::uint64_t _handoff_target_failures{0};
        std::uint64_t _handoffs_compensated{0};
        std::uint64_t _fatal_handoffs{0};
        std::uint64_t _transition_busy_replies{0};
        std::uint64_t _stale_handoff_completions{0};
        std::uint64_t _disconnect_handoff_cleanups{0};
        std::uint64_t _shutdown_handoff_cancels{0};
        bool _handoff_admission_closed{false};
    };
}
