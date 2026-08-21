#pragma once

#include "snf/server/frame_ingress.hpp"
#include "snf/server/message_dispatcher.hpp"
#include "snf/server/party_coordinator.hpp"
#include "snf/server/player_session_directory.hpp"
#include "snf/server/protocol_zone_result_sink.hpp"
#include "snf/server/route_coordinator.hpp"
#include "snf/server/routed_command_ingress.hpp"
#include "snf/server/zone_handoff_service.hpp"

namespace snf::server
{
    struct ProtocolGatewayConfig
    {
        MessageDispatcher dispatcher{};
    };

    // Every dependency is injected, so the gateway can only exist fully assembled.
    // Nothing here is optional: a half-wired gateway used to answer a wiring mistake
    // with a wire-visible protocol error, which is no longer representable.
    class ProtocolGateway final : public FrameIngress
    {
    public:
        ProtocolGateway(RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes,
                        PartyCoordinator& parties,
                        ZoneHandoffService& handoffs,
                        ProtocolZoneResultSink& zone_results,
                        ProtocolGatewayConfig config);

        [[nodiscard]] FramePostResult tryPost(FrameEnvelope envelope) override;
        [[nodiscard]] PostResult tryPostConnectionClosed(ConnectionClosed closed) override;
        void drainZoneTransitions();
        [[nodiscard]] bool zoneTransitionsDrained() const noexcept;
        [[nodiscard]] ZoneHandoffStats zoneHandoffStats() const noexcept;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        MessageDispatcher _dispatcher;
        RoutedCommandIngress& _commands;
        PlayerSessionDirectory& _sessions;
        RouteCoordinator& _routes;
        PartyCoordinator& _parties;
        ZoneHandoffService& _handoffs;
        ProtocolZoneResultSink& _zone_results;
    };
}
