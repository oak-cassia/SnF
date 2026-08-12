#pragma once

#include "snf/server/frame_ingress.hpp"
#include "snf/server/message_dispatcher.hpp"
#include "snf/server/player_session_directory.hpp"
#include "snf/server/route_coordinator.hpp"
#include "snf/server/routed_command_ingress.hpp"
#include "snf/server/zone_timer_service.hpp"

namespace snf::server
{
    class ProtocolGateway final : public FrameIngress
    {
    public:
        explicit ProtocolGateway(RoutedCommandIngress& commands);
        ProtocolGateway(RoutedCommandIngress& commands, PlayerSessionDirectory& sessions);
        ProtocolGateway(RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes);
        ProtocolGateway(RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes,
                        ZoneTimerService& zone_timers);
        ProtocolGateway(MessageDispatcher dispatcher, RoutedCommandIngress& commands);
        ProtocolGateway(MessageDispatcher dispatcher,
                        RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions);
        ProtocolGateway(MessageDispatcher dispatcher,
                        RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes);
        ProtocolGateway(MessageDispatcher dispatcher,
                        RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes,
                        ZoneTimerService& zone_timers);

        [[nodiscard]] FramePostResult tryPost(FrameEnvelope envelope) override;
        [[nodiscard]] PostResult tryPostConnectionClosed(ConnectionClosed closed) override;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        MessageDispatcher _dispatcher;
        RoutedCommandIngress& _commands;
        PlayerSessionDirectory _owned_sessions;
        PlayerSessionDirectory& _sessions;
        RouteCoordinator _owned_routes;
        RouteCoordinator& _routes;
        ZoneTimerService* _zone_timers{nullptr};
    };
}
