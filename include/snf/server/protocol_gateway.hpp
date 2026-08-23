#pragma once

#include "snf/server/frame_ingress.hpp"
#include "snf/server/message_dispatcher.hpp"
#include "snf/server/player_session_directory.hpp"
#include "snf/server/protocol_zone_result_sink.hpp"
#include "snf/server/room_entry_service.hpp"
#include "snf/server/route_coordinator.hpp"
#include "snf/server/routed_command_ingress.hpp"
#include "snf/server/zone_handoff_service.hpp"

namespace snf::server
{
    struct ProtocolGatewayConfig
    {
        MessageDispatcher dispatcher{};
    };

    class ProtocolGateway final : public FrameIngress
    {
    public:
        ProtocolGateway(RoutedCommandIngress& commands,
                        PlayerSessionDirectory& sessions,
                        RouteCoordinator& routes,
                        ZoneHandoffService& handoffs,
                        RoomEntryService& room_entries,
                        ProtocolGatewayConfig config);

        [[nodiscard]] FramePostResult tryPost(FrameEnvelope envelope) override;
        [[nodiscard]] PostResult tryPostConnectionClosed(ConnectionClosed closed) override;
        void drainTransitions();
        [[nodiscard]] bool transitionsDrained() const noexcept;
        [[nodiscard]] ZoneHandoffStats zoneHandoffStats() const noexcept;
        [[nodiscard]] RoomEntryStats roomEntryStats() const noexcept;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        MessageDispatcher _dispatcher;
        RoutedCommandIngress& _commands;
        PlayerSessionDirectory& _sessions;
        RouteCoordinator& _routes;
        ZoneHandoffService& _handoffs;
        RoomEntryService& _room_entries;
    };
}
