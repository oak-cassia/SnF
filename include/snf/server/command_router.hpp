#pragma once

#include "snf/server/player_command_ingress.hpp"
#include "snf/server/room_actor_ingress.hpp"
#include "snf/server/routed_command_ingress.hpp"
#include "snf/server/zone_actor_ingress.hpp"

namespace snf::server
{
    class CommandRouter final : public RoutedCommandIngress
    {
    public:
        explicit CommandRouter(PlayerCommandIngress& player_commands) noexcept;
        CommandRouter(PlayerCommandIngress& player_commands, ZoneActorIngress& zone_commands, RoomActorIngress& room_commands) noexcept;

        [[nodiscard]] PostResult tryPost(RoutedCommand command) override;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        PlayerCommandIngress& _player_commands;
        ZoneActorIngress* _zone_commands{nullptr};
        RoomActorIngress* _room_commands{nullptr};
    };
}
