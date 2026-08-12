#pragma once

#include "snf/server/player_command_ingress.hpp"
#include "snf/server/routed_command_ingress.hpp"
#include "snf/server/zone_actor_ingress.hpp"

namespace snf::server
{
    // Routes typed commands to the owning runtime. Only PlayerCommandRoute is
    // implemented until Session, World, and Battle runtimes exist.
    class CommandRouter final : public RoutedCommandIngress
    {
    public:
        explicit CommandRouter(PlayerCommandIngress& player_commands) noexcept;
        CommandRouter(PlayerCommandIngress& player_commands,
                      ZoneActorIngress& zone_commands) noexcept;

        [[nodiscard]] PostResult tryPost(RoutedCommand command) override;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        PlayerCommandIngress& _player_commands;
        ZoneActorIngress* _zone_commands{nullptr};
    };
}
