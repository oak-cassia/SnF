#pragma once

#include "snf/server/command_ingress.hpp"
#include "snf/server/routed_command_ingress.hpp"

namespace snf::server
{
    // Routes typed commands to the owning runtime. Only PlayerCommandRoute is
    // implemented until Session, World, and Battle runtimes exist.
    class CommandRouter final : public RoutedCommandIngress
    {
    public:
        explicit CommandRouter(CommandIngress& player_commands) noexcept;

        [[nodiscard]] PostResult tryPost(RoutedCommand command) override;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        CommandIngress& _player_commands;
    };
}
