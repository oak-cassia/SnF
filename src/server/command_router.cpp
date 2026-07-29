#include "snf/server/command_router.hpp"

#include <type_traits>
#include <utility>
#include <variant>

namespace
{
    template <typename> inline constexpr bool always_false_v = false;
}

namespace snf::server
{
    CommandRouter::CommandRouter(CommandIngress& player_commands) noexcept
        : _player_commands(player_commands)
    {
    }

    PostResult CommandRouter::tryPost(RoutedCommand command)
    {
        return std::visit(
            [this, connection = command.connection](auto route) mutable -> PostResult
            {
                using Route = std::decay_t<decltype(route)>;
                if constexpr (std::is_same_v<Route, PlayerCommandRoute>)
                {
                    return _player_commands.tryPost(InboundCommand{
                        .actor = route.actor,
                        .connection = connection,
                        .command = std::move(route.command),
                    });
                }
                else if constexpr (std::is_same_v<Route, ConnectionClosedRoute>)
                {
                    return _player_commands.tryPostConnectionClosed(route.actor,
                                                                    ConnectionClosed{
                                                                        .connection = connection,
                                                                        .cause = route.cause,
                                                                    });
                }
                else
                {
                    static_assert(always_false_v<Route>, "Unhandled CommandRoute alternative");
                }
            },
            std::move(command.route));
    }

    void CommandRouter::close() noexcept
    {
        _player_commands.close();
    }

    void CommandRouter::cancel() noexcept
    {
        _player_commands.cancel();
    }
}
