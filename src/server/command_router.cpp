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
    CommandRouter::CommandRouter(PlayerCommandIngress& player_commands) noexcept
        : _player_commands(player_commands)
    {
    }

    CommandRouter::CommandRouter(PlayerCommandIngress& player_commands, ZoneActorIngress& zone_commands, RoomActorIngress& room_commands) noexcept
        : _player_commands(player_commands)
        , _zone_commands(&zone_commands)
        , _room_commands(&room_commands)
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
                    return _player_commands.tryPost(PlayerInboundCommand{
                        .actor = route.actor,
                        .connection = connection,
                        .command = std::move(route.command),
                        .request_id = route.request_id,
                        .room_entry = route.room_entry,
                    });
                }
                else if constexpr (std::is_same_v<Route, ConnectionClosedRoute>)
                {
                    return _player_commands.tryPostConnectionClosed(route.actor,
                                                                    ConnectionClosed{
                                                                        .connection = connection,
                                                                        .cause = route.cause,
                                                                        .has_location_snapshot = route.has_location_snapshot,
                                                                        .last_location = route.last_location,
                                                                    });
                }
                else if constexpr (std::is_same_v<Route, ZoneCommandRoute>)
                {
                    if (_zone_commands == nullptr)
                    {
                        return PostResult::Closed;
                    }

                    std::optional<ZoneReplyContext> reply;
                    if (route.reply_kind)
                    {
                        reply = ZoneReplyContext{
                            .connection = connection,
                            .request_id = route.request_id,
                            .kind = *route.reply_kind,
                        };
                    }

                    return _zone_commands->tryPost(ZoneInboundCommand{
                        .zone = route.zone,
                        .command = std::move(route.command),
                        .reply = std::move(reply),
                        .handoff = std::nullopt,
                    });
                }
                else if constexpr (std::is_same_v<Route, RoomCommandRoute>)
                {
                    if (_room_commands == nullptr)
                    {
                        return PostResult::Closed;
                    }

                    std::optional<RoomReplyContext> reply;
                    if (route.reply_kind)
                    {
                        reply = RoomReplyContext{
                            .connection = connection,
                            .request_id = route.request_id,
                            .kind = *route.reply_kind,
                        };
                    }
                    return _room_commands->tryPost(RoomInboundCommand{
                        .room = route.room,
                        .command = std::move(route.command),
                        .reply = std::move(reply),
                    });
                }
                else if constexpr (std::is_same_v<Route, ZoneHandoffCommandRoute>)
                {
                    if (_zone_commands == nullptr || (!route.command.handoff && !route.command.room_entry) || route.command.reply)
                    {
                        return PostResult::Closed;
                    }
                    return _zone_commands->tryPost(std::move(route.command));
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
