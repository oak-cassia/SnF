#include "snf/server/command_router.hpp"
#include "snf/server/player_command_ingress.hpp"

#include <cassert>
#include <optional>
#include <utility>
#include <variant>

namespace
{
    class RecordingCommandIngress final : public snf::server::PlayerCommandIngress
    {
    public:
        [[nodiscard]] snf::runtime::PostResult
        tryPost(snf::server::PlayerInboundCommand command) override
        {
            posted = std::move(command);
            return result;
        }

        [[nodiscard]] snf::runtime::PostResult
        tryPostConnectionClosed(snf::server::ProvisionalActorId actor,
                                snf::server::ConnectionClosed closed) override
        {
            closed_actor = actor;
            closed_connection = closed;
            return lifecycle_result;
        }

        void close() noexcept override
        {
            ++close_count;
        }

        void cancel() noexcept override
        {
            ++cancel_count;
        }

        snf::runtime::PostResult result{snf::runtime::PostResult::Accepted};
        snf::runtime::PostResult lifecycle_result{snf::runtime::PostResult::Accepted};
        std::optional<snf::server::PlayerInboundCommand> posted;
        std::optional<snf::server::ProvisionalActorId> closed_actor;
        std::optional<snf::server::ConnectionClosed> closed_connection;
        int close_count{0};
        int cancel_count{0};
    };

    void test_routes_player_command_and_preserves_post_result()
    {
        RecordingCommandIngress player_commands;
        player_commands.result = snf::runtime::PostResult::Full;
        snf::server::CommandRouter router{player_commands};

        const auto result = router.tryPost(snf::server::RoutedCommand{
            .connection = snf::net::ConnectionId{.descriptor = 42, .generation = 9},
            .route =
                snf::server::PlayerCommandRoute{
                    .actor = snf::server::ProvisionalActorId{.value = 77},
                    .command = snf::server::PingCommand{.request_id = 5, .payload = {}},
                },
        });

        assert(result == snf::runtime::PostResult::Full);
        assert(player_commands.posted.has_value());
        assert(player_commands.posted->actor.value == 77);
        assert(player_commands.posted->connection.descriptor == 42);
        assert(player_commands.posted->connection.generation == 9);
        const auto* ping = std::get_if<snf::server::PingCommand>(&player_commands.posted->command);
        assert(ping != nullptr);
        assert(ping->request_id == 5);
    }

    void test_forwards_lifecycle()
    {
        RecordingCommandIngress player_commands;
        snf::server::CommandRouter router{player_commands};

        router.close();
        router.cancel();

        assert(player_commands.close_count == 1);
        assert(player_commands.cancel_count == 1);
    }

    void test_routes_connection_closed_and_preserves_post_result()
    {
        RecordingCommandIngress player_commands;
        player_commands.lifecycle_result = snf::runtime::PostResult::Full;
        snf::server::CommandRouter router{player_commands};

        const auto result = router.tryPost(snf::server::RoutedCommand{
            .connection = snf::net::ConnectionId{.descriptor = 43, .generation = 10},
            .route =
                snf::server::ConnectionClosedRoute{
                    .actor = snf::server::ProvisionalActorId{.value = 77},
                    .cause = snf::server::ConnectionCloseCause::ProtocolError,
                },
        });

        assert(result == snf::runtime::PostResult::Full);
        assert(player_commands.closed_actor.has_value());
        assert(player_commands.closed_actor->value == 77);
        assert(player_commands.closed_connection.has_value());
        assert(player_commands.closed_connection->connection.descriptor == 43);
        assert(player_commands.closed_connection->connection.generation == 10);
        assert(player_commands.closed_connection->cause ==
               snf::server::ConnectionCloseCause::ProtocolError);
    }
}

void run_command_router_tests()
{
    test_routes_player_command_and_preserves_post_result();
    test_forwards_lifecycle();
    test_routes_connection_closed_and_preserves_post_result();
}
