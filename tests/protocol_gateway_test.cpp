#include "snf/server/protocol_gateway.hpp"

#include <cassert>
#include <optional>
#include <utility>
#include <variant>

namespace
{
    class RecordingRoutedIngress final : public snf::server::RoutedCommandIngress
    {
    public:
        [[nodiscard]] snf::server::PostResult tryPost(snf::server::RoutedCommand command) override
        {
            posted = std::move(command);
            ++post_count;
            return result;
        }

        void close() noexcept override
        {
            ++close_count;
        }

        void cancel() noexcept override
        {
            ++cancel_count;
        }

        snf::server::PostResult result{snf::server::PostResult::Accepted};
        std::optional<snf::server::RoutedCommand> posted;
        int post_count{0};
        int close_count{0};
        int cancel_count{0};
    };

    snf::server::FrameEnvelope make_frame(const snf::protocol::MessageType type)
    {
        return snf::server::FrameEnvelope{
            .connection = snf::net::ConnectionId{.descriptor = 42, .generation = 11},
            .frame =
                snf::protocol::Frame{
                    .type = type,
                    .request_id = 7,
                    .payload = {std::byte{0xAA}},
                },
        };
    }

    void test_dispatches_and_routes_a_supported_frame()
    {
        RecordingRoutedIngress commands;
        snf::server::ProtocolGateway gateway{commands};

        assert(gateway.tryPost(make_frame(snf::protocol::MessageType::Ping)) ==
               snf::server::FramePostResult::Accepted);
        assert(commands.posted.has_value());
        assert(commands.posted->connection.generation == 11);
        const auto* route = std::get_if<snf::server::PlayerCommandRoute>(&commands.posted->route);
        assert(route != nullptr);
        assert(route->actor.value == 11);
        const auto* ping = std::get_if<snf::server::PingCommand>(&route->command);
        assert(ping != nullptr);
        assert(ping->request_id == 7);
        assert(ping->payload == std::vector<std::byte>{std::byte{0xAA}});
    }

    void test_rejects_unsupported_and_invalid_frames_before_routing()
    {
        RecordingRoutedIngress commands;
        snf::server::MessageDispatcher dispatcher;
        assert(dispatcher.registerHandler(
            snf::protocol::MessageType::Pong,
            [](snf::protocol::Frame) -> std::optional<snf::server::PlayerCommand>
            { return std::nullopt; }));
        snf::server::ProtocolGateway gateway{std::move(dispatcher), commands};

        const snf::protocol::Frame unknown_frame{
            .type = static_cast<snf::protocol::MessageType>(999),
            .request_id = 1,
            .payload = {},
        };
        assert(gateway.tryPost(snf::server::FrameEnvelope{
                   .connection = snf::net::ConnectionId{.descriptor = 1, .generation = 1},
                   .frame = unknown_frame,
               }) == snf::server::FramePostResult::UnsupportedMessage);
        assert(gateway.tryPost(make_frame(snf::protocol::MessageType::Pong)) ==
               snf::server::FramePostResult::InvalidPayload);
        assert(commands.post_count == 0);
    }

    void test_preserves_downstream_capacity_and_lifecycle_results()
    {
        RecordingRoutedIngress commands;
        commands.result = snf::server::PostResult::Full;
        snf::server::ProtocolGateway gateway{commands};

        assert(gateway.tryPost(make_frame(snf::protocol::MessageType::Ping)) ==
               snf::server::FramePostResult::Full);
        commands.result = snf::server::PostResult::Closed;
        assert(gateway.tryPost(make_frame(snf::protocol::MessageType::Ping)) ==
               snf::server::FramePostResult::Closed);

        gateway.close();
        gateway.cancel();
        assert(commands.close_count == 1);
        assert(commands.cancel_count == 1);
    }

    void test_routes_connection_closed_with_the_same_provisional_actor_id()
    {
        RecordingRoutedIngress commands;
        commands.result = snf::server::PostResult::Full;
        snf::server::ProtocolGateway gateway{commands};
        const snf::server::ConnectionClosed closed{
            .connection = snf::net::ConnectionId{.descriptor = 8, .generation = 12},
            .cause = snf::server::ConnectionCloseCause::Overflow,
        };

        assert(gateway.tryPostConnectionClosed(closed) == snf::server::PostResult::Full);
        assert(commands.posted.has_value());
        assert(commands.posted->connection == closed.connection);
        const auto* route =
            std::get_if<snf::server::ConnectionClosedRoute>(&commands.posted->route);
        assert(route != nullptr);
        assert(route->actor == snf::server::provisionalActorIdFor(closed.connection));
        assert(route->cause == closed.cause);
    }
}

void run_protocol_gateway_tests()
{
    test_dispatches_and_routes_a_supported_frame();
    test_rejects_unsupported_and_invalid_frames_before_routing();
    test_preserves_downstream_capacity_and_lifecycle_results();
    test_routes_connection_closed_with_the_same_provisional_actor_id();
}
