#include "outbound_reservation_test_support.hpp"
#include "snf/server/outbound_channel.hpp"
#include "snf/server/protocol_gateway.hpp"

#include <cassert>
#include <cstdint>
#include <deque>
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
            route_indices.push_back(command.route.index());
            posted = std::move(command);
            ++post_count;
            if (!results.empty())
            {
                const snf::server::PostResult next = results.front();
                results.pop_front();
                return next;
            }
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
        std::deque<snf::server::PostResult> results;
        std::optional<snf::server::RoutedCommand> posted;
        std::vector<std::size_t> route_indices;
        int post_count{0};
        int close_count{0};
        int cancel_count{0};
    };

    struct GatewayFixtureConfig
    {
        std::size_t max_handoffs{64};
        std::size_t transition_capacity{64};
        std::size_t max_party_members{8};
        std::size_t max_zone_completions_per_turn{64};
        std::size_t max_room_entries{64};
        std::size_t max_room_entry_completions_per_turn{64};
        snf::server::MessageDispatcher dispatcher{};
    };

    // The gateway no longer keeps stand-ins for the dependencies it requires, so the
    // test owns them. One declaration is enough for a case that only drives frames,
    // and the handoff cases lower the capacities they want to exhaust.
    struct GatewayFixture
    {
        explicit GatewayFixture(GatewayFixtureConfig config = {})
            : wake(snf::test::make_wake_descriptor())
            , routes(config.max_handoffs)
            , parties(config.max_party_members)
            , transitions(config.transition_capacity, wake.getDescriptor())
            , room_transitions(config.max_room_entries, wake.getDescriptor())
            , outbound(snf::server::OutboundChannelConfig{.capacity = 4, .max_slots_per_connection = 4}, wake.getDescriptor())
            , zone_results(outbound)
            , handoffs(commands, sessions, routes, transitions, lifecycle, zone_results, config.max_zone_completions_per_turn)
            , room_entries(
                  commands, sessions, routes, room_transitions, lifecycle, outbound, zone_results, config.max_room_entry_completions_per_turn
              )
            , gateway(
                  commands,
                  sessions,
                  routes,
                  parties,
                  handoffs,
                  room_entries,
                  snf::server::ProtocolGatewayConfig{
                      .dispatcher = std::move(config.dispatcher),
                  }
              )
        {
        }

        snf::net::UniqueFileDescriptor wake;
        RecordingRoutedIngress commands;
        snf::server::PlayerSessionDirectory sessions;
        snf::server::RouteCoordinator routes;
        snf::server::PartyCoordinator parties;
        snf::server::ZoneTransitionChannel transitions;
        snf::server::RoomTransitionChannel room_transitions;
        snf::server::OutboundChannel outbound;
        snf::server::ProtocolZoneResultSink zone_results;
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::ZoneHandoffService handoffs;
        snf::server::RoomEntryService room_entries;
        snf::server::ProtocolGateway gateway;
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

    std::vector<std::byte> player_id_payload(const std::uint64_t value)
    {
        std::vector<std::byte> payload(8);
        std::uint64_t remaining = value;
        for (std::size_t index = payload.size(); index > 0; --index)
        {
            payload[index - 1] = static_cast<std::byte>(remaining & 0xFFU);
            remaining >>= 8U;
        }
        return payload;
    }

    void append_u32(std::vector<std::byte>& payload, const std::uint32_t value)
    {
        payload.push_back(static_cast<std::byte>((value >> 24U) & 0xFFU));
        payload.push_back(static_cast<std::byte>((value >> 16U) & 0xFFU));
        payload.push_back(static_cast<std::byte>((value >> 8U) & 0xFFU));
        payload.push_back(static_cast<std::byte>(value & 0xFFU));
    }

    void append_u64(std::vector<std::byte>& payload, const std::uint64_t value)
    {
        append_u32(payload, static_cast<std::uint32_t>(value >> 32U));
        append_u32(payload, static_cast<std::uint32_t>(value));
    }

    snf::server::FrameEnvelope
    make_enter_frame(const snf::net::ConnectionId connection, const std::uint64_t zone, const std::int32_t x, const std::int32_t y)
    {
        std::vector<std::byte> payload;
        append_u64(payload, zone);
        append_u32(payload, static_cast<std::uint32_t>(x));
        append_u32(payload, static_cast<std::uint32_t>(y));
        return snf::server::FrameEnvelope{
            .connection = connection,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::EnterZone,
                    .request_id = 30,
                    .payload = std::move(payload),
                },
        };
    }

    snf::server::FrameEnvelope make_move_frame(const snf::net::ConnectionId connection, const std::int32_t x, const std::int32_t y)
    {
        std::vector<std::byte> payload;
        append_u32(payload, static_cast<std::uint32_t>(x));
        append_u32(payload, static_cast<std::uint32_t>(y));
        return snf::server::FrameEnvelope{
            .connection = connection,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::Move,
                    .request_id = 31,
                    .payload = std::move(payload),
                },
        };
    }

    snf::server::FrameEnvelope make_auth_frame(const snf::net::ConnectionId connection, const snf::server::PlayerId player)
    {
        return snf::server::FrameEnvelope{
            .connection = connection,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::Authenticate,
                    .request_id = 8,
                    .payload = player_id_payload(player.value),
                },
        };
    }

    snf::server::FrameEnvelope make_purchase_frame(const snf::net::ConnectionId connection, const std::uint64_t key, const std::uint32_t product)
    {
        std::vector<std::byte> payload;
        append_u64(payload, key);
        append_u32(payload, product);
        return snf::server::FrameEnvelope{
            .connection = connection,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::Purchase,
                    .request_id = 9,
                    .payload = std::move(payload),
                },
        };
    }

    snf::server::FrameEnvelope make_party_join_frame(const snf::net::ConnectionId connection, const std::uint64_t party)
    {
        return snf::server::FrameEnvelope{
            .connection = connection,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::PartyJoin,
                    .request_id = 10,
                    .payload = player_id_payload(party),
                },
        };
    }

    void test_dispatches_and_routes_a_supported_frame()
    {
        GatewayFixture fixture;

        assert(fixture.gateway.tryPost(make_frame(snf::protocol::MessageType::Ping)) == snf::server::FramePostResult::Accepted);
        assert(fixture.commands.posted.has_value());
        assert(fixture.commands.posted->connection.generation == 11);
        const auto* route = std::get_if<snf::server::PlayerCommandRoute>(&fixture.commands.posted->route);
        assert(route != nullptr);
        assert(route->actor.value == 11);
        const auto* ping = std::get_if<snf::server::PingCommand>(&route->command);
        assert(ping != nullptr);
        assert(route->request_id == 7);
        assert(ping->payload == std::vector<std::byte>{std::byte{0xAA}});
    }

    void test_rejects_unsupported_and_invalid_frames_before_routing()
    {
        snf::server::MessageDispatcher dispatcher;
        assert(dispatcher.registerHandler(
            snf::protocol::MessageType::Pong,
            [](snf::protocol::Frame) -> std::optional<snf::server::PlayerCommand>
            {
                return std::nullopt;
            }
        ));
        GatewayFixture fixture{GatewayFixtureConfig{.dispatcher = std::move(dispatcher)}};

        const snf::protocol::Frame unknown_frame{
            .type = static_cast<snf::protocol::MessageType>(999),
            .request_id = 1,
            .payload = {},
        };
        assert(
            fixture.gateway.tryPost(snf::server::FrameEnvelope{
                .connection = snf::net::ConnectionId{.descriptor = 1, .generation = 1},
                .frame = unknown_frame,
            }) == snf::server::FramePostResult::UnsupportedMessage
        );
        assert(fixture.gateway.tryPost(make_frame(snf::protocol::MessageType::Pong)) == snf::server::FramePostResult::InvalidPayload);
        assert(fixture.commands.post_count == 0);
    }

    void test_preserves_downstream_capacity_and_lifecycle_results()
    {
        GatewayFixture fixture;
        fixture.commands.result = snf::server::PostResult::Full;

        assert(fixture.gateway.tryPost(make_frame(snf::protocol::MessageType::Ping)) == snf::server::FramePostResult::Full);
        fixture.commands.result = snf::server::PostResult::Closed;
        assert(fixture.gateway.tryPost(make_frame(snf::protocol::MessageType::Ping)) == snf::server::FramePostResult::Closed);

        fixture.gateway.close();
        fixture.gateway.cancel();
        assert(fixture.commands.close_count == 1);
        assert(fixture.commands.cancel_count == 1);
    }

    void test_routes_connection_closed_with_the_same_provisional_actor_id()
    {
        GatewayFixture fixture;
        fixture.commands.result = snf::server::PostResult::Full;
        const snf::server::ConnectionClosed closed{
            .connection = snf::net::ConnectionId{.descriptor = 8, .generation = 12},
            .cause = snf::server::ConnectionCloseCause::Overflow,
            .has_location_snapshot = false,
            .last_location = std::nullopt,
        };

        assert(fixture.gateway.tryPostConnectionClosed(closed) == snf::server::PostResult::Full);
        assert(fixture.commands.posted.has_value());
        assert(fixture.commands.posted->connection == closed.connection);
        const auto* route = std::get_if<snf::server::ConnectionClosedRoute>(&fixture.commands.posted->route);
        assert(route != nullptr);
        assert(route->actor == snf::server::provisionalActorIdFor(closed.connection));
        assert(route->cause == closed.cause);
    }

    void test_authentication_attaches_and_routes_to_a_persistent_player()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 50, .generation = 20};
        const snf::server::PlayerId player{.value = 77};

        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        auto* route = std::get_if<snf::server::PlayerCommandRoute>(&fixture.commands.posted->route);
        assert(route != nullptr);
        assert(route->actor == player);
        assert(std::holds_alternative<snf::server::AuthenticateCommand>(route->command));

        auto ping = make_frame(snf::protocol::MessageType::Ping);
        ping.connection = connection;
        assert(fixture.gateway.tryPost(std::move(ping)) == snf::server::FramePostResult::Accepted);
        route = std::get_if<snf::server::PlayerCommandRoute>(&fixture.commands.posted->route);
        assert(route != nullptr);
        assert(route->actor == player);

        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
    }

    void test_purchase_requires_authentication_and_routes_to_the_attached_player()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 55, .generation = 25};
        const snf::server::PlayerId player{.value = 90};

        assert(fixture.gateway.tryPost(make_purchase_frame(connection, 3, 1)) == snf::server::FramePostResult::InvalidPayload);
        assert(fixture.commands.post_count == 0);
        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_purchase_frame(connection, 3, 1)) == snf::server::FramePostResult::Accepted);

        const auto* route = std::get_if<snf::server::PlayerCommandRoute>(&fixture.commands.posted->route);
        assert(route != nullptr);
        assert(route->actor == player);
        const auto* purchase = std::get_if<snf::server::PurchaseCommand>(&route->command);
        assert(purchase != nullptr);
        assert(route->request_id == 9);
        assert(purchase->idempotency_key.value == 3);
        assert(purchase->product == snf::server::BASIC_PRODUCT);
    }

    void test_party_membership_requires_authentication_and_uses_a_shared_route()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 56, .generation = 26};
        const snf::server::PlayerId player{.value = 91};

        assert(fixture.gateway.tryPost(make_party_join_frame(connection, 7)) == snf::server::FramePostResult::InvalidPayload);
        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_party_join_frame(connection, 7)) == snf::server::FramePostResult::Accepted);
        auto* route = std::get_if<snf::server::PartyCommandRoute>(&fixture.commands.posted->route);
        assert(route != nullptr);
        assert(route->party == snf::server::PartyId{.value = 7});
        const auto* join = std::get_if<snf::server::JoinPartyCommand>(&route->command);
        assert(join != nullptr);
        assert(join->player == player);
        assert(join->membership_epoch == 1);

        const int post_count = fixture.commands.post_count;
        assert(fixture.gateway.tryPost(make_party_join_frame(connection, 8)) == snf::server::FramePostResult::InvalidPayload);
        assert(fixture.commands.post_count == post_count);
        assert(
            fixture.gateway.tryPost(snf::server::FrameEnvelope{
                .connection = connection,
                .frame =
                    snf::protocol::Frame{
                        .type = snf::protocol::MessageType::PartyLeave,
                        .request_id = 11,
                        .payload = {},
                    },
            }) == snf::server::FramePostResult::Accepted
        );
        route = std::get_if<snf::server::PartyCommandRoute>(&fixture.commands.posted->route);
        assert(route != nullptr);
        assert(std::holds_alternative<snf::server::LeavePartyCommand>(route->command));
    }

    void test_connection_close_leaves_party_before_player_passivation()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 57, .generation = 27};
        const snf::server::PlayerId player{.value = 92};
        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_party_join_frame(connection, 9)) == snf::server::FramePostResult::Accepted);

        const std::size_t before = fixture.commands.route_indices.size();
        assert(
            fixture.gateway.tryPostConnectionClosed(snf::server::ConnectionClosed{
                .connection = connection,
                .cause = snf::server::ConnectionCloseCause::PeerClosed,
                .has_location_snapshot = false,
                .last_location = std::nullopt,
            }) == snf::server::PostResult::Accepted
        );
        assert(fixture.commands.route_indices.size() == before + 2);
        assert(fixture.commands.route_indices[before] == 3);
        assert(fixture.commands.route_indices[before + 1] == 1);
    }

    void test_rejects_duplicate_player_and_auth_after_provisional_activity()
    {
        GatewayFixture fixture;
        const snf::server::PlayerId player{.value = 88};
        const snf::net::ConnectionId first{.descriptor = 51, .generation = 21};
        const snf::net::ConnectionId second{.descriptor = 52, .generation = 22};

        assert(fixture.gateway.tryPost(make_auth_frame(first, player)) == snf::server::FramePostResult::Accepted);
        const int accepted_post_count = fixture.commands.post_count;
        assert(fixture.gateway.tryPost(make_auth_frame(second, player)) == snf::server::FramePostResult::InvalidPayload);
        assert(fixture.commands.post_count == accepted_post_count);

        auto pre_auth_ping = make_frame(snf::protocol::MessageType::Ping);
        pre_auth_ping.connection = second;
        assert(fixture.gateway.tryPost(std::move(pre_auth_ping)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_auth_frame(second, snf::server::PlayerId{.value = 89})) == snf::server::FramePostResult::InvalidPayload);
    }

    void test_rolls_back_refused_auth_and_keeps_close_retry_target()
    {
        GatewayFixture fixture;
        const snf::server::PlayerId player{.value = 99};
        const snf::net::ConnectionId first{.descriptor = 53, .generation = 23};
        const snf::net::ConnectionId second{.descriptor = 54, .generation = 24};

        fixture.commands.result = snf::server::PostResult::Full;
        assert(fixture.gateway.tryPost(make_auth_frame(first, player)) == snf::server::FramePostResult::Full);

        fixture.commands.result = snf::server::PostResult::Accepted;
        assert(fixture.gateway.tryPost(make_auth_frame(second, player)) == snf::server::FramePostResult::Accepted);

        fixture.commands.result = snf::server::PostResult::Full;
        const snf::server::ConnectionClosed closed{
            .connection = second,
            .cause = snf::server::ConnectionCloseCause::PeerClosed,
            .has_location_snapshot = false,
            .last_location = std::nullopt,
        };
        assert(fixture.gateway.tryPostConnectionClosed(closed) == snf::server::PostResult::Full);
        auto* close_route = std::get_if<snf::server::ConnectionClosedRoute>(&fixture.commands.posted->route);
        assert(close_route != nullptr);
        assert(close_route->actor == player);

        fixture.commands.result = snf::server::PostResult::Accepted;
        assert(fixture.gateway.tryPostConnectionClosed(closed) == snf::server::PostResult::Accepted);
        close_route = std::get_if<snf::server::ConnectionClosedRoute>(&fixture.commands.posted->route);
        assert(close_route != nullptr);
        assert(close_route->actor == player);
    }

    void test_routes_authenticated_zone_enter_move_leave_with_one_epoch()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 60, .generation = 30};
        const snf::server::PlayerId player{.value = 101};

        assert(fixture.gateway.tryPost(make_enter_frame(connection, 5, 1, 2)) == snf::server::FramePostResult::InvalidPayload);
        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        fixture.sessions.noteLocation(
            connection,
            snf::server::PlayerLocation{
                .zone = snf::server::ZoneId{.value = 5},
                .position = {.x = 8, .y = 9},
            }
        );

        fixture.commands.result = snf::server::PostResult::Full;
        assert(fixture.gateway.tryPost(make_enter_frame(connection, 5, 1, 2)) == snf::server::FramePostResult::Full);
        fixture.commands.result = snf::server::PostResult::Accepted;
        assert(fixture.gateway.tryPost(make_enter_frame(connection, 5, 1, 2)) == snf::server::FramePostResult::Accepted);
        auto* zone = std::get_if<snf::server::ZoneCommandRoute>(&fixture.commands.posted->route);
        assert(zone != nullptr);
        const auto* enter = std::get_if<snf::server::EnterZoneCommand>(&zone->command);
        assert(enter != nullptr);
        assert(zone->zone == snf::server::ZoneId{.value = 5});
        assert(enter->player == player);
        assert(enter->route_epoch == 2);
        const std::uint64_t route_epoch = enter->route_epoch;
        assert((enter->position == snf::server::ZonePosition{.x = 8, .y = 9}));
        assert(zone->reply_kind == snf::server::ZoneReplyKind::Entered);

        assert(fixture.gateway.tryPost(make_move_frame(connection, -3, 4)) == snf::server::FramePostResult::Accepted);
        zone = std::get_if<snf::server::ZoneCommandRoute>(&fixture.commands.posted->route);
        assert(zone != nullptr);
        const auto* move = std::get_if<snf::server::MoveInZoneCommand>(&zone->command);
        assert(move != nullptr);
        assert(move->route_epoch == route_epoch);
        assert((move->position == snf::server::ZonePosition{.x = -3, .y = 4}));

        assert(
            fixture.gateway.tryPost(snf::server::FrameEnvelope{
                .connection = connection,
                .frame =
                    snf::protocol::Frame{
                        .type = snf::protocol::MessageType::LeaveZone,
                        .request_id = 32,
                        .payload = {},
                    },
            }) == snf::server::FramePostResult::Accepted
        );
        zone = std::get_if<snf::server::ZoneCommandRoute>(&fixture.commands.posted->route);
        assert(zone != nullptr);
        const auto* leave = std::get_if<snf::server::LeaveZoneCommand>(&zone->command);
        assert(leave != nullptr);
        assert(leave->route_epoch == route_epoch);
        assert(!fixture.sessions.locationFor(connection).has_value());
        const int post_count_after_leave = fixture.commands.post_count;
        assert(fixture.gateway.tryPost(make_move_frame(connection, 0, 0)) == snf::server::FramePostResult::InvalidPayload);
        assert(fixture.commands.post_count == post_count_after_leave);
    }

    void test_connection_close_leaves_zone_before_player_passivation()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 61, .generation = 31};
        const snf::server::PlayerId player{.value = 102};
        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_enter_frame(connection, 6, 0, 0)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_move_frame(connection, -7, 12)) == snf::server::FramePostResult::Accepted);

        const std::size_t before = fixture.commands.route_indices.size();
        assert(
            fixture.gateway.tryPostConnectionClosed(snf::server::ConnectionClosed{
                .connection = connection,
                .cause = snf::server::ConnectionCloseCause::PeerClosed,
                .has_location_snapshot = false,
                .last_location = std::nullopt,
            }) == snf::server::PostResult::Accepted
        );
        assert(fixture.commands.route_indices.size() == before + 2);
        assert(fixture.commands.route_indices[before] == 2);
        assert(fixture.commands.route_indices[before + 1] == 1);
        const auto* close = std::get_if<snf::server::ConnectionClosedRoute>(&fixture.commands.posted->route);
        assert(close != nullptr);
        assert(
            (close->last_location ==
             snf::server::PlayerLocation{
                 .zone = snf::server::ZoneId{.value = 6},
                 .position = {.x = -7, .y = 12},
             })
        );
    }

    void test_cross_zone_handoff_hides_route_until_target_completion()
    {
        GatewayFixture fixture{GatewayFixtureConfig{
            .max_handoffs = 2,
            .transition_capacity = 2,
            .max_party_members = 4,
            .max_zone_completions_per_turn = 1,
        }};
        const snf::net::ConnectionId connection{.descriptor = 62, .generation = 32};
        const snf::server::PlayerId player{.value = 103};
        const snf::server::ZoneId source_zone{.value = 10};
        const snf::server::ZoneId target_zone{.value = 11};

        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_enter_frame(connection, source_zone.value, 1, 2)) == snf::server::FramePostResult::Accepted);
        assert(fixture.routes.routeFor(connection)->zone == source_zone);

        assert(fixture.gateway.tryPost(make_enter_frame(connection, target_zone.value, 3, 4)) == snf::server::FramePostResult::Accepted);
        const auto* source_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(source_stage != nullptr && source_stage->command.handoff);
        const snf::server::ZoneHandoffContext source_context = *source_stage->command.handoff;
        assert(source_context.step == snf::server::ZoneHandoffStep::LeaveSource);
        assert(!fixture.routes.routeFor(connection));

        assert(fixture.transitions.publish(
            source_context.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = source_context.handoff_id,
                .connection = snf::net::ConnectionId{.descriptor = 999, .generation = 999},
                .player = player,
                .zone = source_zone,
                .route_epoch = source_context.route_epoch,
                .step = source_context.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 1, .y = 2},
            }
        ));
        fixture.gateway.drainTransitions();
        assert(fixture.gateway.zoneHandoffStats().stale_completions == 1);
        assert(!fixture.routes.routeFor(connection));

        const int posts_before_busy = fixture.commands.post_count;
        assert(fixture.gateway.tryPost(make_move_frame(connection, 5, 6)) == snf::server::FramePostResult::Accepted);
        assert(fixture.commands.post_count == posts_before_busy);
        const auto busy = fixture.outbound.tryPop();
        assert(busy);
        const auto* busy_send = std::get_if<snf::server::SendFrame>(&busy->action);
        assert(busy_send != nullptr);
        assert(busy_send->frame.type == snf::protocol::MessageType::Moved);
        assert(busy_send->frame.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::TransitionInProgress));
        assert(fixture.lifecycle.terminalCount() == 1);

        assert(fixture.transitions.publish(
            source_context.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = source_context.handoff_id,
                .connection = connection,
                .player = player,
                .zone = source_zone,
                .route_epoch = source_context.route_epoch,
                .step = source_context.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 1, .y = 2},
            }
        ));
        fixture.gateway.drainTransitions();
        const auto* target_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(target_stage != nullptr && target_stage->command.handoff);
        const snf::server::ZoneHandoffContext target_context = *target_stage->command.handoff;
        assert(target_context.step == snf::server::ZoneHandoffStep::EnterTarget);
        assert(!fixture.routes.routeFor(connection));

        assert(fixture.transitions.publish(
            target_context.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = target_context.handoff_id,
                .connection = connection,
                .player = player,
                .zone = target_zone,
                .route_epoch = target_context.route_epoch,
                .step = target_context.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 3, .y = 4},
            }
        ));
        fixture.gateway.drainTransitions();

        const auto route = fixture.routes.routeFor(connection);
        assert(route && route->zone == target_zone && route->route_epoch == 2);
        assert(
            (fixture.sessions.locationFor(connection) ==
             snf::server::PlayerLocation{
                 .zone = target_zone,
                 .position = {.x = 3, .y = 4},
             })
        );
        const auto entered = fixture.outbound.tryPop();
        assert(entered);
        const auto* entered_send = std::get_if<snf::server::SendFrame>(&entered->action);
        assert(entered_send != nullptr);
        assert(entered_send->frame.type == snf::protocol::MessageType::ZoneEntered);
        assert(fixture.lifecycle.terminalCount() == 2);
        assert(fixture.transitions.stats().reservations == 0);

        fixture.commands.result = snf::server::PostResult::Full;
        assert(fixture.gateway.tryPost(make_enter_frame(connection, source_zone.value, 7, 8)) == snf::server::FramePostResult::Accepted);
        const auto failed = fixture.outbound.tryPop();
        assert(failed);
        const auto* failed_send = std::get_if<snf::server::SendFrame>(&failed->action);
        assert(failed_send != nullptr);
        assert(failed_send->frame.type == snf::protocol::MessageType::ZoneEntered);
        assert(failed_send->frame.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::TransferFailed));
        const auto stable_after_failure = fixture.routes.routeFor(connection);
        assert(stable_after_failure && stable_after_failure->zone == target_zone && stable_after_failure->route_epoch == 2);
        assert(fixture.lifecycle.terminalCount() == 3);
        assert(fixture.transitions.stats().reservations == 0);
        assert(fixture.gateway.zoneHandoffStats().failures_before_source_leave == 1);

        fixture.commands.result = snf::server::PostResult::Accepted;
        assert(fixture.gateway.tryPost(make_enter_frame(connection, source_zone.value, 9, 10)) == snf::server::FramePostResult::Accepted);
        source_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(source_stage != nullptr && source_stage->command.handoff);
        const snf::server::ZoneHandoffContext compensation_leave = *source_stage->command.handoff;
        fixture.commands.results.push_back(snf::server::PostResult::Full);
        fixture.commands.results.push_back(snf::server::PostResult::Accepted);
        assert(fixture.transitions.publish(
            compensation_leave.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = compensation_leave.handoff_id,
                .connection = connection,
                .player = player,
                .zone = target_zone,
                .route_epoch = compensation_leave.route_epoch,
                .step = compensation_leave.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 3, .y = 4},
            }
        ));
        fixture.gateway.drainTransitions();
        const auto* restore_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(restore_stage != nullptr && restore_stage->command.handoff);
        const snf::server::ZoneHandoffContext restore_context = *restore_stage->command.handoff;
        assert(restore_context.step == snf::server::ZoneHandoffStep::RestoreSource);
        assert(restore_context.route_epoch == 5);
        assert(fixture.transitions.publish(
            restore_context.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = restore_context.handoff_id,
                .connection = connection,
                .player = player,
                .zone = target_zone,
                .route_epoch = restore_context.route_epoch,
                .step = restore_context.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 3, .y = 4},
            }
        ));
        fixture.gateway.drainTransitions();
        const auto compensated_route = fixture.routes.routeFor(connection);
        assert(compensated_route && compensated_route->zone == target_zone && compensated_route->route_epoch == 5);
        const auto compensation_reply = fixture.outbound.tryPop();
        assert(compensation_reply);
        const auto* compensation_send = std::get_if<snf::server::SendFrame>(&compensation_reply->action);
        assert(compensation_send != nullptr);
        assert(compensation_send->frame.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::TransferFailed));
        assert(fixture.lifecycle.terminalCount() == 4);
        assert(fixture.gateway.zoneHandoffStats().target_failures == 1);
        assert(fixture.gateway.zoneHandoffStats().compensated == 1);
        assert(fixture.routes.stats().handoffs_restored == 1);

        assert(fixture.gateway.tryPost(make_enter_frame(connection, source_zone.value, 11, 12)) == snf::server::FramePostResult::Accepted);
        source_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(source_stage != nullptr && source_stage->command.handoff);
        const snf::server::ZoneHandoffContext fatal_leave = *source_stage->command.handoff;
        fixture.commands.results.push_back(snf::server::PostResult::Full);
        fixture.commands.results.push_back(snf::server::PostResult::Full);
        assert(fixture.transitions.publish(
            fatal_leave.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = fatal_leave.handoff_id,
                .connection = connection,
                .player = player,
                .zone = target_zone,
                .route_epoch = fatal_leave.route_epoch,
                .step = fatal_leave.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 3, .y = 4},
            }
        ));
        fixture.gateway.drainTransitions();
        assert(!fixture.routes.routeFor(connection));
        const auto fatal_location = fixture.sessions.locationSnapshotFor(connection);
        assert(fatal_location.known && !fatal_location.location);
        std::vector<snf::net::ConnectionId> failed_connections;
        assert(!fixture.outbound.takePendingAdmissionFailures(failed_connections));
        assert(failed_connections == std::vector<snf::net::ConnectionId>{connection});
        assert(fixture.gateway.zoneHandoffStats().fatal == 1);
        assert(fixture.lifecycle.terminalCount() == 5);
        assert(fixture.transitions.stats().reservations == 0);
    }

    void test_cross_zone_disconnect_waits_for_source_or_target_cleanup()
    {
        GatewayFixture fixture{GatewayFixtureConfig{
            .max_handoffs = 2,
            .transition_capacity = 2,
            .max_party_members = 4,
            .max_zone_completions_per_turn = 1,
        }};
        const snf::server::ZoneId source_zone{.value = 20};
        const snf::server::ZoneId target_zone{.value = 21};

        const auto begin_transfer = [&](const snf::net::ConnectionId connection, const snf::server::PlayerId player)
        {
            assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
            assert(fixture.gateway.tryPost(make_enter_frame(connection, source_zone.value, 1, 2)) == snf::server::FramePostResult::Accepted);
            assert(fixture.gateway.tryPost(make_enter_frame(connection, target_zone.value, 3, 4)) == snf::server::FramePostResult::Accepted);
            const auto* stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
            assert(stage != nullptr && stage->command.handoff);
            return *stage->command.handoff;
        };
        const auto close_value = [](const snf::net::ConnectionId connection)
        {
            return snf::server::ConnectionClosed{
                .connection = connection,
                .cause = snf::server::ConnectionCloseCause::PeerClosed,
                .has_location_snapshot = false,
                .last_location = std::nullopt,
            };
        };

        const snf::net::ConnectionId first_connection{.descriptor = 70, .generation = 40};
        const snf::server::PlayerId first_player{.value = 110};
        const auto first_source = begin_transfer(first_connection, first_player);
        assert(fixture.gateway.tryPostConnectionClosed(close_value(first_connection)) == snf::server::PostResult::Full);
        assert(fixture.transitions.publish(
            first_source.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = first_source.handoff_id,
                .connection = first_connection,
                .player = first_player,
                .zone = source_zone,
                .route_epoch = first_source.route_epoch,
                .step = first_source.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 1, .y = 2},
            }
        ));
        fixture.gateway.drainTransitions();
        assert(fixture.gateway.transitionsDrained());
        assert(!fixture.routes.routeFor(first_connection));
        const auto first_snapshot = fixture.sessions.locationSnapshotFor(first_connection);
        assert(first_snapshot.known && !first_snapshot.location);
        assert(fixture.gateway.tryPostConnectionClosed(close_value(first_connection)) == snf::server::PostResult::Accepted);
        const auto* first_close = std::get_if<snf::server::ConnectionClosedRoute>(&fixture.commands.posted->route);
        assert(first_close != nullptr && first_close->has_location_snapshot && !first_close->last_location);

        const snf::net::ConnectionId second_connection{.descriptor = 71, .generation = 41};
        const snf::server::PlayerId second_player{.value = 111};
        const auto second_source = begin_transfer(second_connection, second_player);
        assert(fixture.transitions.publish(
            second_source.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = second_source.handoff_id,
                .connection = second_connection,
                .player = second_player,
                .zone = source_zone,
                .route_epoch = second_source.route_epoch,
                .step = second_source.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 1, .y = 2},
            }
        ));
        fixture.gateway.drainTransitions();
        const auto* target_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(target_stage != nullptr && target_stage->command.handoff);
        const snf::server::ZoneHandoffContext target_context = *target_stage->command.handoff;
        assert(fixture.gateway.tryPostConnectionClosed(close_value(second_connection)) == snf::server::PostResult::Full);
        assert(fixture.transitions.publish(
            target_context.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = target_context.handoff_id,
                .connection = second_connection,
                .player = second_player,
                .zone = target_zone,
                .route_epoch = target_context.route_epoch,
                .step = target_context.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 3, .y = 4},
            }
        ));
        fixture.gateway.drainTransitions();
        const auto* cleanup_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(cleanup_stage != nullptr && cleanup_stage->command.handoff);
        const snf::server::ZoneHandoffContext cleanup_context = *cleanup_stage->command.handoff;
        assert(cleanup_context.step == snf::server::ZoneHandoffStep::CleanupTarget);
        assert(fixture.transitions.publish(
            cleanup_context.ticket,
            snf::server::ZoneHandoffCompletion{
                .handoff_id = cleanup_context.handoff_id,
                .connection = second_connection,
                .player = second_player,
                .zone = target_zone,
                .route_epoch = cleanup_context.route_epoch,
                .step = cleanup_context.step,
                .status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 3, .y = 4},
            }
        ));
        fixture.gateway.drainTransitions();
        assert(fixture.gateway.transitionsDrained());
        assert(fixture.gateway.tryPostConnectionClosed(close_value(second_connection)) == snf::server::PostResult::Accepted);
        const auto* second_close = std::get_if<snf::server::ConnectionClosedRoute>(&fixture.commands.posted->route);
        assert(second_close != nullptr && second_close->has_location_snapshot && !second_close->last_location);
        assert(fixture.gateway.zoneHandoffStats().disconnect_cleanups == 2);
        assert(fixture.lifecycle.terminalCount() == 2);

        const snf::net::ConnectionId third_connection{.descriptor = 72, .generation = 42};
        const snf::server::PlayerId third_player{.value = 112};
        static_cast<void>(begin_transfer(third_connection, third_player));
        fixture.gateway.close();
        fixture.gateway.cancel();
        assert(fixture.gateway.transitionsDrained());
        assert(!fixture.routes.handoffFor(third_connection));
        const auto third_snapshot = fixture.sessions.locationSnapshotFor(third_connection);
        assert(third_snapshot.known && !third_snapshot.location);
        assert(fixture.gateway.zoneHandoffStats().shutdown_cancels == 1);
        assert(fixture.lifecycle.terminalCount() == 3);
    }
    snf::server::FrameEnvelope make_room_join_frame(const snf::net::ConnectionId connection, const std::uint64_t room)
    {
        std::vector<std::byte> payload;
        append_u64(payload, room);
        return snf::server::FrameEnvelope{
            .connection = connection,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::RoomJoin,
                    .request_id = 40,
                    .payload = std::move(payload),
                },
        };
    }

    snf::server::FrameEnvelope make_battle_start_frame(const snf::net::ConnectionId connection, const std::uint64_t room)
    {
        std::vector<std::byte> payload;
        append_u64(payload, room);
        return snf::server::FrameEnvelope{
            .connection = connection,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::BattleStart,
                    .request_id = 41,
                    .payload = std::move(payload),
                },
        };
    }

    snf::server::FrameEnvelope make_room_leave_frame(const snf::net::ConnectionId connection)
    {
        return snf::server::FrameEnvelope{
            .connection = connection,
            .frame =
                snf::protocol::Frame{
                    .type = snf::protocol::MessageType::RoomLeave,
                    .request_id = 42,
                    .payload = {},
                },
        };
    }

    void test_room_handoff_lifecycle()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 55, .generation = 1};
        const snf::server::PlayerId player{.value = 100};
        const snf::server::ZoneId zone{.value = 10};
        const snf::server::RoomId room{.value = 50};

        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_enter_frame(connection, zone.value, 15, 25)) == snf::server::FramePostResult::Accepted);
        assert(fixture.routes.routeFor(connection).has_value());

        // 1. Send RoomJoin
        assert(fixture.gateway.tryPost(make_room_join_frame(connection, room.value)) == snf::server::FramePostResult::Accepted);
        assert(!fixture.routes.routeFor(connection).has_value()); // Hidden during entry
        const auto entry = fixture.routes.roomEntryFor(connection);
        assert(entry && entry->room == room);

        const auto* join_stage = std::get_if<snf::server::PlayerCommandRoute>(&fixture.commands.posted->route);
        assert(join_stage != nullptr);
        const auto* join_req = std::get_if<snf::server::JoinRoomRequest>(&join_stage->command);
        // The saga's identity travels beside the game command, never inside it.
        assert(join_req != nullptr && join_req->room == room);
        assert(join_stage->room_entry.has_value());
        const snf::server::RoomEntryContext join_context = *join_stage->room_entry;

        // 2. RoomActor completes JoinRoom -> Applied
        assert(fixture.room_transitions.publish(
            join_context.ticket,
            snf::server::RoomTransitionCompletion{
                .entry_id = join_context.entry_id,
                .connection = connection,
                .player = player,
                .room = room,
                .step = snf::server::RoomEntryStep::JoinRoom,
                .room_status = snf::server::RoomCommandStatus::Applied,
            }
        ));
        fixture.gateway.drainTransitions();

        // 3. Next step posted: LeaveSource on source zone
        const auto* leave_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(leave_stage != nullptr && leave_stage->command.room_entry.has_value());
        const snf::server::RoomEntryContext leave_context = *leave_stage->command.room_entry;
        assert(leave_context.step == snf::server::RoomEntryStep::LeaveSource);

        // 4. ZoneActor completes LeaveZone -> Applied with position {15, 25}
        assert(fixture.room_transitions.publish(
            leave_context.ticket,
            snf::server::RoomTransitionCompletion{
                .entry_id = leave_context.entry_id,
                .connection = connection,
                .player = player,
                .zone = zone,
                .step = snf::server::RoomEntryStep::LeaveSource,
                .zone_status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 15, .y = 25},
            }
        ));
        fixture.gateway.drainTransitions();

        // Now InRoom! Zone route is erased, connection is authoritative in Room
        assert(!fixture.routes.routeFor(connection).has_value());
        assert(fixture.routes.inRoomFor(connection).has_value());
        assert(fixture.routes.inRoomFor(connection)->room == room);
        assert(fixture.routes.inRoomFor(connection)->return_zone == zone);

        // Outbound receives RoomJoined frame
        const auto join_reply = fixture.outbound.tryPop();
        assert(join_reply);
        const auto* join_send = std::get_if<snf::server::SendFrame>(&join_reply->action);
        assert(join_send != nullptr && join_send->frame.type == snf::protocol::MessageType::RoomJoined);

        // 5. While InRoom, a Zone command is answered rather than refused. InvalidPayload
        // would close the connection, and a Move already in flight when the entry landed
        // is an ordinary client race.
        assert(fixture.gateway.tryPost(make_move_frame(connection, 20, 30)) == snf::server::FramePostResult::Accepted);
        const auto move_reply = fixture.outbound.tryPop();
        assert(move_reply);
        const auto* move_send = std::get_if<snf::server::SendFrame>(&move_reply->action);
        assert(move_send != nullptr && move_send->frame.type == snf::protocol::MessageType::Moved);
        assert(move_send->frame.payload.at(0) == static_cast<std::byte>(static_cast<std::uint8_t>(snf::server::ZoneCommandStatus::InRoom)));
        // Nothing reached a ZoneActor, so the route is untouched.
        assert(!fixture.routes.routeFor(connection).has_value());
        assert(fixture.routes.inRoomFor(connection)->room == room);

        assert(fixture.gateway.tryPost(make_enter_frame(connection, 99, 0, 0)) == snf::server::FramePostResult::Accepted);
        const auto enter_reply = fixture.outbound.tryPop();
        assert(enter_reply);
        const auto* enter_send = std::get_if<snf::server::SendFrame>(&enter_reply->action);
        assert(enter_send != nullptr && enter_send->frame.type == snf::protocol::MessageType::ZoneEntered);
        assert(enter_send->frame.payload.at(0) == static_cast<std::byte>(static_cast<std::uint8_t>(snf::server::ZoneCommandStatus::InRoom)));

        // 6. StartBattle for wrong room rejected; for correct room accepted
        assert(fixture.gateway.tryPost(make_battle_start_frame(connection, 999)) == snf::server::FramePostResult::InvalidPayload);
        assert(fixture.gateway.tryPost(make_battle_start_frame(connection, room.value)) == snf::server::FramePostResult::Accepted);

        // 7. RoomLeave -> Leaves room and begins return to zone
        assert(fixture.gateway.tryPost(make_room_leave_frame(connection)) == snf::server::FramePostResult::Accepted);

        // Return posted: EnterZone to return_zone
        const auto* enter_return_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(enter_return_stage != nullptr && enter_return_stage->command.room_entry.has_value());
        const snf::server::RoomEntryContext return_context = *enter_return_stage->command.room_entry;

        // ZoneActor completes EnterZone
        assert(fixture.room_transitions.publish(
            return_context.ticket,
            snf::server::RoomTransitionCompletion{
                .entry_id = return_context.entry_id,
                .return_id = return_context.return_id,
                .connection = connection,
                .player = player,
                .zone = zone,
                .step = return_context.step,
                .zone_status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 15, .y = 25},
            }
        ));
        fixture.gateway.drainTransitions();

        // Restored to Zone!
        const auto restored_route = fixture.routes.routeFor(connection);
        assert(restored_route && restored_route->zone == zone && restored_route->route_epoch == 2);
        assert(!fixture.routes.inRoomFor(connection).has_value());

        // Outbound receives unsolicited ReturnedToZone frame
        const auto return_reply = fixture.outbound.tryPop();
        assert(return_reply);
        const auto* return_send = std::get_if<snf::server::SendFrame>(&return_reply->action);
        assert(return_send != nullptr && return_send->frame.type == snf::protocol::MessageType::ReturnedToZone);
    }

    // The only path that ends an entry before it touched the source Zone, and the path
    // a refused Room mailbox now reports through. Nothing covered it before, so the
    // rollback it performs was never executed.
    void test_a_refused_room_join_ends_the_entry_and_restores_the_zone_route()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 61, .generation = 1};
        const snf::server::PlayerId player{.value = 300};
        const snf::server::ZoneId zone{.value = 10};
        const snf::server::RoomId room{.value = 50};

        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_enter_frame(connection, zone.value, 15, 25)) == snf::server::FramePostResult::Accepted);
        const auto source_route = fixture.routes.routeFor(connection);
        assert(source_route.has_value());

        assert(fixture.gateway.tryPost(make_room_join_frame(connection, room.value)) == snf::server::FramePostResult::Accepted);
        const auto* join_stage = std::get_if<snf::server::PlayerCommandRoute>(&fixture.commands.posted->route);
        assert(join_stage != nullptr && join_stage->room_entry.has_value());
        const snf::server::RoomEntryContext join_context = *join_stage->room_entry;
        assert(fixture.room_transitions.stats().reservations == 1);
        const std::uint64_t releases_before = fixture.lifecycle.releaseCount();

        // What the Room answers when it refuses, and what the Player's binding publishes
        // when the join never reached a Room at all: the same completion, so the same
        // ending. EntryFailed is the second case.
        assert(fixture.room_transitions.publish(
            join_context.ticket,
            snf::server::RoomTransitionCompletion{
                .entry_id = join_context.entry_id,
                .connection = connection,
                .player = player,
                .room = room,
                .step = snf::server::RoomEntryStep::JoinRoom,
                .room_status = snf::server::RoomCommandStatus::EntryFailed,
            }
        ));
        fixture.gateway.drainTransitions();

        const auto reply = fixture.outbound.tryPop();
        assert(reply);
        const auto* send = std::get_if<snf::server::SendFrame>(&reply->action);
        assert(send != nullptr && send->frame.type == snf::protocol::MessageType::RoomJoined);
        // The frame that asked, answered once.
        assert(send->frame.request_id == 40);
        assert(send->frame.payload.at(0) == static_cast<std::byte>(static_cast<std::uint8_t>(snf::server::RoomCommandStatus::EntryFailed)));

        // The source Zone was never touched, so the route comes back exactly as it was
        // rather than as a new epoch.
        const auto restored = fixture.routes.routeFor(connection);
        assert(restored.has_value() && *restored == *source_route);
        assert(!fixture.routes.roomEntryFor(connection).has_value());
        assert(!fixture.routes.inRoomFor(connection).has_value());

        // Nothing is left holding the saga: no ticket, no pending entry, and the client
        // command reached exactly one terminal.
        assert(fixture.room_transitions.stats().reservations == 0);
        assert(fixture.room_entries.stats().pending == 0);
        assert(fixture.lifecycle.releaseCount() == releases_before + 1);

        // And the connection is usable again, in the Zone it never left.
        assert(fixture.gateway.tryPost(make_move_frame(connection, 20, 30)) == snf::server::FramePostResult::Accepted);
        const auto* move_route = std::get_if<snf::server::ZoneCommandRoute>(&fixture.commands.posted->route);
        assert(move_route != nullptr && move_route->zone == zone);
    }

    // Both of these used to close the connection, because tryStart reads the zone route
    // and RouteCoordinator hides it in either state.
    void test_a_room_join_without_a_zone_is_answered_rather_than_closed()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 62, .generation = 1};
        const snf::server::PlayerId player{.value = 400};
        const snf::server::RoomId room{.value = 50};

        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        const int posts_before = fixture.commands.post_count;

        // Authenticated but in no Zone: there is nothing to leave and nothing to return
        // to when the battle ends. A client reaches this by leaving a Zone first, so it
        // is a refused join and not a protocol error.
        assert(fixture.gateway.tryPost(make_room_join_frame(connection, room.value)) == snf::server::FramePostResult::Accepted);

        const auto reply = fixture.outbound.tryPop();
        assert(reply);
        const auto* send = std::get_if<snf::server::SendFrame>(&reply->action);
        assert(send != nullptr && send->frame.type == snf::protocol::MessageType::RoomJoined);
        assert(send->frame.payload.at(0) == static_cast<std::byte>(static_cast<std::uint8_t>(snf::server::RoomCommandStatus::EntryFailed)));

        // Nothing was started, so nothing has to be cleaned up.
        assert(fixture.commands.post_count == posts_before);
        assert(!fixture.routes.roomEntryFor(connection).has_value());
        assert(fixture.room_transitions.stats().reservations == 0);
        assert(fixture.room_entries.stats().pending == 0);
    }

    void test_a_second_room_join_while_in_a_room_is_answered_rather_than_closed()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 63, .generation = 1};
        const snf::server::PlayerId player{.value = 401};
        const snf::server::ZoneId zone{.value = 10};
        const snf::server::RoomId room{.value = 50};

        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_enter_frame(connection, zone.value, 15, 25)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_room_join_frame(connection, room.value)) == snf::server::FramePostResult::Accepted);
        const auto* join_stage = std::get_if<snf::server::PlayerCommandRoute>(&fixture.commands.posted->route);
        assert(join_stage != nullptr && join_stage->room_entry.has_value());
        assert(fixture.room_transitions.publish(
            join_stage->room_entry->ticket,
            snf::server::RoomTransitionCompletion{
                .entry_id = join_stage->room_entry->entry_id,
                .connection = connection,
                .player = player,
                .room = room,
                .step = snf::server::RoomEntryStep::JoinRoom,
                .room_status = snf::server::RoomCommandStatus::Applied,
            }
        ));
        fixture.gateway.drainTransitions();
        const auto* leave_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(leave_stage != nullptr && leave_stage->command.room_entry.has_value());
        assert(fixture.room_transitions.publish(
            leave_stage->command.room_entry->ticket,
            snf::server::RoomTransitionCompletion{
                .entry_id = leave_stage->command.room_entry->entry_id,
                .connection = connection,
                .player = player,
                .zone = zone,
                .step = snf::server::RoomEntryStep::LeaveSource,
                .zone_status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 15, .y = 25},
            }
        ));
        fixture.gateway.drainTransitions();
        assert(fixture.routes.inRoomFor(connection).has_value());
        static_cast<void>(fixture.outbound.tryPop()); // the RoomJoined that was granted
        const int posts_before = fixture.commands.post_count;

        // A retry, or a second tap. The client already holds the grant, so the answer
        // says so instead of dropping it mid-battle.
        assert(fixture.gateway.tryPost(make_room_join_frame(connection, room.value)) == snf::server::FramePostResult::Accepted);

        const auto reply = fixture.outbound.tryPop();
        assert(reply);
        const auto* send = std::get_if<snf::server::SendFrame>(&reply->action);
        assert(send != nullptr && send->frame.type == snf::protocol::MessageType::RoomJoined);
        assert(send->frame.payload.at(0) == static_cast<std::byte>(static_cast<std::uint8_t>(snf::server::RoomCommandStatus::AlreadyJoined)));

        assert(fixture.commands.post_count == posts_before);
        assert(fixture.routes.inRoomFor(connection)->room == room);
        assert(fixture.room_entries.stats().pending == 0);
    }

    void test_room_disconnect_cleans_up_room_and_sessions()
    {
        GatewayFixture fixture;
        const snf::net::ConnectionId connection{.descriptor = 60, .generation = 1};
        const snf::server::PlayerId player{.value = 200};
        const snf::server::ZoneId zone{.value = 10};
        const snf::server::RoomId room{.value = 50};

        assert(fixture.gateway.tryPost(make_auth_frame(connection, player)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_enter_frame(connection, zone.value, 15, 25)) == snf::server::FramePostResult::Accepted);
        assert(fixture.gateway.tryPost(make_room_join_frame(connection, room.value)) == snf::server::FramePostResult::Accepted);

        const auto* join_stage = std::get_if<snf::server::PlayerCommandRoute>(&fixture.commands.posted->route);
        const snf::server::RoomEntryContext join_context = *join_stage->room_entry;

        assert(fixture.room_transitions.publish(
            join_context.ticket,
            snf::server::RoomTransitionCompletion{
                .entry_id = join_context.entry_id,
                .connection = connection,
                .player = player,
                .room = room,
                .step = snf::server::RoomEntryStep::JoinRoom,
                .room_status = snf::server::RoomCommandStatus::Applied,
            }
        ));
        fixture.gateway.drainTransitions();

        const auto* leave_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&fixture.commands.posted->route);
        assert(leave_stage != nullptr && leave_stage->command.room_entry.has_value());
        const snf::server::RoomEntryContext leave_context = *leave_stage->command.room_entry;

        assert(fixture.room_transitions.publish(
            leave_context.ticket,
            snf::server::RoomTransitionCompletion{
                .entry_id = leave_context.entry_id,
                .connection = connection,
                .player = player,
                .zone = zone,
                .step = snf::server::RoomEntryStep::LeaveSource,
                .zone_status = snf::server::ZoneCommandStatus::Applied,
                .position = snf::server::ZonePosition{.x = 15, .y = 25},
            }
        ));
        fixture.gateway.drainTransitions();
        assert(fixture.routes.inRoomFor(connection).has_value());

        // Disconnect while in room
        const auto close_res = fixture.gateway.tryPostConnectionClosed(snf::server::ConnectionClosed{
            .connection = connection,
            .cause = snf::server::ConnectionCloseCause::PeerClosed,
            .has_location_snapshot = false,
            .last_location = std::nullopt,
        });
        assert(close_res == snf::server::PostResult::Accepted);
        assert(!fixture.routes.inRoomFor(connection).has_value());
        assert(!fixture.routes.routeFor(connection).has_value());
    }
}

void run_protocol_gateway_tests()
{
    test_dispatches_and_routes_a_supported_frame();
    test_rejects_unsupported_and_invalid_frames_before_routing();
    test_preserves_downstream_capacity_and_lifecycle_results();
    test_routes_connection_closed_with_the_same_provisional_actor_id();
    test_authentication_attaches_and_routes_to_a_persistent_player();
    test_purchase_requires_authentication_and_routes_to_the_attached_player();
    test_party_membership_requires_authentication_and_uses_a_shared_route();
    test_connection_close_leaves_party_before_player_passivation();
    test_rejects_duplicate_player_and_auth_after_provisional_activity();
    test_rolls_back_refused_auth_and_keeps_close_retry_target();
    test_routes_authenticated_zone_enter_move_leave_with_one_epoch();
    test_connection_close_leaves_zone_before_player_passivation();
    test_cross_zone_handoff_hides_route_until_target_completion();
    test_cross_zone_disconnect_waits_for_source_or_target_cleanup();
    test_room_handoff_lifecycle();
    test_a_refused_room_join_ends_the_entry_and_restores_the_zone_route();
    test_a_room_join_without_a_zone_is_answered_rather_than_closed();
    test_a_second_room_join_while_in_a_room_is_answered_rather_than_closed();
    test_room_disconnect_cleans_up_room_and_sessions();
}
