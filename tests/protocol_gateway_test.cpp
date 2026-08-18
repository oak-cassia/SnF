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

    snf::server::FrameEnvelope make_enter_frame(const snf::net::ConnectionId connection,
                                                const std::uint64_t zone,
                                                const std::int32_t x,
                                                const std::int32_t y)
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

    snf::server::FrameEnvelope make_move_frame(const snf::net::ConnectionId connection,
                                               const std::int32_t x,
                                               const std::int32_t y)
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

    snf::server::FrameEnvelope make_auth_frame(const snf::net::ConnectionId connection,
                                               const snf::server::PlayerId player)
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

    snf::server::FrameEnvelope make_purchase_frame(const snf::net::ConnectionId connection,
                                                   const std::uint64_t key,
                                                   const std::uint32_t product)
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

    snf::server::FrameEnvelope make_party_join_frame(const snf::net::ConnectionId connection,
                                                     const std::uint64_t party)
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
            .has_location_snapshot = false,
            .last_location = std::nullopt,
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

    void test_authentication_attaches_and_routes_to_a_persistent_player()
    {
        RecordingRoutedIngress commands;
        snf::server::ProtocolGateway gateway{commands};
        const snf::net::ConnectionId connection{.descriptor = 50, .generation = 20};
        const snf::server::PlayerId player{.value = 77};

        assert(gateway.tryPost(make_auth_frame(connection, player)) ==
               snf::server::FramePostResult::Accepted);
        auto* route = std::get_if<snf::server::PlayerCommandRoute>(&commands.posted->route);
        assert(route != nullptr);
        assert(route->actor == player);
        assert(std::holds_alternative<snf::server::AuthenticateCommand>(route->command));

        auto ping = make_frame(snf::protocol::MessageType::Ping);
        ping.connection = connection;
        assert(gateway.tryPost(std::move(ping)) == snf::server::FramePostResult::Accepted);
        route = std::get_if<snf::server::PlayerCommandRoute>(&commands.posted->route);
        assert(route != nullptr);
        assert(route->actor == player);

        assert(gateway.tryPost(make_auth_frame(connection, player)) ==
               snf::server::FramePostResult::Accepted);
    }

    void test_purchase_requires_authentication_and_routes_to_the_attached_player()
    {
        RecordingRoutedIngress commands;
        snf::server::ProtocolGateway gateway{commands};
        const snf::net::ConnectionId connection{.descriptor = 55, .generation = 25};
        const snf::server::PlayerId player{.value = 90};

        assert(gateway.tryPost(make_purchase_frame(connection, 3, 1)) ==
               snf::server::FramePostResult::InvalidPayload);
        assert(commands.post_count == 0);
        assert(gateway.tryPost(make_auth_frame(connection, player)) ==
               snf::server::FramePostResult::Accepted);
        assert(gateway.tryPost(make_purchase_frame(connection, 3, 1)) ==
               snf::server::FramePostResult::Accepted);

        const auto* route = std::get_if<snf::server::PlayerCommandRoute>(&commands.posted->route);
        assert(route != nullptr);
        assert(route->actor == player);
        const auto* purchase = std::get_if<snf::server::PurchaseCommand>(&route->command);
        assert(purchase != nullptr);
        assert(purchase->request_id == 9);
        assert(purchase->idempotency_key.value == 3);
        assert(purchase->product == snf::server::BASIC_PRODUCT);
    }

    void test_party_membership_requires_authentication_and_uses_a_shared_route()
    {
        RecordingRoutedIngress commands;
        snf::server::ProtocolGateway gateway{commands};
        const snf::net::ConnectionId connection{.descriptor = 56, .generation = 26};
        const snf::server::PlayerId player{.value = 91};

        assert(gateway.tryPost(make_party_join_frame(connection, 7)) ==
               snf::server::FramePostResult::InvalidPayload);
        assert(gateway.tryPost(make_auth_frame(connection, player)) ==
               snf::server::FramePostResult::Accepted);
        assert(gateway.tryPost(make_party_join_frame(connection, 7)) ==
               snf::server::FramePostResult::Accepted);
        auto* route = std::get_if<snf::server::PartyCommandRoute>(&commands.posted->route);
        assert(route != nullptr);
        assert(route->party == snf::server::PartyId{.value = 7});
        const auto* join = std::get_if<snf::server::JoinPartyCommand>(&route->command);
        assert(join != nullptr);
        assert(join->player == player);
        assert(join->membership_epoch == 1);

        const int post_count = commands.post_count;
        assert(gateway.tryPost(make_party_join_frame(connection, 8)) ==
               snf::server::FramePostResult::InvalidPayload);
        assert(commands.post_count == post_count);
        assert(gateway.tryPost(snf::server::FrameEnvelope{
                   .connection = connection,
                   .frame =
                       snf::protocol::Frame{
                           .type = snf::protocol::MessageType::PartyLeave,
                           .request_id = 11,
                           .payload = {},
                       },
               }) == snf::server::FramePostResult::Accepted);
        route = std::get_if<snf::server::PartyCommandRoute>(&commands.posted->route);
        assert(route != nullptr);
        assert(std::holds_alternative<snf::server::LeavePartyCommand>(route->command));
    }

    void test_connection_close_leaves_party_before_player_passivation()
    {
        RecordingRoutedIngress commands;
        snf::server::ProtocolGateway gateway{commands};
        const snf::net::ConnectionId connection{.descriptor = 57, .generation = 27};
        const snf::server::PlayerId player{.value = 92};
        assert(gateway.tryPost(make_auth_frame(connection, player)) ==
               snf::server::FramePostResult::Accepted);
        assert(gateway.tryPost(make_party_join_frame(connection, 9)) ==
               snf::server::FramePostResult::Accepted);

        const std::size_t before = commands.route_indices.size();
        assert(gateway.tryPostConnectionClosed(snf::server::ConnectionClosed{
                   .connection = connection,
                   .cause = snf::server::ConnectionCloseCause::PeerClosed,
                   .has_location_snapshot = false,
                   .last_location = std::nullopt,
               }) == snf::server::PostResult::Accepted);
        assert(commands.route_indices.size() == before + 2);
        assert(commands.route_indices[before] == 3);
        assert(commands.route_indices[before + 1] == 1);
    }

    void test_rejects_duplicate_player_and_auth_after_provisional_activity()
    {
        RecordingRoutedIngress commands;
        snf::server::ProtocolGateway gateway{commands};
        const snf::server::PlayerId player{.value = 88};
        const snf::net::ConnectionId first{.descriptor = 51, .generation = 21};
        const snf::net::ConnectionId second{.descriptor = 52, .generation = 22};

        assert(gateway.tryPost(make_auth_frame(first, player)) ==
               snf::server::FramePostResult::Accepted);
        const int accepted_post_count = commands.post_count;
        assert(gateway.tryPost(make_auth_frame(second, player)) ==
               snf::server::FramePostResult::InvalidPayload);
        assert(commands.post_count == accepted_post_count);

        auto pre_auth_ping = make_frame(snf::protocol::MessageType::Ping);
        pre_auth_ping.connection = second;
        assert(gateway.tryPost(std::move(pre_auth_ping)) == snf::server::FramePostResult::Accepted);
        assert(gateway.tryPost(make_auth_frame(second, snf::server::PlayerId{.value = 89})) ==
               snf::server::FramePostResult::InvalidPayload);
    }

    void test_rolls_back_refused_auth_and_keeps_close_retry_target()
    {
        RecordingRoutedIngress commands;
        snf::server::ProtocolGateway gateway{commands};
        const snf::server::PlayerId player{.value = 99};
        const snf::net::ConnectionId first{.descriptor = 53, .generation = 23};
        const snf::net::ConnectionId second{.descriptor = 54, .generation = 24};

        commands.result = snf::server::PostResult::Full;
        assert(gateway.tryPost(make_auth_frame(first, player)) ==
               snf::server::FramePostResult::Full);

        commands.result = snf::server::PostResult::Accepted;
        assert(gateway.tryPost(make_auth_frame(second, player)) ==
               snf::server::FramePostResult::Accepted);

        commands.result = snf::server::PostResult::Full;
        const snf::server::ConnectionClosed closed{
            .connection = second,
            .cause = snf::server::ConnectionCloseCause::PeerClosed,
            .has_location_snapshot = false,
            .last_location = std::nullopt,
        };
        assert(gateway.tryPostConnectionClosed(closed) == snf::server::PostResult::Full);
        auto* close_route =
            std::get_if<snf::server::ConnectionClosedRoute>(&commands.posted->route);
        assert(close_route != nullptr);
        assert(close_route->actor == player);

        commands.result = snf::server::PostResult::Accepted;
        assert(gateway.tryPostConnectionClosed(closed) == snf::server::PostResult::Accepted);
        close_route = std::get_if<snf::server::ConnectionClosedRoute>(&commands.posted->route);
        assert(close_route != nullptr);
        assert(close_route->actor == player);
    }

    void test_routes_authenticated_zone_enter_move_leave_with_one_epoch()
    {
        RecordingRoutedIngress commands;
        snf::server::PlayerSessionDirectory sessions;
        snf::server::RouteCoordinator routes;
        snf::server::ProtocolGateway gateway{commands, sessions, routes};
        const snf::net::ConnectionId connection{.descriptor = 60, .generation = 30};
        const snf::server::PlayerId player{.value = 101};

        assert(gateway.tryPost(make_enter_frame(connection, 5, 1, 2)) ==
               snf::server::FramePostResult::InvalidPayload);
        assert(gateway.tryPost(make_auth_frame(connection, player)) ==
               snf::server::FramePostResult::Accepted);
        sessions.noteLocation(connection,
                              snf::server::PlayerLocation{
                                  .zone = snf::server::ZoneId{.value = 5},
                                  .position = {.x = 8, .y = 9},
                              });

        commands.result = snf::server::PostResult::Full;
        assert(gateway.tryPost(make_enter_frame(connection, 5, 1, 2)) ==
               snf::server::FramePostResult::Full);
        commands.result = snf::server::PostResult::Accepted;
        assert(gateway.tryPost(make_enter_frame(connection, 5, 1, 2)) ==
               snf::server::FramePostResult::Accepted);
        auto* zone = std::get_if<snf::server::ZoneCommandRoute>(&commands.posted->route);
        assert(zone != nullptr);
        const auto* enter = std::get_if<snf::server::EnterZoneCommand>(&zone->command);
        assert(enter != nullptr);
        assert(zone->zone == snf::server::ZoneId{.value = 5});
        assert(enter->player == player);
        assert(enter->route_epoch == 2);
        const std::uint64_t route_epoch = enter->route_epoch;
        assert((enter->position == snf::server::ZonePosition{.x = 8, .y = 9}));
        assert(zone->reply_kind == snf::server::ZoneReplyKind::Entered);

        assert(gateway.tryPost(make_move_frame(connection, -3, 4)) ==
               snf::server::FramePostResult::Accepted);
        zone = std::get_if<snf::server::ZoneCommandRoute>(&commands.posted->route);
        assert(zone != nullptr);
        const auto* move = std::get_if<snf::server::MoveInZoneCommand>(&zone->command);
        assert(move != nullptr);
        assert(move->route_epoch == route_epoch);
        assert((move->position == snf::server::ZonePosition{.x = -3, .y = 4}));

        assert(gateway.tryPost(snf::server::FrameEnvelope{
                   .connection = connection,
                   .frame =
                       snf::protocol::Frame{
                           .type = snf::protocol::MessageType::LeaveZone,
                           .request_id = 32,
                           .payload = {},
                       },
               }) == snf::server::FramePostResult::Accepted);
        zone = std::get_if<snf::server::ZoneCommandRoute>(&commands.posted->route);
        assert(zone != nullptr);
        const auto* leave = std::get_if<snf::server::LeaveZoneCommand>(&zone->command);
        assert(leave != nullptr);
        assert(leave->route_epoch == route_epoch);
        assert(!sessions.locationFor(connection).has_value());
        const int post_count_after_leave = commands.post_count;
        assert(gateway.tryPost(make_move_frame(connection, 0, 0)) ==
               snf::server::FramePostResult::InvalidPayload);
        assert(commands.post_count == post_count_after_leave);
    }

    void test_connection_close_leaves_zone_before_player_passivation()
    {
        RecordingRoutedIngress commands;
        snf::server::PlayerSessionDirectory sessions;
        snf::server::RouteCoordinator routes;
        snf::server::ProtocolGateway gateway{commands, sessions, routes};
        const snf::net::ConnectionId connection{.descriptor = 61, .generation = 31};
        const snf::server::PlayerId player{.value = 102};
        assert(gateway.tryPost(make_auth_frame(connection, player)) ==
               snf::server::FramePostResult::Accepted);
        assert(gateway.tryPost(make_enter_frame(connection, 6, 0, 0)) ==
               snf::server::FramePostResult::Accepted);
        assert(gateway.tryPost(make_move_frame(connection, -7, 12)) ==
               snf::server::FramePostResult::Accepted);

        const std::size_t before = commands.route_indices.size();
        assert(gateway.tryPostConnectionClosed(snf::server::ConnectionClosed{
                   .connection = connection,
                   .cause = snf::server::ConnectionCloseCause::PeerClosed,
                   .has_location_snapshot = false,
                   .last_location = std::nullopt,
               }) == snf::server::PostResult::Accepted);
        assert(commands.route_indices.size() == before + 2);
        assert(commands.route_indices[before] == 2);
        assert(commands.route_indices[before + 1] == 1);
        const auto* close =
            std::get_if<snf::server::ConnectionClosedRoute>(&commands.posted->route);
        assert(close != nullptr);
        assert((close->last_location == snf::server::PlayerLocation{
                                            .zone = snf::server::ZoneId{.value = 6},
                                            .position = {.x = -7, .y = 12},
                                        }));
    }

    void test_cross_zone_handoff_hides_route_until_target_completion()
    {
        RecordingRoutedIngress commands;
        snf::server::PlayerSessionDirectory sessions;
        snf::server::RouteCoordinator routes{2};
        snf::server::PartyCoordinator parties{4};
        const auto wake = snf::test::make_wake_descriptor();
        snf::server::ZoneTransitionChannel transitions{2, wake.getDescriptor()};
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 4, .max_slots_per_connection = 4},
            wake.getDescriptor()};
        snf::server::ProtocolZoneResultSink zone_results{outbound};
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::ProtocolGateway gateway{
            commands, sessions, routes, parties, transitions, lifecycle, zone_results, 1};
        const snf::net::ConnectionId connection{.descriptor = 62, .generation = 32};
        const snf::server::PlayerId player{.value = 103};
        const snf::server::ZoneId source_zone{.value = 10};
        const snf::server::ZoneId target_zone{.value = 11};

        assert(gateway.tryPost(make_auth_frame(connection, player)) ==
               snf::server::FramePostResult::Accepted);
        assert(gateway.tryPost(make_enter_frame(connection, source_zone.value, 1, 2)) ==
               snf::server::FramePostResult::Accepted);
        assert(routes.routeFor(connection)->zone == source_zone);

        assert(gateway.tryPost(make_enter_frame(connection, target_zone.value, 3, 4)) ==
               snf::server::FramePostResult::Accepted);
        const auto* source_stage =
            std::get_if<snf::server::ZoneHandoffCommandRoute>(&commands.posted->route);
        assert(source_stage != nullptr && source_stage->command.handoff);
        const snf::server::ZoneHandoffContext source_context = *source_stage->command.handoff;
        assert(source_context.step == snf::server::ZoneHandoffStep::LeaveSource);
        assert(!routes.routeFor(connection));

        assert(transitions.publish(
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
            }));
        gateway.drainZoneTransitions();
        assert(gateway.zoneHandoffStats().stale_completions == 1);
        assert(!routes.routeFor(connection));

        const int posts_before_busy = commands.post_count;
        assert(gateway.tryPost(make_move_frame(connection, 5, 6)) ==
               snf::server::FramePostResult::Accepted);
        assert(commands.post_count == posts_before_busy);
        const auto busy = outbound.tryPop();
        assert(busy);
        const auto* busy_send = std::get_if<snf::server::SendFrame>(&busy->action);
        assert(busy_send != nullptr);
        assert(busy_send->frame.type == snf::protocol::MessageType::Moved);
        assert(busy_send->frame.payload[0] ==
               static_cast<std::byte>(snf::server::ZoneCommandStatus::TransitionInProgress));
        assert(lifecycle.terminalCount() == 1);

        assert(transitions.publish(source_context.ticket,
                                   snf::server::ZoneHandoffCompletion{
                                       .handoff_id = source_context.handoff_id,
                                       .connection = connection,
                                       .player = player,
                                       .zone = source_zone,
                                       .route_epoch = source_context.route_epoch,
                                       .step = source_context.step,
                                       .status = snf::server::ZoneCommandStatus::Applied,
                                       .position = snf::server::ZonePosition{.x = 1, .y = 2},
                                   }));
        gateway.drainZoneTransitions();
        const auto* target_stage =
            std::get_if<snf::server::ZoneHandoffCommandRoute>(&commands.posted->route);
        assert(target_stage != nullptr && target_stage->command.handoff);
        const snf::server::ZoneHandoffContext target_context = *target_stage->command.handoff;
        assert(target_context.step == snf::server::ZoneHandoffStep::EnterTarget);
        assert(!routes.routeFor(connection));

        assert(transitions.publish(target_context.ticket,
                                   snf::server::ZoneHandoffCompletion{
                                       .handoff_id = target_context.handoff_id,
                                       .connection = connection,
                                       .player = player,
                                       .zone = target_zone,
                                       .route_epoch = target_context.route_epoch,
                                       .step = target_context.step,
                                       .status = snf::server::ZoneCommandStatus::Applied,
                                       .position = snf::server::ZonePosition{.x = 3, .y = 4},
                                   }));
        gateway.drainZoneTransitions();

        const auto route = routes.routeFor(connection);
        assert(route && route->zone == target_zone && route->route_epoch == 2);
        assert((sessions.locationFor(connection) == snf::server::PlayerLocation{
                                                        .zone = target_zone,
                                                        .position = {.x = 3, .y = 4},
                                                    }));
        const auto entered = outbound.tryPop();
        assert(entered);
        const auto* entered_send = std::get_if<snf::server::SendFrame>(&entered->action);
        assert(entered_send != nullptr);
        assert(entered_send->frame.type == snf::protocol::MessageType::ZoneEntered);
        assert(lifecycle.terminalCount() == 2);
        assert(transitions.stats().reservations == 0);

        commands.result = snf::server::PostResult::Full;
        assert(gateway.tryPost(make_enter_frame(connection, source_zone.value, 7, 8)) ==
               snf::server::FramePostResult::Accepted);
        const auto failed = outbound.tryPop();
        assert(failed);
        const auto* failed_send = std::get_if<snf::server::SendFrame>(&failed->action);
        assert(failed_send != nullptr);
        assert(failed_send->frame.type == snf::protocol::MessageType::ZoneEntered);
        assert(failed_send->frame.payload[0] ==
               static_cast<std::byte>(snf::server::ZoneCommandStatus::TransferFailed));
        const auto stable_after_failure = routes.routeFor(connection);
        assert(stable_after_failure && stable_after_failure->zone == target_zone &&
               stable_after_failure->route_epoch == 2);
        assert(lifecycle.terminalCount() == 3);
        assert(transitions.stats().reservations == 0);
        assert(gateway.zoneHandoffStats().failures_before_source_leave == 1);

        commands.result = snf::server::PostResult::Accepted;
        assert(gateway.tryPost(make_enter_frame(connection, source_zone.value, 9, 10)) ==
               snf::server::FramePostResult::Accepted);
        source_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&commands.posted->route);
        assert(source_stage != nullptr && source_stage->command.handoff);
        const snf::server::ZoneHandoffContext compensation_leave = *source_stage->command.handoff;
        commands.results.push_back(snf::server::PostResult::Full);
        commands.results.push_back(snf::server::PostResult::Accepted);
        assert(transitions.publish(compensation_leave.ticket,
                                   snf::server::ZoneHandoffCompletion{
                                       .handoff_id = compensation_leave.handoff_id,
                                       .connection = connection,
                                       .player = player,
                                       .zone = target_zone,
                                       .route_epoch = compensation_leave.route_epoch,
                                       .step = compensation_leave.step,
                                       .status = snf::server::ZoneCommandStatus::Applied,
                                       .position = snf::server::ZonePosition{.x = 3, .y = 4},
                                   }));
        gateway.drainZoneTransitions();
        const auto* restore_stage =
            std::get_if<snf::server::ZoneHandoffCommandRoute>(&commands.posted->route);
        assert(restore_stage != nullptr && restore_stage->command.handoff);
        const snf::server::ZoneHandoffContext restore_context = *restore_stage->command.handoff;
        assert(restore_context.step == snf::server::ZoneHandoffStep::RestoreSource);
        assert(restore_context.route_epoch == 5);
        assert(transitions.publish(restore_context.ticket,
                                   snf::server::ZoneHandoffCompletion{
                                       .handoff_id = restore_context.handoff_id,
                                       .connection = connection,
                                       .player = player,
                                       .zone = target_zone,
                                       .route_epoch = restore_context.route_epoch,
                                       .step = restore_context.step,
                                       .status = snf::server::ZoneCommandStatus::Applied,
                                       .position = snf::server::ZonePosition{.x = 3, .y = 4},
                                   }));
        gateway.drainZoneTransitions();
        const auto compensated_route = routes.routeFor(connection);
        assert(compensated_route && compensated_route->zone == target_zone &&
               compensated_route->route_epoch == 5);
        const auto compensation_reply = outbound.tryPop();
        assert(compensation_reply);
        const auto* compensation_send =
            std::get_if<snf::server::SendFrame>(&compensation_reply->action);
        assert(compensation_send != nullptr);
        assert(compensation_send->frame.payload[0] ==
               static_cast<std::byte>(snf::server::ZoneCommandStatus::TransferFailed));
        assert(lifecycle.terminalCount() == 4);
        assert(gateway.zoneHandoffStats().target_failures == 1);
        assert(gateway.zoneHandoffStats().compensated == 1);
        assert(routes.stats().handoffs_restored == 1);

        assert(gateway.tryPost(make_enter_frame(connection, source_zone.value, 11, 12)) ==
               snf::server::FramePostResult::Accepted);
        source_stage = std::get_if<snf::server::ZoneHandoffCommandRoute>(&commands.posted->route);
        assert(source_stage != nullptr && source_stage->command.handoff);
        const snf::server::ZoneHandoffContext fatal_leave = *source_stage->command.handoff;
        commands.results.push_back(snf::server::PostResult::Full);
        commands.results.push_back(snf::server::PostResult::Full);
        assert(transitions.publish(fatal_leave.ticket,
                                   snf::server::ZoneHandoffCompletion{
                                       .handoff_id = fatal_leave.handoff_id,
                                       .connection = connection,
                                       .player = player,
                                       .zone = target_zone,
                                       .route_epoch = fatal_leave.route_epoch,
                                       .step = fatal_leave.step,
                                       .status = snf::server::ZoneCommandStatus::Applied,
                                       .position = snf::server::ZonePosition{.x = 3, .y = 4},
                                   }));
        gateway.drainZoneTransitions();
        assert(!routes.routeFor(connection));
        const auto fatal_location = sessions.locationSnapshotFor(connection);
        assert(fatal_location.known && !fatal_location.location);
        std::vector<snf::net::ConnectionId> failed_connections;
        assert(!outbound.takePendingAdmissionFailures(failed_connections));
        assert(failed_connections == std::vector<snf::net::ConnectionId>{connection});
        assert(gateway.zoneHandoffStats().fatal == 1);
        assert(lifecycle.terminalCount() == 5);
        assert(transitions.stats().reservations == 0);
    }

    void test_cross_zone_disconnect_waits_for_source_or_target_cleanup()
    {
        RecordingRoutedIngress commands;
        snf::server::PlayerSessionDirectory sessions;
        snf::server::RouteCoordinator routes{2};
        snf::server::PartyCoordinator parties{4};
        const auto wake = snf::test::make_wake_descriptor();
        snf::server::ZoneTransitionChannel transitions{2, wake.getDescriptor()};
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 4, .max_slots_per_connection = 4},
            wake.getDescriptor()};
        snf::server::ProtocolZoneResultSink zone_results{outbound};
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::ProtocolGateway gateway{
            commands, sessions, routes, parties, transitions, lifecycle, zone_results, 1};
        const snf::server::ZoneId source_zone{.value = 20};
        const snf::server::ZoneId target_zone{.value = 21};

        const auto begin_transfer =
            [&](const snf::net::ConnectionId connection, const snf::server::PlayerId player)
        {
            assert(gateway.tryPost(make_auth_frame(connection, player)) ==
                   snf::server::FramePostResult::Accepted);
            assert(gateway.tryPost(make_enter_frame(connection, source_zone.value, 1, 2)) ==
                   snf::server::FramePostResult::Accepted);
            assert(gateway.tryPost(make_enter_frame(connection, target_zone.value, 3, 4)) ==
                   snf::server::FramePostResult::Accepted);
            const auto* stage =
                std::get_if<snf::server::ZoneHandoffCommandRoute>(&commands.posted->route);
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
        assert(gateway.tryPostConnectionClosed(close_value(first_connection)) ==
               snf::server::PostResult::Full);
        assert(transitions.publish(first_source.ticket,
                                   snf::server::ZoneHandoffCompletion{
                                       .handoff_id = first_source.handoff_id,
                                       .connection = first_connection,
                                       .player = first_player,
                                       .zone = source_zone,
                                       .route_epoch = first_source.route_epoch,
                                       .step = first_source.step,
                                       .status = snf::server::ZoneCommandStatus::Applied,
                                       .position = snf::server::ZonePosition{.x = 1, .y = 2},
                                   }));
        gateway.drainZoneTransitions();
        assert(gateway.zoneTransitionsDrained());
        assert(!routes.routeFor(first_connection));
        const auto first_snapshot = sessions.locationSnapshotFor(first_connection);
        assert(first_snapshot.known && !first_snapshot.location);
        assert(gateway.tryPostConnectionClosed(close_value(first_connection)) ==
               snf::server::PostResult::Accepted);
        const auto* first_close =
            std::get_if<snf::server::ConnectionClosedRoute>(&commands.posted->route);
        assert(first_close != nullptr && first_close->has_location_snapshot &&
               !first_close->last_location);

        const snf::net::ConnectionId second_connection{.descriptor = 71, .generation = 41};
        const snf::server::PlayerId second_player{.value = 111};
        const auto second_source = begin_transfer(second_connection, second_player);
        assert(transitions.publish(second_source.ticket,
                                   snf::server::ZoneHandoffCompletion{
                                       .handoff_id = second_source.handoff_id,
                                       .connection = second_connection,
                                       .player = second_player,
                                       .zone = source_zone,
                                       .route_epoch = second_source.route_epoch,
                                       .step = second_source.step,
                                       .status = snf::server::ZoneCommandStatus::Applied,
                                       .position = snf::server::ZonePosition{.x = 1, .y = 2},
                                   }));
        gateway.drainZoneTransitions();
        const auto* target_stage =
            std::get_if<snf::server::ZoneHandoffCommandRoute>(&commands.posted->route);
        assert(target_stage != nullptr && target_stage->command.handoff);
        const snf::server::ZoneHandoffContext target_context = *target_stage->command.handoff;
        assert(gateway.tryPostConnectionClosed(close_value(second_connection)) ==
               snf::server::PostResult::Full);
        assert(transitions.publish(target_context.ticket,
                                   snf::server::ZoneHandoffCompletion{
                                       .handoff_id = target_context.handoff_id,
                                       .connection = second_connection,
                                       .player = second_player,
                                       .zone = target_zone,
                                       .route_epoch = target_context.route_epoch,
                                       .step = target_context.step,
                                       .status = snf::server::ZoneCommandStatus::Applied,
                                       .position = snf::server::ZonePosition{.x = 3, .y = 4},
                                   }));
        gateway.drainZoneTransitions();
        const auto* cleanup_stage =
            std::get_if<snf::server::ZoneHandoffCommandRoute>(&commands.posted->route);
        assert(cleanup_stage != nullptr && cleanup_stage->command.handoff);
        const snf::server::ZoneHandoffContext cleanup_context = *cleanup_stage->command.handoff;
        assert(cleanup_context.step == snf::server::ZoneHandoffStep::CleanupTarget);
        assert(transitions.publish(cleanup_context.ticket,
                                   snf::server::ZoneHandoffCompletion{
                                       .handoff_id = cleanup_context.handoff_id,
                                       .connection = second_connection,
                                       .player = second_player,
                                       .zone = target_zone,
                                       .route_epoch = cleanup_context.route_epoch,
                                       .step = cleanup_context.step,
                                       .status = snf::server::ZoneCommandStatus::Applied,
                                       .position = snf::server::ZonePosition{.x = 3, .y = 4},
                                   }));
        gateway.drainZoneTransitions();
        assert(gateway.zoneTransitionsDrained());
        assert(gateway.tryPostConnectionClosed(close_value(second_connection)) ==
               snf::server::PostResult::Accepted);
        const auto* second_close =
            std::get_if<snf::server::ConnectionClosedRoute>(&commands.posted->route);
        assert(second_close != nullptr && second_close->has_location_snapshot &&
               !second_close->last_location);
        assert(gateway.zoneHandoffStats().disconnect_cleanups == 2);
        assert(lifecycle.terminalCount() == 2);

        const snf::net::ConnectionId third_connection{.descriptor = 72, .generation = 42};
        const snf::server::PlayerId third_player{.value = 112};
        static_cast<void>(begin_transfer(third_connection, third_player));
        gateway.close();
        gateway.cancel();
        assert(gateway.zoneTransitionsDrained());
        assert(!routes.handoffFor(third_connection));
        const auto third_snapshot = sessions.locationSnapshotFor(third_connection);
        assert(third_snapshot.known && !third_snapshot.location);
        assert(gateway.zoneHandoffStats().shutdown_cancels == 1);
        assert(lifecycle.terminalCount() == 3);
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
}
