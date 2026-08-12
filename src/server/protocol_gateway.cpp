#include "snf/server/protocol_gateway.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <utility>

namespace
{
    std::uint32_t read_u32(std::span<const std::byte> bytes, const std::size_t offset)
    {
        return (std::to_integer<std::uint32_t>(bytes[offset]) << 24U) |
               (std::to_integer<std::uint32_t>(bytes[offset + 1]) << 16U) |
               (std::to_integer<std::uint32_t>(bytes[offset + 2]) << 8U) |
               std::to_integer<std::uint32_t>(bytes[offset + 3]);
    }

    std::uint64_t read_u64(std::span<const std::byte> bytes, const std::size_t offset)
    {
        return (static_cast<std::uint64_t>(read_u32(bytes, offset)) << 32U) |
               read_u32(bytes, offset + 4);
    }

    snf::server::FramePostResult frame_post_result(const snf::server::PostResult result)
    {
        switch (result)
        {
        case snf::server::PostResult::Accepted:
            return snf::server::FramePostResult::Accepted;
        case snf::server::PostResult::Full:
            return snf::server::FramePostResult::Full;
        case snf::server::PostResult::Closed:
            return snf::server::FramePostResult::Closed;
        }
        return snf::server::FramePostResult::Closed;
    }
}

namespace snf::server
{
    ProtocolGateway::ProtocolGateway(RoutedCommandIngress& commands)
        : ProtocolGateway(MessageDispatcher{}, commands)
    {
    }

    ProtocolGateway::ProtocolGateway(RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions)
        : ProtocolGateway(MessageDispatcher{}, commands, sessions)
    {
    }

    ProtocolGateway::ProtocolGateway(MessageDispatcher dispatcher, RoutedCommandIngress& commands)
        : _dispatcher(std::move(dispatcher))
        , _commands(commands)
        , _owned_sessions()
        , _sessions(_owned_sessions)
        , _owned_routes()
        , _routes(_owned_routes)
        , _owned_parties()
        , _parties(_owned_parties)
    {
    }

    ProtocolGateway::ProtocolGateway(MessageDispatcher dispatcher,
                                     RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions)
        : _dispatcher(std::move(dispatcher))
        , _commands(commands)
        , _owned_sessions()
        , _sessions(sessions)
        , _owned_routes()
        , _routes(_owned_routes)
        , _owned_parties()
        , _parties(_owned_parties)
    {
    }

    ProtocolGateway::ProtocolGateway(RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions,
                                     RouteCoordinator& routes)
        : ProtocolGateway(MessageDispatcher{}, commands, sessions, routes)
    {
    }

    ProtocolGateway::ProtocolGateway(MessageDispatcher dispatcher,
                                     RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions,
                                     RouteCoordinator& routes)
        : _dispatcher(std::move(dispatcher))
        , _commands(commands)
        , _owned_sessions()
        , _sessions(sessions)
        , _owned_routes()
        , _routes(routes)
        , _owned_parties()
        , _parties(_owned_parties)
    {
    }

    ProtocolGateway::ProtocolGateway(RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions,
                                     RouteCoordinator& routes,
                                     ZoneTimerService& zone_timers)
        : ProtocolGateway(MessageDispatcher{}, commands, sessions, routes, zone_timers)
    {
    }

    ProtocolGateway::ProtocolGateway(MessageDispatcher dispatcher,
                                     RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions,
                                     RouteCoordinator& routes,
                                     ZoneTimerService& zone_timers)
        : _dispatcher(std::move(dispatcher))
        , _commands(commands)
        , _owned_sessions()
        , _sessions(sessions)
        , _owned_routes()
        , _routes(routes)
        , _owned_parties()
        , _parties(_owned_parties)
        , _zone_timers(&zone_timers)
    {
    }

    ProtocolGateway::ProtocolGateway(RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions,
                                     RouteCoordinator& routes,
                                     ZoneTimerService& zone_timers,
                                     PartyCoordinator& parties)
        : ProtocolGateway(MessageDispatcher{}, commands, sessions, routes, zone_timers, parties)
    {
    }

    ProtocolGateway::ProtocolGateway(MessageDispatcher dispatcher,
                                     RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions,
                                     RouteCoordinator& routes,
                                     ZoneTimerService& zone_timers,
                                     PartyCoordinator& parties)
        : _dispatcher(std::move(dispatcher))
        , _commands(commands)
        , _owned_sessions()
        , _sessions(sessions)
        , _owned_routes()
        , _routes(routes)
        , _owned_parties()
        , _parties(parties)
        , _zone_timers(&zone_timers)
    {
    }

    FramePostResult ProtocolGateway::tryPost(FrameEnvelope envelope)
    {
        const auto post_zone =
            [this, connection = envelope.connection](ZoneCommandRoute route) -> FramePostResult
        {
            return frame_post_result(_commands.tryPost(RoutedCommand{
                .connection = connection,
                .route = std::move(route),
            }));
        };
        const auto post_party =
            [this, connection = envelope.connection](PartyCommandRoute route) -> FramePostResult
        {
            return frame_post_result(_commands.tryPost(RoutedCommand{
                .connection = connection,
                .route = std::move(route),
            }));
        };

        if (envelope.frame.type == snf::protocol::MessageType::PartyJoin)
        {
            constexpr std::size_t PARTY_JOIN_PAYLOAD_SIZE = 8;
            const auto player = _sessions.playerFor(envelope.connection);
            if (!player || envelope.frame.payload.size() != PARTY_JOIN_PAYLOAD_SIZE)
            {
                return FramePostResult::InvalidPayload;
            }
            const PartyId party{
                .value = read_u64(std::span<const std::byte>{envelope.frame.payload}, 0),
            };
            const auto admission = _parties.tryJoin(envelope.connection, *player, party);
            if (!admission)
            {
                return FramePostResult::InvalidPayload;
            }

            FramePostResult result;
            try
            {
                result = post_party(PartyCommandRoute{
                    .party = party,
                    .command =
                        JoinPartyCommand{
                            .player = *player,
                            .membership_epoch = admission->route.membership_epoch,
                        },
                    .reply_kind = PartyReplyKind::Joined,
                    .request_id = envelope.frame.request_id,
                });
            }
            catch (...)
            {
                _parties.rollbackJoin(*admission);
                throw;
            }
            if (result != FramePostResult::Accepted)
            {
                _parties.rollbackJoin(*admission);
            }
            return result;
        }

        if (envelope.frame.type == snf::protocol::MessageType::PartyLeave)
        {
            const auto route = _parties.beginLeave(envelope.connection);
            if (!route || !envelope.frame.payload.empty())
            {
                return FramePostResult::InvalidPayload;
            }
            FramePostResult result;
            try
            {
                result = post_party(PartyCommandRoute{
                    .party = route->party,
                    .command =
                        LeavePartyCommand{
                            .player = route->player,
                            .membership_epoch = route->membership_epoch,
                        },
                    .reply_kind = PartyReplyKind::Left,
                    .request_id = envelope.frame.request_id,
                });
            }
            catch (...)
            {
                _parties.rollbackLeave(*route);
                throw;
            }
            if (result == FramePostResult::Full)
            {
                _parties.rollbackLeave(*route);
            }
            else if (result == FramePostResult::Closed)
            {
                _parties.abandon(route->connection);
            }
            return result;
        }

        if (envelope.frame.type == snf::protocol::MessageType::EnterZone)
        {
            constexpr std::size_t ENTER_PAYLOAD_SIZE = 16;
            const auto player = _sessions.playerFor(envelope.connection);
            if (!player || envelope.frame.payload.size() != ENTER_PAYLOAD_SIZE)
            {
                return FramePostResult::InvalidPayload;
            }

            const std::span<const std::byte> payload{envelope.frame.payload};
            const ZoneId zone{.value = read_u64(payload, 0)};
            ZonePosition position{
                .x = static_cast<std::int32_t>(read_u32(payload, 8)),
                .y = static_cast<std::int32_t>(read_u32(payload, 12)),
            };
            if (const auto restored = _sessions.locationFor(envelope.connection);
                restored && restored->zone == zone)
            {
                position = restored->position;
            }
            const auto admission = _routes.tryEnter(envelope.connection, *player, zone);
            if (!admission)
            {
                return FramePostResult::InvalidPayload;
            }

            ZoneTimerRegistration timer_registration{
                .result = snf::runtime::PostResult::Accepted,
                .timer = std::nullopt,
                .created = false,
            };
            if (_zone_timers != nullptr)
            {
                timer_registration = _zone_timers->tryEnsureTimer(zone);
                if (timer_registration.result != snf::runtime::PostResult::Accepted)
                {
                    _routes.rollbackEnter(*admission);
                    return frame_post_result(timer_registration.result);
                }
            }

            FramePostResult result;
            try
            {
                result = post_zone(ZoneCommandRoute{
                    .zone = zone,
                    .command =
                        EnterZoneCommand{
                            .player = *player,
                            .route_epoch = admission->route.route_epoch,
                            .position = position,
                        },
                    .reply_kind = ZoneReplyKind::Entered,
                    .request_id = envelope.frame.request_id,
                });
            }
            catch (...)
            {
                _routes.rollbackEnter(*admission);
                if (_zone_timers != nullptr && timer_registration.created)
                {
                    try
                    {
                        static_cast<void>(_zone_timers->tryCancelTimer(zone));
                    }
                    catch (...)
                    {
                        // Preserve the command-post failure that started cleanup.
                    }
                }
                throw;
            }
            if (result != FramePostResult::Accepted)
            {
                _routes.rollbackEnter(*admission);
                if (_zone_timers != nullptr && timer_registration.created)
                {
                    static_cast<void>(_zone_timers->tryCancelTimer(zone));
                }
            }
            else if (admission->created)
            {
                _sessions.noteLocation(envelope.connection,
                                       PlayerLocation{.zone = zone, .position = position});
            }
            return result;
        }

        if (envelope.frame.type == snf::protocol::MessageType::Move)
        {
            constexpr std::size_t MOVE_PAYLOAD_SIZE = 8;
            const auto route = _routes.routeFor(envelope.connection);
            if (!route || envelope.frame.payload.size() != MOVE_PAYLOAD_SIZE)
            {
                return FramePostResult::InvalidPayload;
            }

            const std::span<const std::byte> payload{envelope.frame.payload};
            const ZonePosition position{
                .x = static_cast<std::int32_t>(read_u32(payload, 0)),
                .y = static_cast<std::int32_t>(read_u32(payload, 4)),
            };
            const FramePostResult result = post_zone(ZoneCommandRoute{
                .zone = route->zone,
                .command =
                    MoveInZoneCommand{
                        .player = route->player,
                        .route_epoch = route->route_epoch,
                        .position = position,
                    },
                .reply_kind = ZoneReplyKind::Moved,
                .request_id = envelope.frame.request_id,
            });
            if (result == FramePostResult::Accepted)
            {
                _sessions.noteLocation(envelope.connection,
                                       PlayerLocation{.zone = route->zone, .position = position});
            }
            return result;
        }

        if (envelope.frame.type == snf::protocol::MessageType::LeaveZone)
        {
            const auto route = _routes.routeFor(envelope.connection);
            if (!route || !envelope.frame.payload.empty())
            {
                return FramePostResult::InvalidPayload;
            }

            const FramePostResult result = post_zone(ZoneCommandRoute{
                .zone = route->zone,
                .command =
                    LeaveZoneCommand{
                        .player = route->player,
                        .route_epoch = route->route_epoch,
                    },
                .reply_kind = ZoneReplyKind::Left,
                .request_id = envelope.frame.request_id,
            });
            if (result != FramePostResult::Full)
            {
                _routes.completeLeave(*route);
                _sessions.noteLocation(envelope.connection, std::nullopt);
                if (_zone_timers != nullptr && _routes.routeCountFor(route->zone) == 0)
                {
                    static_cast<void>(_zone_timers->tryCancelTimer(route->zone));
                }
            }
            return result;
        }

        DispatchResult dispatch_result = _dispatcher.dispatch(std::move(envelope.frame));
        if (!dispatch_result.handled())
        {
            return dispatch_result.status == DispatchStatus::HandlerNotFound
                       ? FramePostResult::UnsupportedMessage
                       : FramePostResult::InvalidPayload;
        }

        PlayerActorId actor = provisionalActorIdFor(envelope.connection);
        std::optional<PlayerId> new_attachment;
        if (const auto* authenticate = std::get_if<AuthenticateCommand>(&*dispatch_result.command))
        {
            const PlayerAttachResult attach_result =
                _sessions.tryAttach(envelope.connection, authenticate->player);
            if (attach_result == PlayerAttachResult::Attached)
            {
                new_attachment = authenticate->player;
            }
            else if (attach_result != PlayerAttachResult::AlreadyAttached)
            {
                return FramePostResult::InvalidPayload;
            }
            actor = authenticate->player;
        }
        else if (const auto player = _sessions.playerFor(envelope.connection))
        {
            actor = *player;
        }
        else if (std::holds_alternative<PurchaseCommand>(*dispatch_result.command))
        {
            // A purchase is persistent Player state. It must never execute on the
            // connection-scoped provisional actor used by unauthenticated PING.
            return FramePostResult::InvalidPayload;
        }

        PostResult post_result;
        try
        {
            post_result = _commands.tryPost(RoutedCommand{
                .connection = envelope.connection,
                .route =
                    PlayerCommandRoute{
                        .actor = actor,
                        .command = std::move(*dispatch_result.command),
                    },
            });
        }
        catch (...)
        {
            if (new_attachment)
            {
                _sessions.rollbackAttach(envelope.connection, *new_attachment);
            }
            throw;
        }

        if (post_result == PostResult::Accepted)
        {
            if (actor.kind() == snf::runtime::ActorKind::ProvisionalPlayer)
            {
                static_cast<void>(_sessions.noteProvisionalActivity(envelope.connection));
            }
        }
        else if (new_attachment)
        {
            _sessions.rollbackAttach(envelope.connection, *new_attachment);
        }

        switch (post_result)
        {
        case PostResult::Accepted:
            return FramePostResult::Accepted;
        case PostResult::Full:
            return FramePostResult::Full;
        case PostResult::Closed:
            return FramePostResult::Closed;
        }

        return FramePostResult::Closed;
    }

    PostResult ProtocolGateway::tryPostConnectionClosed(ConnectionClosed closed)
    {
        if (!closed.has_location_snapshot)
        {
            const PlayerLocationSnapshot snapshot =
                _sessions.locationSnapshotFor(closed.connection);
            closed.has_location_snapshot = snapshot.known;
            closed.last_location = snapshot.location;
        }
        if (const auto current_party = _parties.routeFor(closed.connection);
            current_party && !current_party->leaving)
        {
            const auto party = _parties.beginLeave(closed.connection);
            if (!party)
            {
                return PostResult::Full;
            }
            PostResult party_result;
            try
            {
                party_result = _commands.tryPost(RoutedCommand{
                    .connection = closed.connection,
                    .route =
                        PartyCommandRoute{
                            .party = party->party,
                            .command =
                                LeavePartyCommand{
                                    .player = party->player,
                                    .membership_epoch = party->membership_epoch,
                                },
                            .reply_kind = std::nullopt,
                            .request_id = 0,
                        },
                });
            }
            catch (...)
            {
                _parties.rollbackLeave(*party);
                throw;
            }
            if (party_result == PostResult::Full)
            {
                _parties.rollbackLeave(*party);
                return PostResult::Full;
            }
            if (party_result == PostResult::Closed)
            {
                _parties.abandon(party->connection);
            }
        }
        if (const auto route = _routes.routeFor(closed.connection))
        {
            const PostResult zone_result = _commands.tryPost(RoutedCommand{
                .connection = closed.connection,
                .route =
                    ZoneCommandRoute{
                        .zone = route->zone,
                        .command =
                            LeaveZoneCommand{
                                .player = route->player,
                                .route_epoch = route->route_epoch,
                            },
                        .reply_kind = std::nullopt,
                        .request_id = 0,
                    },
            });
            if (zone_result == PostResult::Full)
            {
                return PostResult::Full;
            }
            _routes.completeLeave(*route);
            if (_zone_timers != nullptr && _routes.routeCountFor(route->zone) == 0)
            {
                static_cast<void>(_zone_timers->tryCancelTimer(route->zone));
            }
        }

        const std::optional<PlayerId> player = _sessions.playerFor(closed.connection);
        const PlayerActorId actor = player
                                        ? PlayerActorId{*player}
                                        : PlayerActorId{provisionalActorIdFor(closed.connection)};
        const bool began_persistent_close = player && _sessions.beginClose(closed.connection);

        PostResult result;
        try
        {
            result = _commands.tryPost(RoutedCommand{
                .connection = closed.connection,
                .route =
                    ConnectionClosedRoute{
                        .actor = actor,
                        .cause = closed.cause,
                        .has_location_snapshot = closed.has_location_snapshot,
                        .last_location = closed.last_location,
                    },
            });
        }
        catch (...)
        {
            if (began_persistent_close)
            {
                _sessions.rollbackClose(closed.connection);
            }
            throw;
        }

        if (result == PostResult::Full)
        {
            if (began_persistent_close)
            {
                _sessions.rollbackClose(closed.connection);
            }
            return result;
        }

        if (result == PostResult::Closed)
        {
            _sessions.abandon(closed.connection);
        }
        else if (!player)
        {
            _sessions.clearProvisionalActivity(closed.connection);
        }

        return result;
    }

    void ProtocolGateway::close() noexcept
    {
        _commands.close();
    }

    void ProtocolGateway::cancel() noexcept
    {
        _commands.cancel();
    }
}
