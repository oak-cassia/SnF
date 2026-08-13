#include "snf/server/protocol_gateway.hpp"

#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>
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
                                     PartyCoordinator& parties,
                                     ZoneTransitionChannel& zone_transitions,
                                     CommandLifecycleSink& lifecycle,
                                     ProtocolZoneResultSink& zone_results,
                                     const std::size_t max_zone_completions_per_turn)
        : ProtocolGateway(MessageDispatcher{}, commands, sessions, routes, zone_timers, parties)
    {
        if (max_zone_completions_per_turn == 0 ||
            max_zone_completions_per_turn > zone_transitions.capacity())
        {
            throw std::invalid_argument{
                "Zone completion turn budget must be positive and no greater than capacity"};
        }
        _zone_transitions = &zone_transitions;
        _lifecycle = &lifecycle;
        _zone_results = &zone_results;
        _max_zone_completions_per_turn = max_zone_completions_per_turn;
        _active_zone_handoffs.reserve(zone_transitions.capacity());
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

    FramePostResult ProtocolGateway::tryStartZoneHandoff(const FrameEnvelope& envelope,
                                                         const PlayerId player,
                                                         const ZoneId target_zone,
                                                         const ZonePosition requested_position,
                                                         const SessionRoute& source)
    {
        if (_zone_transitions == nullptr || _lifecycle == nullptr || _zone_results == nullptr ||
            _zone_timers == nullptr)
        {
            return FramePostResult::InvalidPayload;
        }

        const auto source_location = _sessions.locationFor(envelope.connection);
        if (!source_location || source_location->zone != source.zone)
        {
            return FramePostResult::InvalidPayload;
        }

        CommandReleaseToken release{*_lifecycle, envelope.connection};
        const ZoneTimerRegistration timer = _zone_timers->tryEnsureTimer(target_zone);
        if (timer.result != snf::runtime::PostResult::Accepted)
        {
            replyZoneStatus(envelope.connection,
                            player,
                            source.zone,
                            source.route_epoch,
                            source_location->position,
                            envelope.frame.request_id,
                            ZoneReplyKind::Entered,
                            ZoneCommandStatus::TransferFailed);
            ++_handoff_failures_before_source_leave;
            return FramePostResult::Accepted;
        }

        const auto handoff = _routes.tryBeginHandoff(envelope.connection,
                                                     player,
                                                     target_zone,
                                                     source_location->position,
                                                     requested_position,
                                                     envelope.frame.request_id);
        if (!handoff)
        {
            if (timer.created)
            {
                static_cast<void>(_zone_timers->tryCancelTimer(target_zone));
            }
            replyZoneStatus(envelope.connection,
                            player,
                            source.zone,
                            source.route_epoch,
                            source_location->position,
                            envelope.frame.request_id,
                            ZoneReplyKind::Entered,
                            ZoneCommandStatus::TransferFailed);
            ++_handoff_failures_before_source_leave;
            return FramePostResult::Accepted;
        }

        const auto ticket = _zone_transitions->tryReserve(handoff->id);
        if (!ticket)
        {
            static_cast<void>(_routes.rollbackHandoffBeforeLeave(envelope.connection, handoff->id));
            if (timer.created)
            {
                static_cast<void>(_zone_timers->tryCancelTimer(target_zone));
            }
            replyZoneStatus(envelope.connection,
                            player,
                            source.zone,
                            source.route_epoch,
                            source_location->position,
                            envelope.frame.request_id,
                            ZoneReplyKind::Entered,
                            ZoneCommandStatus::TransferFailed);
            ++_handoff_failures_before_source_leave;
            return FramePostResult::Accepted;
        }

        try
        {
            const auto [active, inserted] = _active_zone_handoffs.try_emplace(
                envelope.connection,
                ActiveZoneHandoff{
                    .ticket = *ticket,
                    .release = std::move(release),
                    .target_timer_created = timer.created,
                    .started_at = std::chrono::steady_clock::now(),
                });
            static_cast<void>(active);
            if (!inserted)
            {
                throw std::logic_error{"Connection already owns a Zone handoff"};
            }

            if (postZoneHandoffStage(*handoff, ZoneHandoffStep::LeaveSource) !=
                PostResult::Accepted)
            {
                failHandoffBeforeSourceLeave(
                    envelope.connection, handoff->id, ZoneCommandStatus::TransferFailed);
            }
        }
        catch (...)
        {
            if (const auto active = _active_zone_handoffs.find(envelope.connection);
                active != _active_zone_handoffs.end())
            {
                _zone_transitions->release(active->second.ticket);
                _active_zone_handoffs.erase(active);
            }
            else
            {
                _zone_transitions->release(*ticket);
            }
            static_cast<void>(_routes.rollbackHandoffBeforeLeave(envelope.connection, handoff->id));
            if (timer.created)
            {
                static_cast<void>(_zone_timers->tryCancelTimer(target_zone));
            }
            throw;
        }

        return FramePostResult::Accepted;
    }

    PostResult ProtocolGateway::postZoneHandoffStage(const ZoneHandoff& handoff,
                                                     const ZoneHandoffStep step)
    {
        const auto active = _active_zone_handoffs.find(handoff.source.connection);
        if (active == _active_zone_handoffs.end())
        {
            return PostResult::Closed;
        }

        ZoneId zone = handoff.source.zone;
        std::uint64_t epoch = handoff.source.route_epoch;
        ZoneCommand command = LeaveZoneCommand{
            .player = handoff.source.player,
            .route_epoch = epoch,
        };
        if (step == ZoneHandoffStep::EnterTarget)
        {
            zone = handoff.target_zone;
            epoch = handoff.target_epoch;
            command = EnterZoneCommand{
                .player = handoff.source.player,
                .route_epoch = epoch,
                .position = handoff.requested_target_position,
            };
        }
        else if (step != ZoneHandoffStep::LeaveSource)
        {
            return PostResult::Closed;
        }

        return _commands.tryPost(RoutedCommand{
            .connection = handoff.source.connection,
            .route =
                ZoneHandoffCommandRoute{
                    .command =
                        ZoneInboundCommand{
                            .zone = zone,
                            .command = std::move(command),
                            .reply = std::nullopt,
                            .handoff =
                                ZoneHandoffContext{
                                    .handoff_id = handoff.id,
                                    .ticket = active->second.ticket,
                                    .connection = handoff.source.connection,
                                    .player = handoff.source.player,
                                    .step = step,
                                    .route_epoch = epoch,
                                },
                        },
                },
        });
    }

    void ProtocolGateway::failHandoffBeforeSourceLeave(const snf::net::ConnectionId connection,
                                                       const ZoneHandoffId handoff_id,
                                                       const ZoneCommandStatus status)
    {
        const auto handoff = _routes.handoffFor(connection);
        const auto active = _active_zone_handoffs.find(connection);
        if (!handoff || handoff->id != handoff_id || active == _active_zone_handoffs.end())
        {
            throw std::logic_error{"Zone handoff rollback identity diverged"};
        }

        const bool target_timer_created = active->second.target_timer_created;
        const ZoneTransitionTicket ticket = active->second.ticket;
        const auto started_at = active->second.started_at;
        if (!_routes.rollbackHandoffBeforeLeave(connection, handoff_id))
        {
            throw std::logic_error{"Zone handoff could not roll back before source leave"};
        }
        if (target_timer_created && _zone_timers != nullptr &&
            _routes.routeCountFor(handoff->target_zone) == 0)
        {
            static_cast<void>(_zone_timers->tryCancelTimer(handoff->target_zone));
        }
        _zone_transitions->release(ticket);
        replyZoneStatus(connection,
                        handoff->source.player,
                        handoff->source.zone,
                        handoff->source.route_epoch,
                        handoff->last_source_position,
                        handoff->request_id,
                        ZoneReplyKind::Entered,
                        status);
        _zone_transition_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at));
        ++_handoff_failures_before_source_leave;
        _active_zone_handoffs.erase(active);
    }

    void ProtocolGateway::replyZoneStatus(const snf::net::ConnectionId connection,
                                          const PlayerId player,
                                          const ZoneId zone,
                                          const std::uint64_t route_epoch,
                                          const ZonePosition position,
                                          const std::uint32_t request_id,
                                          const ZoneReplyKind kind,
                                          const ZoneCommandStatus status)
    {
        if (_zone_results == nullptr)
        {
            return;
        }
        _zone_results->accept(
            ZoneInboundCommand{
                .zone = zone,
                .command =
                    EnterZoneCommand{
                        .player = player,
                        .route_epoch = route_epoch,
                        .position = position,
                    },
                .reply =
                    ZoneReplyContext{
                        .connection = connection,
                        .request_id = request_id,
                        .kind = kind,
                    },
                .handoff = std::nullopt,
            },
            ZoneResult{
                .status = status,
                .player = player,
                .position = position,
                .route_epoch = route_epoch,
                .tick = 0,
                .visible_players = {},
            });
    }

    bool ProtocolGateway::isValidCompletion(const ZoneHandoff& handoff,
                                            const ZoneHandoffCompletion& completion) const noexcept
    {
        if (completion.handoff_id != handoff.id ||
            completion.connection != handoff.source.connection ||
            completion.player != handoff.source.player || completion.step != handoff.step)
        {
            return false;
        }
        if (completion.step == ZoneHandoffStep::LeaveSource)
        {
            return completion.zone == handoff.source.zone &&
                   completion.route_epoch == handoff.source.route_epoch;
        }
        if (completion.step == ZoneHandoffStep::EnterTarget)
        {
            return completion.zone == handoff.target_zone &&
                   completion.route_epoch == handoff.target_epoch;
        }
        return false;
    }

    void ProtocolGateway::handleZoneHandoffCompletion(ZoneHandoffCompletion completion)
    {
        const auto handoff = _routes.handoffFor(completion.connection);
        const auto active = _active_zone_handoffs.find(completion.connection);
        if (!handoff || active == _active_zone_handoffs.end() ||
            !isValidCompletion(*handoff, completion))
        {
            ++_stale_handoff_completions;
            return;
        }

        if (completion.step == ZoneHandoffStep::LeaveSource)
        {
            if (completion.status != ZoneCommandStatus::Applied || !completion.position ||
                !_routes.noteSourceLeft(
                    completion.connection, completion.handoff_id, *completion.position))
            {
                throw std::logic_error{"Source Zone leave did not complete authoritatively"};
            }
            const auto next = _routes.handoffFor(completion.connection);
            if (!next ||
                postZoneHandoffStage(*next, ZoneHandoffStep::EnterTarget) != PostResult::Accepted)
            {
                throw std::runtime_error{"Target Zone enter could not be admitted"};
            }
            return;
        }

        if ((completion.status != ZoneCommandStatus::Applied &&
             completion.status != ZoneCommandStatus::AlreadyPresent) ||
            !completion.position)
        {
            throw std::logic_error{"Target Zone enter did not complete authoritatively"};
        }

        const auto route =
            _routes.completeTargetEnter(completion.connection, completion.handoff_id);
        if (!route)
        {
            throw std::logic_error{"Target Zone route publish identity diverged"};
        }
        _sessions.noteLocation(completion.connection,
                               PlayerLocation{
                                   .zone = route->zone,
                                   .position = *completion.position,
                               });
        if (_zone_timers != nullptr && _routes.routeCountFor(handoff->source.zone) == 0)
        {
            static_cast<void>(_zone_timers->tryCancelTimer(handoff->source.zone));
        }

        replyZoneStatus(completion.connection,
                        route->player,
                        route->zone,
                        route->route_epoch,
                        *completion.position,
                        handoff->request_id,
                        ZoneReplyKind::Entered,
                        completion.status);
        _zone_transitions->release(active->second.ticket);
        _zone_transition_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - active->second.started_at));
        _active_zone_handoffs.erase(active);
    }

    void ProtocolGateway::drainZoneTransitions()
    {
        if (_zone_transitions == nullptr)
        {
            return;
        }
        for (std::size_t index = 0; index < _max_zone_completions_per_turn; ++index)
        {
            auto completion = _zone_transitions->tryPop();
            if (!completion)
            {
                break;
            }
            handleZoneHandoffCompletion(std::move(*completion));
        }
        _zone_transitions->wakeIfPending();
    }

    ZoneHandoffGatewayStats ProtocolGateway::zoneHandoffStats() const noexcept
    {
        return ZoneHandoffGatewayStats{
            .transition_nanoseconds = _zone_transition_nanoseconds.snapshot(),
            .failures_before_source_leave = _handoff_failures_before_source_leave,
            .transition_busy_replies = _transition_busy_replies,
            .stale_completions = _stale_handoff_completions,
            .pending = _active_zone_handoffs.size(),
        };
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

            if (const auto handoff = _routes.handoffFor(envelope.connection))
            {
                if (_lifecycle == nullptr || _zone_results == nullptr)
                {
                    return FramePostResult::InvalidPayload;
                }
                CommandReleaseToken release{*_lifecycle, envelope.connection};
                replyZoneStatus(envelope.connection,
                                handoff->source.player,
                                handoff->target_zone,
                                handoff->target_epoch,
                                handoff->requested_target_position,
                                envelope.frame.request_id,
                                ZoneReplyKind::Entered,
                                ZoneCommandStatus::TransitionInProgress);
                ++_transition_busy_replies;
                return FramePostResult::Accepted;
            }
            if (const auto source = _routes.routeFor(envelope.connection);
                source && source->zone != zone)
            {
                return tryStartZoneHandoff(envelope, *player, zone, position, *source);
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
            if (envelope.frame.payload.size() != MOVE_PAYLOAD_SIZE)
            {
                return FramePostResult::InvalidPayload;
            }

            if (const auto handoff = _routes.handoffFor(envelope.connection))
            {
                if (_lifecycle == nullptr || _zone_results == nullptr)
                {
                    return FramePostResult::InvalidPayload;
                }
                CommandReleaseToken release{*_lifecycle, envelope.connection};
                replyZoneStatus(envelope.connection,
                                handoff->source.player,
                                handoff->target_zone,
                                handoff->target_epoch,
                                handoff->requested_target_position,
                                envelope.frame.request_id,
                                ZoneReplyKind::Moved,
                                ZoneCommandStatus::TransitionInProgress);
                ++_transition_busy_replies;
                return FramePostResult::Accepted;
            }
            const auto route = _routes.routeFor(envelope.connection);
            if (!route)
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
            if (!envelope.frame.payload.empty())
            {
                return FramePostResult::InvalidPayload;
            }

            if (const auto handoff = _routes.handoffFor(envelope.connection))
            {
                if (_lifecycle == nullptr || _zone_results == nullptr)
                {
                    return FramePostResult::InvalidPayload;
                }
                CommandReleaseToken release{*_lifecycle, envelope.connection};
                replyZoneStatus(envelope.connection,
                                handoff->source.player,
                                handoff->target_zone,
                                handoff->target_epoch,
                                handoff->requested_target_position,
                                envelope.frame.request_id,
                                ZoneReplyKind::Left,
                                ZoneCommandStatus::TransitionInProgress);
                ++_transition_busy_replies;
                return FramePostResult::Accepted;
            }
            const auto route = _routes.routeFor(envelope.connection);
            if (!route)
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
        if (_zone_transitions != nullptr)
        {
            _zone_transitions->cancel();
        }
        for (const auto& [connection, active] : _active_zone_handoffs)
        {
            static_cast<void>(active);
            _routes.abandon(connection);
        }
        _active_zone_handoffs.clear();
        _commands.cancel();
    }
}
