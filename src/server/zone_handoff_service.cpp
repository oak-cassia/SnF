#include "snf/server/zone_handoff_service.hpp"

#include <chrono>
#include <optional>
#include <stdexcept>
#include <utility>

namespace snf::server
{
    ZoneHandoffService::ZoneHandoffService(RoutedCommandIngress& commands,
                                           PlayerSessionDirectory& sessions,
                                           RouteCoordinator& routes,
                                           ZoneTransitionChannel& zone_transitions,
                                           CommandLifecycleSink& lifecycle,
                                           ProtocolZoneResultSink& zone_results,
                                           const std::size_t max_completions_per_turn)
        : _commands(commands)
        , _sessions(sessions)
        , _routes(routes)
        , _zone_transitions(zone_transitions)
        , _lifecycle(lifecycle)
        , _zone_results(zone_results)
        , _max_completions_per_turn(max_completions_per_turn)
    {
        if (_max_completions_per_turn == 0 || _max_completions_per_turn > zone_transitions.capacity())
        {
            throw std::invalid_argument{"Zone completion turn budget must be positive and no greater than capacity"};
        }
        _active.reserve(zone_transitions.capacity());
    }

    bool ZoneHandoffService::tryReplyTransitionInProgress(const snf::net::ConnectionId connection, const std::uint32_t request_id, const ZoneReplyKind kind)
    {
        const auto handoff = _routes.handoffFor(connection);
        if (!handoff)
        {
            return false;
        }

        const CommandReleaseToken release{_lifecycle, connection};
        _zone_results.replyStatus(
            connection, handoff->source.player, handoff->target_zone, handoff->target_epoch, handoff->requested_target_position, request_id, kind, ZoneCommandStatus::TransitionInProgress);
        ++_transition_busy_replies;
        return true;
    }

    bool ZoneHandoffService::noteDisconnect(const snf::net::ConnectionId connection) noexcept
    {
        const auto active = _active.find(connection);
        if (active == _active.end())
        {
            return false;
        }

        active->second.disconnecting = true;
        return true;
    }

    void ZoneHandoffService::close() noexcept
    {
        _admission_closed = true;
    }

    void ZoneHandoffService::cancel() noexcept
    {
        _admission_closed = true;
        _zone_transitions.cancel();
        _shutdown_handoff_cancels += _active.size();
        for (const auto& [connection, active] : _active)
        {
            static_cast<void>(active);
            _sessions.noteLocation(connection, std::nullopt);
            _routes.abandon(connection);
        }
        _active.clear();
    }

    FramePostResult ZoneHandoffService::tryStart(
        const snf::net::ConnectionId connection, const std::uint32_t request_id, const PlayerId player, const ZoneId target_zone, const ZonePosition requested_position, const SessionRoute& source)
    {
        if (_admission_closed)
        {
            return FramePostResult::InvalidPayload;
        }

        const auto source_location = _sessions.locationFor(connection);
        if (!source_location || source_location->zone != source.zone)
        {
            return FramePostResult::InvalidPayload;
        }

        CommandReleaseToken release{_lifecycle, connection};

        const auto handoff = _routes.tryBeginHandoff(connection, player, target_zone, source_location->position, requested_position, request_id);
        if (!handoff)
        {
            _zone_results.replyStatus(connection, player, source.zone, source.route_epoch, source_location->position, request_id, ZoneReplyKind::Entered, ZoneCommandStatus::TransferFailed);
            ++_handoff_failures_before_source_leave;
            return FramePostResult::Accepted;
        }

        const auto ticket = _zone_transitions.tryReserve(handoff->id);
        if (!ticket)
        {
            static_cast<void>(_routes.rollbackHandoffBeforeLeave(connection, handoff->id));
            _zone_results.replyStatus(connection, player, source.zone, source.route_epoch, source_location->position, request_id, ZoneReplyKind::Entered, ZoneCommandStatus::TransferFailed);
            ++_handoff_failures_before_source_leave;
            return FramePostResult::Accepted;
        }

        bool inserted_active = false;
        try
        {
            const auto [active, inserted] = _active.try_emplace(connection,
                                                                ActiveHandoff{
                                                                    .ticket = *ticket,
                                                                    .release = std::move(release),
                                                                    .started_at = std::chrono::steady_clock::now(),
                                                                });
            static_cast<void>(active);
            if (!inserted)
            {
                throw std::logic_error{"Connection already owns a Zone handoff"};
            }
            inserted_active = true;

            if (postStage(*handoff, ZoneHandoffStep::LeaveSource) != PostResult::Accepted)
            {
                failHandoffBeforeSourceLeave(connection, handoff->id, ZoneCommandStatus::TransferFailed);
            }
        }
        catch (...)
        {
            if (const auto active = _active.find(connection); inserted_active && active != _active.end())
            {
                _zone_transitions.release(active->second.ticket);
                _active.erase(active);
            }
            else
            {
                _zone_transitions.release(*ticket);
            }
            static_cast<void>(_routes.rollbackHandoffBeforeLeave(connection, handoff->id));
            throw;
        }

        return FramePostResult::Accepted;
    }

    PostResult ZoneHandoffService::postStage(const ZoneHandoff& handoff, const ZoneHandoffStep step)
    {
        const auto active = _active.find(handoff.source.connection);
        if (active == _active.end())
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
        else if (step == ZoneHandoffStep::RestoreSource)
        {
            if (handoff.restore_epoch == 0)
            {
                return PostResult::Closed;
            }
            epoch = handoff.restore_epoch;
            command = EnterZoneCommand{
                .player = handoff.source.player,
                .route_epoch = epoch,
                .position = handoff.last_source_position,
            };
        }
        else if (step == ZoneHandoffStep::CleanupTarget)
        {
            zone = handoff.target_zone;
            epoch = handoff.target_epoch;
            command = LeaveZoneCommand{
                .player = handoff.source.player,
                .route_epoch = epoch,
            };
        }
        else if (step == ZoneHandoffStep::CleanupSource)
        {
            if (handoff.restore_epoch == 0)
            {
                return PostResult::Closed;
            }
            epoch = handoff.restore_epoch;
            command = LeaveZoneCommand{
                .player = handoff.source.player,
                .route_epoch = epoch,
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

    void ZoneHandoffService::failHandoffBeforeSourceLeave(const snf::net::ConnectionId connection, const ZoneHandoffId handoff_id, const ZoneCommandStatus status)
    {
        const auto handoff = _routes.handoffFor(connection);
        const auto active = _active.find(connection);
        if (!handoff || handoff->id != handoff_id || active == _active.end())
        {
            throw std::logic_error{"Zone handoff rollback identity diverged"};
        }

        const ZoneTransitionTicket ticket = active->second.ticket;
        const auto started_at = active->second.started_at;
        if (!_routes.rollbackHandoffBeforeLeave(connection, handoff_id))
        {
            throw std::logic_error{"Zone handoff could not roll back before source leave"};
        }
        _zone_transitions.release(ticket);
        _zone_results.replyStatus(
            connection, handoff->source.player, handoff->source.zone, handoff->source.route_epoch, handoff->last_source_position, handoff->request_id, ZoneReplyKind::Entered, status);
        _zone_transition_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_at));
        ++_handoff_failures_before_source_leave;
        _active.erase(active);
    }

    void ZoneHandoffService::finishActiveHandoff(const snf::net::ConnectionId connection)
    {
        const auto active = _active.find(connection);
        if (active == _active.end())
        {
            return;
        }
        _zone_transitions.release(active->second.ticket);
        _zone_transition_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - active->second.started_at));
        _active.erase(active);
    }

    void ZoneHandoffService::beginSourceRestore(const ZoneHandoff& handoff)
    {
        ++_handoff_target_failures;
        if (!_routes.beginSourceRestore(handoff.source.connection, handoff.id))
        {
            finishFatalHandoff(handoff);
            return;
        }
        const auto restore = _routes.handoffFor(handoff.source.connection);
        if (!restore || postStage(*restore, ZoneHandoffStep::RestoreSource) != PostResult::Accepted)
        {
            finishFatalHandoff(restore.value_or(handoff));
        }
    }

    void ZoneHandoffService::finishSourceRestore(const ZoneHandoff& handoff, const ZonePosition position)
    {
        const auto active = _active.find(handoff.source.connection);
        if (active == _active.end())
        {
            return;
        }
        if (active->second.disconnecting)
        {
            beginDisconnectCleanup(handoff, ZoneHandoffStep::CleanupSource);
            return;
        }

        const auto route = _routes.completeSourceRestore(handoff.source.connection, handoff.id);
        if (!route)
        {
            finishFatalHandoff(handoff);
            return;
        }
        _sessions.noteLocation(handoff.source.connection,
                               PlayerLocation{
                                   .zone = route->zone,
                                   .position = position,
                               });
        _zone_results.replyStatus(handoff.source.connection, route->player, route->zone, route->route_epoch, position, handoff.request_id, ZoneReplyKind::Entered, ZoneCommandStatus::TransferFailed);
        ++_handoffs_compensated;
        finishActiveHandoff(handoff.source.connection);
    }

    void ZoneHandoffService::beginDisconnectCleanup(const ZoneHandoff& handoff, const ZoneHandoffStep cleanup_step)
    {
        if (!_routes.beginCleanup(handoff.source.connection, handoff.id, cleanup_step))
        {
            finishFatalHandoff(handoff);
            return;
        }
        const auto cleanup = _routes.handoffFor(handoff.source.connection);
        if (!cleanup || postStage(*cleanup, cleanup_step) != PostResult::Accepted)
        {
            finishFatalHandoff(cleanup.value_or(handoff));
        }
    }

    void ZoneHandoffService::finishDisconnectedHandoff(const ZoneHandoff& handoff)
    {
        const auto active = _active.find(handoff.source.connection);
        if (active == _active.end())
        {
            return;
        }
        _sessions.noteLocation(handoff.source.connection, std::nullopt);
        _routes.abandon(handoff.source.connection);
        ++_disconnect_handoff_cleanups;
        finishActiveHandoff(handoff.source.connection);
    }

    void ZoneHandoffService::finishFatalHandoff(const ZoneHandoff& handoff)
    {
        const auto active = _active.find(handoff.source.connection);
        if (active == _active.end())
        {
            return;
        }
        const bool disconnecting = active->second.disconnecting;
        _sessions.noteLocation(handoff.source.connection, std::nullopt);
        _routes.abandon(handoff.source.connection);
        if (!disconnecting)
        {
            _zone_results.reportAdmissionFailure(handoff.source.connection);
        }
        ++_fatal_handoffs;
        finishActiveHandoff(handoff.source.connection);
    }

    bool ZoneHandoffService::isValidCompletion(const ZoneHandoff& handoff, const ZoneHandoffCompletion& completion) const noexcept
    {
        if (completion.handoff_id != handoff.id || completion.connection != handoff.source.connection || completion.player != handoff.source.player || completion.step != handoff.step)
        {
            return false;
        }
        if (completion.step == ZoneHandoffStep::LeaveSource)
        {
            return completion.zone == handoff.source.zone && completion.route_epoch == handoff.source.route_epoch;
        }
        if (completion.step == ZoneHandoffStep::EnterTarget)
        {
            return completion.zone == handoff.target_zone && completion.route_epoch == handoff.target_epoch;
        }
        if (completion.step == ZoneHandoffStep::RestoreSource || completion.step == ZoneHandoffStep::CleanupSource)
        {
            return completion.zone == handoff.source.zone && handoff.restore_epoch != 0 && completion.route_epoch == handoff.restore_epoch;
        }
        if (completion.step == ZoneHandoffStep::CleanupTarget)
        {
            return completion.zone == handoff.target_zone && completion.route_epoch == handoff.target_epoch;
        }
        return false;
    }

    void ZoneHandoffService::handleCompletion(ZoneHandoffCompletion completion)
    {
        const auto handoff = _routes.handoffFor(completion.connection);
        const auto active = _active.find(completion.connection);
        if (!handoff || active == _active.end() || !isValidCompletion(*handoff, completion))
        {
            ++_stale_handoff_completions;
            return;
        }

        if (completion.step == ZoneHandoffStep::LeaveSource)
        {
            if (completion.status != ZoneCommandStatus::Applied || !completion.position || !_routes.noteSourceLeft(completion.connection, completion.handoff_id, *completion.position))
            {
                finishFatalHandoff(*handoff);
                return;
            }
            const auto next = _routes.handoffFor(completion.connection);
            if (!next)
            {
                finishFatalHandoff(*handoff);
                return;
            }
            if (active->second.disconnecting)
            {
                finishDisconnectedHandoff(*next);
                return;
            }
            if (postStage(*next, ZoneHandoffStep::EnterTarget) != PostResult::Accepted)
            {
                beginSourceRestore(*next);
            }
            return;
        }

        if (completion.step == ZoneHandoffStep::EnterTarget)
        {
            if ((completion.status != ZoneCommandStatus::Applied && completion.status != ZoneCommandStatus::AlreadyPresent) || !completion.position)
            {
                if (active->second.disconnecting)
                {
                    ++_handoff_target_failures;
                    finishDisconnectedHandoff(*handoff);
                }
                else
                {
                    beginSourceRestore(*handoff);
                }
                return;
            }
            if (active->second.disconnecting)
            {
                beginDisconnectCleanup(*handoff, ZoneHandoffStep::CleanupTarget);
                return;
            }

            const auto route = _routes.completeTargetEnter(completion.connection, completion.handoff_id);
            if (!route)
            {
                finishFatalHandoff(*handoff);
                return;
            }
            _sessions.noteLocation(completion.connection,
                                   PlayerLocation{
                                       .zone = route->zone,
                                       .position = *completion.position,
                                   });

            _zone_results.replyStatus(completion.connection, route->player, route->zone, route->route_epoch, *completion.position, handoff->request_id, ZoneReplyKind::Entered, completion.status);
            finishActiveHandoff(completion.connection);
            return;
        }

        if (completion.step == ZoneHandoffStep::RestoreSource)
        {
            if ((completion.status != ZoneCommandStatus::Applied && completion.status != ZoneCommandStatus::AlreadyPresent) || !completion.position)
            {
                finishFatalHandoff(*handoff);
                return;
            }
            finishSourceRestore(*handoff, *completion.position);
            return;
        }

        if (completion.step == ZoneHandoffStep::CleanupTarget || completion.step == ZoneHandoffStep::CleanupSource)
        {
            if (completion.status != ZoneCommandStatus::Applied && completion.status != ZoneCommandStatus::PlayerMissing)
            {
                finishFatalHandoff(*handoff);
                return;
            }
            finishDisconnectedHandoff(*handoff);
        }
    }

    void ZoneHandoffService::drain()
    {
        for (std::size_t index = 0; index < _max_completions_per_turn; ++index)
        {
            auto completion = _zone_transitions.tryPop();
            if (!completion)
            {
                break;
            }
            handleCompletion(std::move(*completion));
        }
        _zone_transitions.wakeIfPending();
    }

    bool ZoneHandoffService::drained() const noexcept
    {
        return _active.empty() && _zone_transitions.drained();
    }

    ZoneHandoffStats ZoneHandoffService::stats() const noexcept
    {
        return ZoneHandoffStats{
            .transition_nanoseconds = _zone_transition_nanoseconds.snapshot(),
            .failures_before_source_leave = _handoff_failures_before_source_leave,
            .target_failures = _handoff_target_failures,
            .compensated = _handoffs_compensated,
            .fatal = _fatal_handoffs,
            .transition_busy_replies = _transition_busy_replies,
            .stale_completions = _stale_handoff_completions,
            .disconnect_cleanups = _disconnect_handoff_cleanups,
            .shutdown_cancels = _shutdown_handoff_cancels,
            .pending = _active.size(),
        };
    }
}
