#include "snf/server/room_entry_service.hpp"

#include "snf/protocol/frame.hpp"
#include "snf/server/outbound_action.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

namespace
{
    constexpr std::uint32_t BYTE_MASK = 0xFFU;

    void append_u32(std::vector<std::byte>& bytes, const std::uint32_t value)
    {
        bytes.push_back(static_cast<std::byte>((value >> 24U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>((value >> 16U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>((value >> 8U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>(value & BYTE_MASK));
    }

    void append_u64(std::vector<std::byte>& bytes, const std::uint64_t value)
    {
        append_u32(bytes, static_cast<std::uint32_t>(value >> 32U));
        append_u32(bytes, static_cast<std::uint32_t>(value));
    }
}

namespace snf::server
{
    RoomEntryService::RoomEntryService(RoutedCommandIngress& commands,
                                       PlayerSessionDirectory& sessions,
                                       RouteCoordinator& routes,
                                       RoomTransitionChannel& room_transitions,
                                       CommandLifecycleSink& lifecycle,
                                       OutboundSink& outbound,
                                       ProtocolZoneResultSink& zone_results,
                                       const std::size_t max_completions_per_turn)
        : _commands(commands)
        , _sessions(sessions)
        , _routes(routes)
        , _room_transitions(room_transitions)
        , _lifecycle(lifecycle)
        , _outbound(outbound)
        , _zone_results(zone_results)
        , _max_completions_per_turn(max_completions_per_turn)
    {
        if (_max_completions_per_turn == 0 || _max_completions_per_turn > room_transitions.capacity())
        {
            throw std::invalid_argument{"Room completion turn budget must be positive and no greater than capacity"};
        }
        _active_entries.reserve(room_transitions.capacity());
        _active_returns.reserve(room_transitions.capacity());
    }

    FramePostResult RoomEntryService::tryStart(const snf::net::ConnectionId connection,
                                               const std::uint32_t request_id,
                                               const PlayerId player,
                                               const RoomId room)
    {
        if (_admission_closed)
        {
            return FramePostResult::InvalidPayload;
        }

        const auto source_route = _routes.routeFor(connection);
        if (source_route && source_route->player != player)
        {
            // The session and the route disagree about who this connection is. Only a
            // routing bug produces that, and it is worse than a refused join.
            return FramePostResult::InvalidPayload;
        }
        if (!source_route)
        {
            // No Zone to leave, and none to return to when the battle ends. A client can
            // reach this legitimately -- leave a Zone, then ask for a Room -- so it is
            // answered like the admission failures below rather than closed as a
            // protocol error.
            const CommandReleaseToken refused{_lifecycle, connection};
            replyRoomJoined(connection, request_id, room, RoomCommandStatus::EntryFailed, RoomPhase::Waiting);
            ++_failures_before_source_leave;
            return FramePostResult::Accepted;
        }

        CommandReleaseToken release{_lifecycle, connection};

        const auto entry = _routes.tryBeginRoomEntry(connection, player, room, request_id);
        if (!entry)
        {
            replyRoomJoined(connection, request_id, room, RoomCommandStatus::EntryFailed, RoomPhase::Waiting);
            ++_failures_before_source_leave;
            return FramePostResult::Accepted;
        }

        const auto ticket = _room_transitions.tryReserve(entry->id);
        if (!ticket)
        {
            static_cast<void>(_routes.rollbackRoomEntryBeforeLeave(connection, entry->id));
            replyRoomJoined(connection, request_id, room, RoomCommandStatus::EntryFailed, RoomPhase::Waiting);
            ++_failures_before_source_leave;
            return FramePostResult::Accepted;
        }

        bool inserted_active = false;
        try
        {
            const auto [active_it, inserted] = _active_entries.try_emplace(
                connection,
                ActiveEntry{
                    .connection = connection,
                    .ticket = *ticket,
                    .release = std::move(release),
                    .entry_id = entry->id,
                    .player = player,
                    .room = room,
                    .source_zone = source_route->zone,
                    .source_epoch = source_route->route_epoch,
                    .request_id = request_id,
                    .disconnecting = false,
                    .started_at = std::chrono::steady_clock::now(),
                }
            );
            static_cast<void>(active_it);
            if (!inserted)
            {
                throw std::logic_error{"Connection already owns a Room entry"};
            }
            inserted_active = true;

            const PostResult post_res = _commands.tryPost(RoutedCommand{
                .connection = connection,
                .route =
                    PlayerCommandRoute{
                        .actor = PlayerActorId{player},
                        .command = JoinRoomRequest{.room = room},
                        .request_id = request_id,
                        // Beside the command, not inside it. The Player answers with the
                        // room and the stats it read; which entry saga asked is reactor
                        // identity that its binding pairs back on.
                        .room_entry =
                            RoomEntryContext{
                                .entry_id = entry->id,
                                .return_id = {},
                                .ticket = *ticket,
                                .connection = connection,
                                .player = player,
                                .step = RoomEntryStep::JoinRoom,
                            },
                    },
            });
            if (post_res != PostResult::Accepted)
            {
                failEntryBeforeSourceLeave(connection, room, request_id, RoomCommandStatus::EntryFailed);
            }
        }
        catch (...)
        {
            if (const auto active = _active_entries.find(connection); inserted_active && active != _active_entries.end())
            {
                _room_transitions.release(active->second.ticket);
                _active_entries.erase(active);
            }
            else
            {
                _room_transitions.release(*ticket);
            }
            static_cast<void>(_routes.rollbackRoomEntryBeforeLeave(connection, entry->id));
            throw;
        }

        return FramePostResult::Accepted;
    }

    // The phase in every reply here is Waiting because the reactor does not own it: only
    // the Room knows whether a battle is running. The status is the load-bearing byte.
    bool RoomEntryService::tryReplyRoomBusy(const snf::net::ConnectionId connection, const std::uint32_t request_id, const RoomReplyKind kind)
    {
        const auto entry = _routes.roomEntryFor(connection);
        if (entry)
        {
            const CommandReleaseToken release{_lifecycle, connection};
            replyRoomJoined(connection, request_id, entry->room, RoomCommandStatus::WrongPhase, RoomPhase::Waiting);
            ++_transition_busy_replies;
            return true;
        }

        if (const auto in_room = _routes.inRoomFor(connection))
        {
            // Already in a Room, which hides the zone route -- so without this the join
            // would fall through to tryStart, find no route, and the client would be
            // closed for retrying a join it had already been granted.
            const CommandReleaseToken release{_lifecycle, connection};
            if (kind == RoomReplyKind::Joined)
            {
                replyRoomJoined(connection, request_id, in_room->room, RoomCommandStatus::AlreadyJoined, RoomPhase::Waiting);
            }
            ++_transition_busy_replies;
            return true;
        }

        const auto ret = _routes.roomReturnFor(connection);
        if (ret)
        {
            const CommandReleaseToken release{_lifecycle, connection};
            if (kind == RoomReplyKind::Joined)
            {
                replyRoomJoined(connection, request_id, ret->room, RoomCommandStatus::WrongPhase, RoomPhase::Waiting);
            }
            ++_transition_busy_replies;
            return true;
        }

        return false;
    }

    bool RoomEntryService::tryReplyZoneBlockedByRoom(const snf::net::ConnectionId connection, const std::uint32_t request_id, const ZoneReplyKind kind)
    {
        // An entry in flight has already hidden the zone route, so the command cannot be
        // applied even though the player has not left the Zone yet. That is a transition,
        // and it gets the answer a cross-zone transition gives.
        if (const auto entry = _routes.roomEntryFor(connection))
        {
            const CommandReleaseToken release{_lifecycle, connection};
            _zone_results.replyStatus(
                connection, entry->source.player, entry->source.zone, entry->source.route_epoch, ZonePosition{}, request_id, kind, ZoneCommandStatus::TransitionInProgress);
            ++_transition_busy_replies;
            return true;
        }

        if (const auto in_room = _routes.inRoomFor(connection))
        {
            const CommandReleaseToken release{_lifecycle, connection};
            // The zone named here is the one the return will put them back in. It is the
            // only Zone this connection still has a relationship with, and the epoch is
            // zero because it holds no route while the battle runs.
            _zone_results.replyStatus(connection, in_room->player, in_room->return_zone, 0, in_room->return_position, request_id, kind, ZoneCommandStatus::InRoom);
            ++_transition_busy_replies;
            return true;
        }

        if (const auto ret = _routes.roomReturnFor(connection))
        {
            const CommandReleaseToken release{_lifecycle, connection};
            _zone_results.replyStatus(
                connection, ret->player, ret->return_zone, ret->return_epoch, ret->return_position, request_id, kind, ZoneCommandStatus::TransitionInProgress);
            ++_transition_busy_replies;
            return true;
        }

        return false;
    }

    bool RoomEntryService::noteDisconnect(const snf::net::ConnectionId connection) noexcept
    {
        const auto active_entry = _active_entries.find(connection);
        if (active_entry != _active_entries.end())
        {
            active_entry->second.disconnecting = true;
            return true;
        }

        const auto active_return = _active_returns.find(connection);
        if (active_return != _active_returns.end())
        {
            active_return->second.disconnecting = true;
            return true;
        }

        const auto in_room = _routes.inRoomFor(connection);
        if (in_room)
        {
            // Player in room disconnected: post LeaveRoom and abandon
            static_cast<void>(_commands.tryPost(RoutedCommand{
                .connection = connection,
                .route =
                    RoomCommandRoute{
                        .room = in_room->room,
                        .command = LeaveRoom{.player = in_room->player},
                        .reply_kind = std::nullopt,
                        .request_id = 0,
                    },
            }));
            _routes.abandon(connection);
            _sessions.noteLocation(connection, std::nullopt);
            return false;
        }

        return false;
    }

    void RoomEntryService::startReturn(const snf::net::ConnectionId connection, const RoomId room)
    {
        const auto ret = _routes.tryBeginRoomReturn(connection, room);
        if (!ret)
        {
            return;
        }

        const auto ticket = _room_transitions.tryReserve(ret->id);
        if (!ticket)
        {
            static_cast<void>(_routes.abandonRoomReturn(connection, ret->id));
            _sessions.noteLocation(connection, std::nullopt);
            _outbound.reportAdmissionFailure(connection);
            return;
        }

        _active_returns.insert_or_assign(
            connection,
            ActiveReturn{
                .connection = connection,
                .ticket = *ticket,
                .return_id = ret->id,
                .player = ret->player,
                .room = ret->room,
                .return_zone = ret->return_zone,
                .return_epoch = ret->return_epoch,
                .return_position = ret->return_position,
                .disconnecting = false,
                .started_at = std::chrono::steady_clock::now(),
            }
        );

        const PostResult post_res = _commands.tryPost(RoutedCommand{
            .connection = connection,
            .route =
                ZoneHandoffCommandRoute{
                    .command =
                        ZoneInboundCommand{
                            .zone = ret->return_zone,
                            .command =
                                EnterZoneCommand{
                                    .player = ret->player,
                                    .route_epoch = ret->return_epoch,
                                    .position = ret->return_position,
                                },
                            .reply = std::nullopt,
                            .handoff = std::nullopt,
                            .room_entry =
                                RoomEntryContext{
                                    .entry_id = {},
                                    .return_id = ret->id,
                                    .ticket = *ticket,
                                    .connection = connection,
                                    .player = ret->player,
                                    .step = RoomEntryStep::ReturnZone,
                                },
                        },
                },
        });

        if (post_res != PostResult::Accepted)
        {
            static_cast<void>(_routes.abandonRoomReturn(connection, ret->id));
            _room_transitions.release(*ticket);
            _active_returns.erase(connection);
            _sessions.noteLocation(connection, std::nullopt);
            _outbound.reportAdmissionFailure(connection);
            ++_return_failures;
        }
    }

    void RoomEntryService::drain()
    {
        for (std::size_t index = 0; index < _max_completions_per_turn; ++index)
        {
            auto completion = _room_transitions.tryPop();
            if (!completion)
            {
                break;
            }
            handleCompletion(std::move(*completion));
        }

        // Clears arrive as facts published by a Worker, and the return they ask for is
        // started here, on the thread that owns the route state. Same budget as the
        // completions above: whatever is left over waits for the next wake-up.
        for (std::size_t index = 0; index < _max_completions_per_turn; ++index)
        {
            const auto request = _room_transitions.tryPopReturnRequest();
            if (!request)
            {
                break;
            }
            const auto connection = _sessions.connectionFor(request->player);
            if (!connection)
            {
                // No live session left. The disconnect path already released the Room
                // presence, so there is nothing to put back.
                continue;
            }
            startReturn(*connection, request->room);
        }
        _room_transitions.wakeIfPending();
    }

    void RoomEntryService::handleCompletion(RoomTransitionCompletion completion)
    {
        // The step picks the saga, and the map only confirms it. A connection can hold
        // an entry and a return at once -- a battle can clear while an entry is stuck
        // disconnecting -- so choosing by map first would hand a return's completion to
        // the entry that happens to still be there.
        if (completion.step == RoomEntryStep::ReturnZone)
        {
            if (const auto return_it = _active_returns.find(completion.connection); return_it != _active_returns.end())
            {
                handleReturnZoneCompletion(completion, return_it->second);
                return;
            }
            ++_stale_completions;
            return;
        }

        if (const auto entry_it = _active_entries.find(completion.connection); entry_it != _active_entries.end())
        {
            if (completion.step == RoomEntryStep::JoinRoom)
            {
                handleJoinRoomCompletion(completion, entry_it->second);
                return;
            }
            if (completion.step == RoomEntryStep::LeaveSource)
            {
                handleLeaveSourceCompletion(completion, entry_it->second);
                return;
            }
        }

        ++_stale_completions;
    }

    void RoomEntryService::handleJoinRoomCompletion(const RoomTransitionCompletion& completion, ActiveEntry& active)
    {
        if (completion.room_status != RoomCommandStatus::Applied)
        {
            // Room rejected the join (e.g. RoomFull, WrongPhase, AlreadyJoined)
            static_cast<void>(_routes.rollbackRoomEntryBeforeLeave(active.connection, active.entry_id));
            _room_transitions.release(active.ticket);
            replyRoomJoined(active.connection, active.request_id, active.room, completion.room_status, RoomPhase::Waiting);
            _transition_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - active.started_at));
            _active_entries.erase(active.connection);
            return;
        }

        // Room accepted! Next step: Leave source zone.
        static_cast<void>(_routes.noteRoomJoined(active.connection, active.entry_id));
        if (active.disconnecting)
        {
            // Disconnected before leave source: compensate by leaving room
            static_cast<void>(_commands.tryPost(RoutedCommand{
                .connection = active.connection,
                .route =
                    RoomCommandRoute{
                        .room = active.room,
                        .command = LeaveRoom{.player = active.player},
                        .reply_kind = std::nullopt,
                        .request_id = 0,
                    },
            }));
            static_cast<void>(_routes.rollbackRoomEntryBeforeLeave(active.connection, active.entry_id));
            _room_transitions.release(active.ticket);
            _active_entries.erase(active.connection);
            ++_disconnect_cleanups;
            return;
        }

        const PostResult post_res = _commands.tryPost(RoutedCommand{
            .connection = active.connection,
            .route =
                ZoneHandoffCommandRoute{
                    .command =
                        ZoneInboundCommand{
                            .zone = active.source_zone,
                            .command =
                                LeaveZoneCommand{
                                    .player = active.player,
                                    .route_epoch = active.source_epoch,
                                },
                            .reply = std::nullopt,
                            .handoff = std::nullopt,
                            .room_entry =
                                RoomEntryContext{
                                    .entry_id = active.entry_id,
                                    .return_id = {},
                                    .ticket = active.ticket,
                                    .connection = active.connection,
                                    .player = active.player,
                                    .step = RoomEntryStep::LeaveSource,
                                },
                        },
                },
        });

        if (post_res != PostResult::Accepted)
        {
            compensateFailedSourceLeave(active);
        }
    }

    void RoomEntryService::handleLeaveSourceCompletion(const RoomTransitionCompletion& completion, ActiveEntry& active)
    {
        if (completion.zone_status != ZoneCommandStatus::Applied || !completion.position)
        {
            compensateFailedSourceLeave(active);
            return;
        }

        const auto in_room = _routes.completeRoomEntry(active.connection, active.entry_id, *completion.position);
        if (!in_room)
        {
            compensateFailedSourceLeave(active);
            return;
        }

        _room_transitions.release(active.ticket);
        _transition_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - active.started_at));

        if (active.disconnecting)
        {
            static_cast<void>(_commands.tryPost(RoutedCommand{
                .connection = active.connection,
                .route =
                    RoomCommandRoute{
                        .room = active.room,
                        .command = LeaveRoom{.player = active.player},
                        .reply_kind = std::nullopt,
                        .request_id = 0,
                    },
            }));
            _routes.abandon(active.connection);
            _sessions.noteLocation(active.connection, std::nullopt);
            _active_entries.erase(active.connection);
            ++_disconnect_cleanups;
            return;
        }

        replyRoomJoined(active.connection, active.request_id, active.room, RoomCommandStatus::Applied, RoomPhase::Waiting);
        _active_entries.erase(active.connection);
    }

    void RoomEntryService::compensateFailedSourceLeave(ActiveEntry& active)
    {
        static_cast<void>(_commands.tryPost(RoutedCommand{
            .connection = active.connection,
            .route =
                RoomCommandRoute{
                    .room = active.room,
                    .command = LeaveRoom{.player = active.player},
                    .reply_kind = std::nullopt,
                    .request_id = 0,
                },
        }));

        static_cast<void>(_routes.rollbackRoomEntryBeforeLeave(active.connection, active.entry_id));
        _room_transitions.release(active.ticket);
        replyRoomJoined(active.connection, active.request_id, active.room, RoomCommandStatus::EntryFailed, RoomPhase::Waiting);
        _transition_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - active.started_at));
        ++_compensated;
        ++_source_leave_failures;
        _active_entries.erase(active.connection);
    }

    void RoomEntryService::handleReturnZoneCompletion(const RoomTransitionCompletion& completion, ActiveReturn& active_return)
    {
        if (completion.zone_status != ZoneCommandStatus::Applied && completion.zone_status != ZoneCommandStatus::AlreadyPresent)
        {
            static_cast<void>(_routes.abandonRoomReturn(active_return.connection, active_return.return_id));
            _room_transitions.release(active_return.ticket);
            _sessions.noteLocation(active_return.connection, std::nullopt);
            _outbound.reportAdmissionFailure(active_return.connection);
            ++_return_failures;
            _active_returns.erase(active_return.connection);
            return;
        }

        const auto restored = _routes.completeRoomReturn(active_return.connection, active_return.return_id);
        if (!restored)
        {
            _room_transitions.release(active_return.ticket);
            _sessions.noteLocation(active_return.connection, std::nullopt);
            _outbound.reportAdmissionFailure(active_return.connection);
            ++_return_failures;
            _active_returns.erase(active_return.connection);
            return;
        }

        const ZonePosition pos = completion.position.value_or(active_return.return_position);
        _sessions.noteLocation(active_return.connection, PlayerLocation{.zone = active_return.return_zone, .position = pos});
        _room_transitions.release(active_return.ticket);

        if (active_return.disconnecting)
        {
            _sessions.noteLocation(active_return.connection, std::nullopt);
            _routes.abandon(active_return.connection);
            ++_disconnect_cleanups;
        }
        else
        {
            replyReturnedToZone(active_return.connection, active_return.return_zone, pos);
        }

        _active_returns.erase(active_return.connection);
    }

    void RoomEntryService::failEntryBeforeSourceLeave(const snf::net::ConnectionId connection, const RoomId room, const std::uint32_t request_id, const RoomCommandStatus status)
    {
        const auto active = _active_entries.find(connection);
        if (active == _active_entries.end())
        {
            return;
        }

        const RoomTransitionTicket ticket = active->second.ticket;
        const auto started_at = active->second.started_at;
        static_cast<void>(_routes.rollbackRoomEntryBeforeLeave(connection, active->second.entry_id));
        _room_transitions.release(ticket);
        replyRoomJoined(connection, request_id, room, status, RoomPhase::Waiting);
        _transition_nanoseconds.record(std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started_at));
        ++_failures_before_source_leave;
        _active_entries.erase(active);
    }

    void RoomEntryService::replyRoomJoined(const snf::net::ConnectionId connection,
                                           const std::uint32_t request_id,
                                           const RoomId room,
                                           const RoomCommandStatus status,
                                           const RoomPhase phase)
    {
        std::vector<std::byte> payload;
        payload.reserve(1 + 1 + 8);
        payload.push_back(static_cast<std::byte>(static_cast<std::uint8_t>(status)));
        payload.push_back(static_cast<std::byte>(static_cast<std::uint8_t>(phase)));
        append_u64(payload, room.value);

        static_cast<void>(sendFrame(connection,
                                    snf::protocol::Frame{
                                        .type = snf::protocol::MessageType::RoomJoined,
                                        .request_id = request_id,
                                        .payload = std::move(payload),
                                    }));
    }

    void RoomEntryService::replyReturnedToZone(const snf::net::ConnectionId connection, const ZoneId zone, const ZonePosition position)
    {
        std::vector<std::byte> payload;
        payload.reserve(8 + 4 + 4);
        append_u64(payload, zone.value);
        append_u32(payload, static_cast<std::uint32_t>(position.x));
        append_u32(payload, static_cast<std::uint32_t>(position.y));

        static_cast<void>(sendFrame(connection,
                                    snf::protocol::Frame{
                                        .type = snf::protocol::MessageType::ReturnedToZone,
                                        .request_id = snf::protocol::UNSOLICITED_REQUEST_ID,
                                        .payload = std::move(payload),
                                    }));
    }

    bool RoomEntryService::sendFrame(const snf::net::ConnectionId connection, snf::protocol::Frame frame)
    {
        auto reservation = _outbound.tryReserve(connection, 1);
        if (!reservation)
        {
            _outbound.reportAdmissionFailure(connection);
            return false;
        }
        if (!_outbound.commit(*reservation,
                              SendFrame{
                                  .connection = connection,
                                  .frame = std::move(frame),
                              }))
        {
            _outbound.reportAdmissionFailure(connection);
            return false;
        }
        return true;
    }

    void RoomEntryService::close() noexcept
    {
        _admission_closed = true;
    }

    void RoomEntryService::cancel() noexcept
    {
        _admission_closed = true;
        _room_transitions.cancel();
        _shutdown_cancels += _active_entries.size() + _active_returns.size();
        for (const auto& [connection, active] : _active_entries)
        {
            static_cast<void>(active);
            _sessions.noteLocation(connection, std::nullopt);
            _routes.abandon(connection);
        }
        _active_entries.clear();
        for (const auto& [connection, ret] : _active_returns)
        {
            static_cast<void>(ret);
            _sessions.noteLocation(connection, std::nullopt);
            _routes.abandon(connection);
        }
        _active_returns.clear();
    }

    bool RoomEntryService::drained() const noexcept
    {
        return _active_entries.empty() && _active_returns.empty() && _room_transitions.drained();
    }

    RoomEntryStats RoomEntryService::stats() const noexcept
    {
        return RoomEntryStats{
            .transition_nanoseconds = _transition_nanoseconds.snapshot(),
            .failures_before_source_leave = _failures_before_source_leave,
            .source_leave_failures = _source_leave_failures,
            .return_failures = _return_failures,
            .compensated = _compensated,
            .transition_busy_replies = _transition_busy_replies,
            .stale_completions = _stale_completions,
            .disconnect_cleanups = _disconnect_cleanups,
            .shutdown_cancels = _shutdown_cancels,
            .pending = _active_entries.size() + _active_returns.size(),
        };
    }
}
