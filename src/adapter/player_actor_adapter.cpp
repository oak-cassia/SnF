#include "snf/adapter/player_actor_adapter.hpp"

#include "snf/adapter/game_payloads.hpp"
#include "snf/adapter/to_effects.hpp"
#include "snf/game/street_progression.hpp"

#include <algorithm>

namespace snf::adapter
{
    namespace
    {
        // Everything the transaction needs, copied out before the await. The request
        // owns its data: no pointer or view into the Player survives the suspension.
        [[nodiscard]] snf::worker::SavePlayerRequest toSaveRequest(const snf::server::PlayerRecord& record)
        {
            snf::worker::SavePlayerRequest request{
                .player_id = record.player.value,
                .handled_command_count = record.handled_command_count,
                .has_location = record.last_location.has_value(),
                .zone_id = record.last_location ? record.last_location->zone.value : 0,
                .position_x = record.last_location ? record.last_location->position.x : 0,
                .position_y = record.last_location ? record.last_location->position.y : 0,
                .currency_balance = record.currency_balance,
                .purchased_item_count = record.purchased_item_count,
                .street_experience = record.street_experience,
                .equipped_skill_id = record.skill_loadout.getEquippedSkillId().value,
            };

            const auto owned = record.skill_loadout.getOwnedSkillIds();
            request.owned_skill_ids.reserve(owned.size());
            for (const snf::server::SkillId skill_id : owned)
            {
                request.owned_skill_ids.push_back(skill_id.value);
            }
            return request;
        }

        // The save continuation. It holds the adapter, which the ActorSlot owns for
        // as long as this coroutine can be resumed, and a request that owns its data.
        snf::worker::ActorTask makeSaveTask(
            PlayerActorAdapter& adapter,
            snf::worker::SavePlayerRequest request,
            const snf::server::PlayerStateComponentMask cleared
        )
        {
            auto result = co_await snf::worker::DbAwait{.request = std::move(request)};
            adapter.onSaveCompleted(result, cleared);
            co_return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }
    }

    // One outstanding save timer at a time. The actor is Suspended for the whole
    // await, so this is what keeps a second save from ever being queued behind it.
    void PlayerActorAdapter::scheduleSaveIfDirty(snf::worker::EffectBatch& effects, const std::chrono::steady_clock::time_point now)
    {
        if (_save_scheduled || !_player.hasFlushableDirtyState() || !_player.state().identity().has_value())
        {
            return;
        }

        _save_scheduled = true;
        effects.push(snf::worker::ScheduleTimerEffect{
            .deadline = now + _save_interval,
            .message = GameActorPayloadRegistry::create(PlayerSaveMessage{.player = *_player.state().identity()}),
            .reservation = std::nullopt,
        });
    }

    // The player -> connection half of the session identity. The sink owns the
    // other half and cannot answer this question: the connection it is looking at
    // may not be the one this player is already bound to, and that connection can
    // belong to a different Worker. This is the legacy PlayerConflict outcome,
    // decided where the binding actually lives.
    std::optional<snf::worker::EffectBatch> PlayerActorAdapter::rejectConflictingAuthentication(
        const std::optional<snf::worker::ConnectionRef>& connection
    )
    {
        if (!connection.has_value() || !_bound_connection.has_value() || *_bound_connection == *connection)
        {
            return std::nullopt;
        }

        ++_authentication_conflicts;
        snf::worker::EffectBatch effects;
        effects.push(snf::worker::CloseConnectionEffect{
            .connection = *connection,
            .reason = snf::worker::CloseReason::Application,
            .graceful = true,
        });
        return effects;
    }

    void PlayerActorAdapter::onSaveCompleted(const snf::worker::DbResult& result, const snf::server::PlayerStateComponentMask cleared)
    {
        _save_scheduled = false;

        const auto* saved = std::get_if<snf::worker::SavePlayerResult>(&result);
        if (saved == nullptr)
        {
            // Admission or connection failure before anything was applied.
            _player.restoreDirtyComponents(cleared);
            return;
        }

        switch (saved->outcome)
        {
        case snf::worker::SaveOutcome::Committed:
            ++_committed_saves;
            return;

        case snf::worker::SaveOutcome::FailedBeforeCommit:
            // The transaction is known not to have applied, so the components go
            // back on the dirty list and a later save picks them up.
            _player.restoreDirtyComponents(cleared);
            return;

        case snf::worker::SaveOutcome::CommitOutcomeUnknown:
            // Deliberately not restored. Re-marking them dirty is what would drive
            // an automatic retry of a mutation that may already be applied; the
            // decision belongs to a higher policy with an idempotency key.
            ++_unknown_commits;
            return;
        }
    }

    snf::worker::TurnResult PlayerActorAdapter::dispatch(snf::worker::ActorEnvelope&& envelope, const snf::worker::ActorTurnContext& context)
    {
        if (envelope.is<PlayerSaveMessage>())
        {
            static_cast<void>(envelope.take<PlayerSaveMessage>());
            _save_scheduled = false;

            snf::server::PlayerStateComponentMask cleared{0};
            auto record = _player.takeDirtySnapshot(&cleared);
            if (!record.has_value())
            {
                // Nothing to persist any more.
                return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
            }

            _save_scheduled = true;
            snf::worker::ActorTask task = makeSaveTask(*this, toSaveRequest(*record), cleared);
            const auto status = task.resume();
            if (status == snf::worker::ActorTaskStatus::Suspended)
            {
                return snf::worker::SuspendedTurn{std::move(task)};
            }
            return task.takeCompleted();
        }

        if (envelope.is<PlayerCommandMessage>())
        {
            auto msg = envelope.take<PlayerCommandMessage>();
            if (isClosingConnection(msg.connection))
            {
                return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
            }
            if (std::holds_alternative<snf::server::AuthenticateCommand>(msg.command))
            {
                if (auto conflict = rejectConflictingAuthentication(msg.connection))
                {
                    return snf::worker::CompletedTurn{.effects = std::move(*conflict)};
                }
                _bound_connection = msg.connection;
                _connection_closing = false;
            }
            const auto result = _player.handle(msg.command);
            const PlayerTurnContext turn_ctx{
                .connection = msg.connection,
                .request_id = msg.request_id,
                .player_id = _player.state().identity(),
                .now = context.now,
            };
            auto effects = toEffects(turn_ctx, result);
            scheduleSaveIfDirty(effects, context.now);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<PlayerConnectionClosedMessage>())
        {
            auto msg = envelope.take<PlayerConnectionClosedMessage>();
            // Generation-checked: a close notice for a previous incarnation of the
            // slot must not unbind the connection currently authenticated.
            snf::worker::EffectBatch effects;
            if (_bound_connection.has_value() && *_bound_connection == msg.connection)
            {
                _connection_closing = false;
                _bound_connection.reset();
                _pending_zone_ops.clear();
                if (currentZone().has_value())
                {
                    const auto leaving_zone = *currentZone();
                    _workflow_state = StableRoute{.zone = std::nullopt};
                    if (_player.state().identity().has_value())
                    {
                        effects.push(snf::worker::TellActorEffect{
                            .target =
                                snf::worker::ActorKey{
                                    .kind = snf::worker::ActorKind::Zone,
                                    .entity = leaving_zone.value,
                                },
                            .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                                .connection = std::nullopt,
                                .request_id = 0,
                                .command =
                                    snf::server::LeaveZoneCommand{
                                        .player = *_player.state().identity(),
                                        .route_epoch = _route_epoch,
                                    },
                                .reply_to = std::nullopt,
                            }),
                        });
                    }
                }
                else if (std::holds_alternative<InRoomRoute>(_workflow_state))
                {
                    const auto& in_room = std::get<InRoomRoute>(_workflow_state);
                    const auto room = in_room.room;
                    _workflow_state = StableRoute{.zone = std::nullopt};
                    if (_player.state().identity().has_value())
                    {
                        effects.push(snf::worker::TellActorEffect{
                            .target =
                                snf::worker::ActorKey{
                                    .kind = snf::worker::ActorKind::Room,
                                    .entity = room.value,
                                },
                            .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                                .connection = std::nullopt,
                                .request_id = 0,
                                .command =
                                    snf::server::LeaveRoom{
                                        .player = *_player.state().identity(),
                                    },
                                .reply_to = std::nullopt,
                            }),
                        });
                    }
                }
                else if (std::holds_alternative<EnteringRoute>(_workflow_state))
                {
                    const auto& entering = std::get<EnteringRoute>(_workflow_state);
                    if (entering.step == WorkflowStep::RoomJoinStep2_LeaveZone)
                    {
                        const auto room = entering.target_room;
                        if (_player.state().identity().has_value())
                        {
                            effects.push(snf::worker::TellActorEffect{
                                .target =
                                    snf::worker::ActorKey{
                                        .kind = snf::worker::ActorKind::Room,
                                        .entity = room.value,
                                    },
                                .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                                    .connection = std::nullopt,
                                    .request_id = 0,
                                    .command =
                                        snf::server::LeaveRoom{
                                            .player = *_player.state().identity(),
                                        },
                                    .reply_to = std::nullopt,
                                }),
                            });
                        }
                    }
                    _workflow_state = StableRoute{.zone = std::nullopt};
                }
                else if (std::holds_alternative<TransferringRoute>(_workflow_state))
                {
                    const auto transfer = std::get<TransferringRoute>(_workflow_state);
                    failCrossZoneKnownNone(effects, transfer, false);
                }
                else
                {
                    _workflow_state = StableRoute{.zone = std::nullopt};
                }
                scheduleSaveIfDirty(effects, context.now);
            }
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<PlayerZoneRequestMessage>())
        {
            auto msg = envelope.take<PlayerZoneRequestMessage>();
            return handleZoneRequest(std::move(msg), context);
        }

        if (envelope.is<PlayerRoomRequestMessage>())
        {
            auto msg = envelope.take<PlayerRoomRequestMessage>();
            return handleRoomRequest(std::move(msg), context);
        }

        if (envelope.is<ZoneOutcomeMessage>())
        {
            auto msg = envelope.take<ZoneOutcomeMessage>();
            return handleZoneOutcome(std::move(msg), context);
        }

        if (envelope.is<RoomOutcomeMessage>())
        {
            auto msg = envelope.take<RoomOutcomeMessage>();
            return handleRoomOutcome(std::move(msg), context);
        }

        if (envelope.is<PlayerWorkflowTimeoutMessage>())
        {
            auto msg = envelope.take<PlayerWorkflowTimeoutMessage>();
            return handleWorkflowTimeout(std::move(msg), context);
        }

        if (envelope.is<PingMessage>())
        {
            auto msg = envelope.take<PingMessage>();
            if (isClosingConnection(msg.connection))
            {
                return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
            }
            const auto result = _player.handle(snf::server::PingCommand{
                .payload = std::move(msg.payload),
            });
            const PlayerTurnContext turn_ctx{
                .connection = msg.connection,
                .request_id = msg.request_id,
                .player_id = _player.state().identity(),
                .now = context.now,
            };
            auto effects = toEffects(turn_ctx, result);
            scheduleSaveIfDirty(effects, context.now);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<ExperienceGrantMessage>())
        {
            auto msg = envelope.take<ExperienceGrantMessage>();
            _player.grantStreetExperience(msg.grant.experience);
            snf::worker::EffectBatch effects;
            scheduleSaveIfDirty(effects, context.now);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
    }

    constexpr auto WORKFLOW_TIMEOUT = std::chrono::milliseconds{1000};

    bool PlayerActorAdapter::isClosingConnection(const snf::worker::ConnectionRef& connection) const noexcept
    {
        return _connection_closing && _bound_connection.has_value() && *_bound_connection == connection;
    }

    bool PlayerActorAdapter::isClosingConnection(const std::optional<snf::worker::ConnectionRef>& connection) const noexcept
    {
        return connection.has_value() && isClosingConnection(*connection);
    }

    void PlayerActorAdapter::appendCrossZoneCleanup(snf::worker::EffectBatch& effects, const TransferringRoute& transfer)
    {
        const auto player_id = _player.state().identity();
        if (!player_id.has_value())
        {
            return;
        }

        const auto append_leave = [&effects, player = *player_id](const snf::server::ZoneId zone, const std::uint64_t epoch)
        {
            effects.push(snf::worker::TellActorEffect{
                .target = snf::worker::ActorKey{snf::worker::ActorKind::Zone, zone.value},
                .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                    .connection = std::nullopt,
                    .request_id = 0,
                    .command = snf::server::LeaveZoneCommand{.player = player, .route_epoch = epoch},
                    .reply_to = std::nullopt,
                }),
            });
        };

        const auto source_cleanup_epoch =
            transfer.step == WorkflowStep::CrossZoneRestoreSource && transfer.restore_epoch != 0 ? transfer.restore_epoch : transfer.source_epoch;
        append_leave(transfer.source_zone, source_cleanup_epoch);
        append_leave(transfer.target_zone, transfer.target_epoch);
    }

    void PlayerActorAdapter::failCrossZoneKnownNone(snf::worker::EffectBatch& effects, const TransferringRoute& transfer, const bool close_connection)
    {
        appendCrossZoneCleanup(effects, transfer);
        _pending_zone_ops.clear();
        _player.setLastLocation(std::nullopt);
        _workflow_state = StableRoute{.zone = std::nullopt};

        if (close_connection && _bound_connection.has_value())
        {
            _connection_closing = true;
            effects.push(snf::worker::CloseConnectionEffect{
                .connection = *_bound_connection,
                .reason = snf::worker::CloseReason::Application,
                .graceful = true,
            });
        }
    }

    snf::worker::TurnResult PlayerActorAdapter::handleZoneRequest(PlayerZoneRequestMessage&& msg, const snf::worker::ActorTurnContext& context)
    {
        if (isClosingConnection(msg.connection))
        {
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        const auto player_id = _player.state().identity();
        if (!player_id.has_value() || !_bound_connection.has_value() || *_bound_connection != msg.connection)
        {
            snf::worker::EffectBatch effects;
            effects.push(snf::worker::CloseConnectionEffect{
                .connection = msg.connection,
                .reason = snf::worker::CloseReason::Application,
                .graceful = true,
            });
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        return std::visit(
            [this, &msg, player = *player_id, &context](auto&& req) -> snf::worker::TurnResult
            {
                using T = std::decay_t<decltype(req)>;
                if constexpr (std::is_same_v<T, EnterZoneRequest>)
                {
                    if (req.zone.value == 0)
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (isInRoom())
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::InRoom,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, req.zone, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (std::holds_alternative<EnteringRoute>(_workflow_state) || std::holds_alternative<ReturningRoute>(_workflow_state) ||
                        std::holds_alternative<TransferringRoute>(_workflow_state))
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::TransitionInProgress,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, req.zone, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto active_zone = currentZone();
                    if (active_zone.has_value() && *active_zone != req.zone)
                    {
                        const bool has_boundary_op = std::any_of(
                            _pending_zone_ops.begin(),
                            _pending_zone_ops.end(),
                            [](const auto& op)
                            {
                                return op.step != WorkflowStep::ZoneMove;
                            }
                        );
                        if (has_boundary_op)
                        {
                            const snf::server::ZoneResult result{
                                .status = snf::server::ZoneCommandStatus::TransitionInProgress,
                                .player = player,
                                .position = std::nullopt,
                                .route_epoch = _route_epoch,
                                .tick = 0,
                                .visible_players = {},
                            };
                            snf::worker::EffectBatch effects;
                            effects.push(snf::worker::SendFrameEffect{
                                .connection = msg.connection,
                                .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, req.zone, result, msg.request_id),
                                .critical = false,
                            });
                            return snf::worker::CompletedTurn{.effects = std::move(effects)};
                        }

                        const auto correlation = ++_correlation_sequence;
                        auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                            .correlation_id = correlation,
                            .step = WorkflowStep::CrossZoneLeaveSource,
                        });
                        std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                        if (_timer_admission != nullptr)
                        {
                            reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                            if (!reservation.has_value())
                            {
                                const snf::server::ZoneResult result{
                                    .status = snf::server::ZoneCommandStatus::TransferFailed,
                                    .player = player,
                                    .position = std::nullopt,
                                    .route_epoch = _route_epoch,
                                    .tick = 0,
                                    .visible_players = {},
                                };
                                snf::worker::EffectBatch effects;
                                effects.push(snf::worker::SendFrameEffect{
                                    .connection = msg.connection,
                                    .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, *active_zone, result, msg.request_id),
                                    .critical = false,
                                });
                                return snf::worker::CompletedTurn{.effects = std::move(effects)};
                            }
                        }

                        const auto source_epoch = _route_epoch;
                        const auto target_epoch = _route_epoch + 1;
                        const auto source_position =
                            _player.state().lastLocation().has_value() && _player.state().lastLocation()->zone == *active_zone
                                ? _player.state().lastLocation()->position
                                : snf::server::ZonePosition{0, 0};
                        _route_epoch = target_epoch;
                        _workflow_state = TransferringRoute{
                            .source_zone = *active_zone,
                            .source_epoch = source_epoch,
                            .source_position = source_position,
                            .target_zone = req.zone,
                            .target_epoch = target_epoch,
                            .target_position = req.position,
                            .restore_epoch = 0,
                            .request_id = msg.request_id,
                            .correlation_id = correlation,
                            .step = WorkflowStep::CrossZoneLeaveSource,
                        };

                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::TellActorEffect{
                            .target = snf::worker::ActorKey{snf::worker::ActorKind::Zone, active_zone->value},
                            .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                                .connection = std::nullopt,
                                .request_id = msg.request_id,
                                .command =
                                    snf::server::LeaveZoneCommand{
                                        .player = player,
                                        .route_epoch = source_epoch,
                                    },
                                .reply_to =
                                    WorkflowReplyTo{
                                        .player = player,
                                        .connection_generation = msg.connection.generation,
                                        .correlation_id = correlation,
                                        .step = WorkflowStep::CrossZoneLeaveSource,
                                        .route_epoch = source_epoch,
                                        .request_id = msg.request_id,
                                    },
                            }),
                        });
                        if (reservation.has_value())
                        {
                            effects.push(snf::worker::ScheduleTimerEffect{
                                .deadline = context.now + WORKFLOW_TIMEOUT,
                                .message = std::move(timeout_msg),
                                .reservation = std::move(*reservation),
                            });
                        }
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto next_epoch = (active_zone == req.zone) ? _route_epoch : (_route_epoch + 1);

                    // Location R1: reconnect to the same zone restores the authoritative saved position.
                    auto entry_position = req.position;
                    if (_player.state().lastLocation().has_value() && _player.state().lastLocation()->zone == req.zone)
                    {
                        entry_position = _player.state().lastLocation()->position;
                    }

                    if (!_pending_zone_ops.empty())
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::TransferFailed,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, req.zone, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto correlation = ++_correlation_sequence;
                    auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                        .correlation_id = correlation,
                        .step = WorkflowStep::ZoneEnter,
                    });
                    std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                    if (_timer_admission != nullptr)
                    {
                        reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                        if (!reservation.has_value())
                        {
                            const snf::server::ZoneResult result{
                                .status = snf::server::ZoneCommandStatus::TransferFailed,
                                .player = player,
                                .position = std::nullopt,
                                .route_epoch = _route_epoch,
                                .tick = 0,
                                .visible_players = {},
                            };
                            snf::worker::EffectBatch effects;
                            effects.push(snf::worker::SendFrameEffect{
                                .connection = msg.connection,
                                .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, req.zone, result, msg.request_id),
                                .critical = false,
                            });
                            return snf::worker::CompletedTurn{.effects = std::move(effects)};
                        }
                    }

                    _pending_zone_ops.push_back(PendingZoneOperation{
                        .correlation_id = correlation,
                        .step = WorkflowStep::ZoneEnter,
                        .target_zone = req.zone,
                        .target_epoch = next_epoch,
                        .request_id = msg.request_id,
                    });

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target =
                            snf::worker::ActorKey{
                                .kind = snf::worker::ActorKind::Zone,
                                .entity = req.zone.value,
                            },
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = std::nullopt,
                            .request_id = msg.request_id,
                            .command =
                                snf::server::EnterZoneCommand{
                                    .player = player,
                                    .route_epoch = next_epoch,
                                    .position = entry_position,
                                },
                            .reply_to =
                                WorkflowReplyTo{
                                    .player = player,
                                    .connection_generation = _bound_connection->generation,
                                    .correlation_id = correlation,
                                    .step = WorkflowStep::ZoneEnter,
                                    .route_epoch = next_epoch,
                                    .request_id = msg.request_id,
                                },
                        }),
                    });

                    if (reservation.has_value())
                    {
                        effects.push(snf::worker::ScheduleTimerEffect{
                            .deadline = context.now + WORKFLOW_TIMEOUT,
                            .message = std::move(timeout_msg),
                            .reservation = std::move(*reservation),
                        });
                    }

                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
                else if constexpr (std::is_same_v<T, MoveRequest>)
                {
                    if (isInRoom())
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::InRoom,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(ZoneReplyFrameKind::Moved, currentZone().value_or(snf::server::ZoneId{0}), result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (std::holds_alternative<EnteringRoute>(_workflow_state) || std::holds_alternative<ReturningRoute>(_workflow_state) ||
                        std::holds_alternative<TransferringRoute>(_workflow_state))
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::TransitionInProgress,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(
                                ZoneReplyFrameKind::Moved,
                                std::holds_alternative<TransferringRoute>(_workflow_state) ? std::get<TransferringRoute>(_workflow_state).target_zone
                                                                                           : currentZone().value_or(snf::server::ZoneId{0}),
                                result,
                                msg.request_id
                            ),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto active_zone = currentZone();
                    if (!active_zone.has_value())
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const bool has_boundary_op = std::any_of(
                        _pending_zone_ops.begin(),
                        _pending_zone_ops.end(),
                        [](const auto& op) {
                            return op.step == WorkflowStep::ZoneEnter || op.step == WorkflowStep::ZoneLeave;
                        }
                    );
                    if (has_boundary_op)
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::TransitionInProgress,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(ZoneReplyFrameKind::Moved, currentZone().value_or(snf::server::ZoneId{0}), result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (_pending_zone_ops.size() >= MAX_PENDING_ZONE_OPS)
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::TransferFailed,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(ZoneReplyFrameKind::Moved, *active_zone, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto correlation = ++_correlation_sequence;
                    auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                        .correlation_id = correlation,
                        .step = WorkflowStep::ZoneMove,
                    });
                    std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                    if (_timer_admission != nullptr)
                    {
                        reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                        if (!reservation.has_value())
                        {
                            const snf::server::ZoneResult result{
                                .status = snf::server::ZoneCommandStatus::TransferFailed,
                                .player = player,
                                .position = std::nullopt,
                                .route_epoch = _route_epoch,
                                .tick = 0,
                                .visible_players = {},
                            };
                            snf::worker::EffectBatch effects;
                            effects.push(snf::worker::SendFrameEffect{
                                .connection = msg.connection,
                                .frame = encodeZoneReply(ZoneReplyFrameKind::Moved, *active_zone, result, msg.request_id),
                                .critical = false,
                            });
                            return snf::worker::CompletedTurn{.effects = std::move(effects)};
                        }
                    }

                    _pending_zone_ops.push_back(PendingZoneOperation{
                        .correlation_id = correlation,
                        .step = WorkflowStep::ZoneMove,
                        .target_zone = *active_zone,
                        .target_epoch = _route_epoch,
                        .request_id = msg.request_id,
                    });

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target =
                            snf::worker::ActorKey{
                                .kind = snf::worker::ActorKind::Zone,
                                .entity = active_zone->value,
                            },
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = std::nullopt,
                            .request_id = msg.request_id,
                            .command =
                                snf::server::MoveInZoneCommand{
                                    .player = player,
                                    .route_epoch = _route_epoch,
                                    .position = req.position,
                                },
                            .reply_to =
                                WorkflowReplyTo{
                                    .player = player,
                                    .connection_generation = _bound_connection->generation,
                                    .correlation_id = correlation,
                                    .step = WorkflowStep::ZoneMove,
                                    .route_epoch = _route_epoch,
                                    .request_id = msg.request_id,
                                },
                        }),
                    });

                    if (reservation.has_value())
                    {
                        effects.push(snf::worker::ScheduleTimerEffect{
                            .deadline = context.now + WORKFLOW_TIMEOUT,
                            .message = std::move(timeout_msg),
                            .reservation = std::move(*reservation),
                        });
                    }

                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
                else if constexpr (std::is_same_v<T, LeaveRequest>)
                {
                    if (isInRoom())
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::InRoom,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(ZoneReplyFrameKind::Left, currentZone().value_or(snf::server::ZoneId{0}), result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (std::holds_alternative<EnteringRoute>(_workflow_state) || std::holds_alternative<ReturningRoute>(_workflow_state) ||
                        std::holds_alternative<TransferringRoute>(_workflow_state))
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::TransitionInProgress,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(
                                ZoneReplyFrameKind::Left,
                                std::holds_alternative<TransferringRoute>(_workflow_state) ? std::get<TransferringRoute>(_workflow_state).target_zone
                                                                                           : currentZone().value_or(snf::server::ZoneId{0}),
                                result,
                                msg.request_id
                            ),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto active_zone = currentZone();
                    if (!active_zone.has_value())
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (!_pending_zone_ops.empty())
                    {
                        const snf::server::ZoneResult result{
                            .status = snf::server::ZoneCommandStatus::TransitionInProgress,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = _route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeZoneReply(ZoneReplyFrameKind::Left, *active_zone, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto correlation = ++_correlation_sequence;
                    auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                        .correlation_id = correlation,
                        .step = WorkflowStep::ZoneLeave,
                    });
                    std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                    if (_timer_admission != nullptr)
                    {
                        reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                        if (!reservation.has_value())
                        {
                            const snf::server::ZoneResult result{
                                .status = snf::server::ZoneCommandStatus::TransferFailed,
                                .player = player,
                                .position = std::nullopt,
                                .route_epoch = _route_epoch,
                                .tick = 0,
                                .visible_players = {},
                            };
                            snf::worker::EffectBatch effects;
                            effects.push(snf::worker::SendFrameEffect{
                                .connection = msg.connection,
                                .frame = encodeZoneReply(ZoneReplyFrameKind::Left, *active_zone, result, msg.request_id),
                                .critical = false,
                            });
                            return snf::worker::CompletedTurn{.effects = std::move(effects)};
                        }
                    }

                    _pending_zone_ops.push_back(PendingZoneOperation{
                        .correlation_id = correlation,
                        .step = WorkflowStep::ZoneLeave,
                        .target_zone = *active_zone,
                        .target_epoch = _route_epoch,
                        .request_id = msg.request_id,
                    });

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target =
                            snf::worker::ActorKey{
                                .kind = snf::worker::ActorKind::Zone,
                                .entity = active_zone->value,
                            },
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = std::nullopt,
                            .request_id = msg.request_id,
                            .command =
                                snf::server::LeaveZoneCommand{
                                    .player = player,
                                    .route_epoch = _route_epoch,
                                },
                            .reply_to =
                                WorkflowReplyTo{
                                    .player = player,
                                    .connection_generation = _bound_connection->generation,
                                    .correlation_id = correlation,
                                    .step = WorkflowStep::ZoneLeave,
                                    .route_epoch = _route_epoch,
                                    .request_id = msg.request_id,
                                },
                        }),
                    });

                    if (reservation.has_value())
                    {
                        effects.push(snf::worker::ScheduleTimerEffect{
                            .deadline = context.now + WORKFLOW_TIMEOUT,
                            .message = std::move(timeout_msg),
                            .reservation = std::move(*reservation),
                        });
                    }

                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
            },
            msg.request
        );
    }

    snf::worker::TurnResult PlayerActorAdapter::handleRoomRequest(
        PlayerRoomRequestMessage&& msg,
        const snf::worker::ActorTurnContext& context
    )
    {
        if (isClosingConnection(msg.connection))
        {
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        const auto player_id = _player.state().identity();
        if (!player_id.has_value())
        {
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        if (!_bound_connection.has_value() || *_bound_connection != msg.connection)
        {
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        return std::visit(
            [&](auto&& req) -> snf::worker::TurnResult
            {
                using T = std::decay_t<decltype(req)>;
                if constexpr (std::is_same_v<T, RoomJoinRequest>)
                {
                    if (req.room.value == 0)
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (isInRoom())
                    {
                        const snf::server::RoomResult result{
                            .status = snf::server::RoomCommandStatus::AlreadyJoined,
                            .phase = snf::server::RoomPhase::Waiting,
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeRoomReply(RoomReplyFrameKind::Joined, req.room, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (std::holds_alternative<EnteringRoute>(_workflow_state) ||
                        std::holds_alternative<ReturningRoute>(_workflow_state))
                    {
                        const snf::server::RoomResult result{
                            .status = snf::server::RoomCommandStatus::EntryFailed,
                            .phase = snf::server::RoomPhase::Waiting,
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeRoomReply(RoomReplyFrameKind::Joined, req.room, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (!std::holds_alternative<StableRoute>(_workflow_state))
                    {
                        const snf::server::RoomResult result{
                            .status = snf::server::RoomCommandStatus::EntryFailed,
                            .phase = snf::server::RoomPhase::Waiting,
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeRoomReply(RoomReplyFrameKind::Joined, req.room, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto& stable = std::get<StableRoute>(_workflow_state);
                    if (!stable.zone.has_value() || !_pending_zone_ops.empty())
                    {
                        const snf::server::RoomResult result{
                            .status = snf::server::RoomCommandStatus::EntryFailed,
                            .phase = snf::server::RoomPhase::Waiting,
                        };
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = msg.connection,
                            .frame = encodeRoomReply(RoomReplyFrameKind::Joined, req.room, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto correlation = ++_correlation_sequence;
                    auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                        .correlation_id = correlation,
                        .step = WorkflowStep::RoomJoinStep1_JoinRoom,
                    });
                    std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                    if (_timer_admission != nullptr)
                    {
                        reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                        if (!reservation.has_value())
                        {
                            const snf::server::RoomResult result{
                                .status = snf::server::RoomCommandStatus::RuntimeOverloaded,
                                .phase = snf::server::RoomPhase::Waiting,
                            };
                            snf::worker::EffectBatch effects;
                            effects.push(snf::worker::SendFrameEffect{
                                .connection = msg.connection,
                                .frame = encodeRoomReply(RoomReplyFrameKind::Joined, req.room, result, msg.request_id),
                                .critical = false,
                            });
                            return snf::worker::CompletedTurn{.effects = std::move(effects)};
                        }
                    }

                    const auto source_zone = *stable.zone;
                    const auto source_epoch = _route_epoch;
                    const auto return_pos = _player.state().lastLocation().has_value()
                                                ? _player.state().lastLocation()->position
                                                : snf::server::ZonePosition{0, 0};

                    _workflow_state = EnteringRoute{
                        .target_room = req.room,
                        .source_zone = source_zone,
                        .source_epoch = source_epoch,
                        .return_position = return_pos,
                        .request_id = msg.request_id,
                        .correlation_id = correlation,
                        .step = WorkflowStep::RoomJoinStep1_JoinRoom,
                    };

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target = snf::worker::ActorKey{snf::worker::ActorKind::Room, req.room.value},
                        .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                            .connection = msg.connection,
                            .request_id = msg.request_id,
                            .command =
                                snf::server::JoinRoom{
                                    .player = *player_id,
                                    .stats = snf::server::combatStats(snf::server::streetLevel(_player.state().streetExperience())),
                                    .equipped_skill_id = _player.state().getSkillLoadout().getEquippedSkillId(),
                                },
                            .reply_to =
                                WorkflowReplyTo{
                                    .player = *player_id,
                                    .connection_generation = msg.connection.generation,
                                    .correlation_id = correlation,
                                    .step = WorkflowStep::RoomJoinStep1_JoinRoom,
                                    .route_epoch = 0,
                                    .request_id = msg.request_id,
                                    },
                        }),
                    });

                    if (reservation.has_value())
                    {
                        effects.push(snf::worker::ScheduleTimerEffect{
                            .deadline = context.now + WORKFLOW_TIMEOUT,
                            .message = std::move(timeout_msg),
                            .reservation = std::move(*reservation),
                        });
                    }

                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
                else if constexpr (std::is_same_v<T, BattleStartRequest>)
                {
                    if (!std::holds_alternative<InRoomRoute>(_workflow_state))
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }
                    const auto& in_room = std::get<InRoomRoute>(_workflow_state);
                    if (in_room.room != req.room)
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target = snf::worker::ActorKey{snf::worker::ActorKind::Room, in_room.room.value},
                        .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                            .connection = msg.connection,
                            .request_id = msg.request_id,
                            .command = snf::server::StartBattle{},
                            .reply_to = std::nullopt,
                        }),
                    });
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
                else if constexpr (std::is_same_v<T, UseSkillRequest>)
                {
                    if (!std::holds_alternative<InRoomRoute>(_workflow_state))
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }
                    const auto& in_room = std::get<InRoomRoute>(_workflow_state);
                    if (in_room.room != req.room)
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target = snf::worker::ActorKey{snf::worker::ActorKind::Room, in_room.room.value},
                        .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                            .connection = msg.connection,
                            .request_id = msg.request_id,
                            .command =
                                snf::server::UseSkill{
                                    .player = *player_id,
                                    .skill_id = req.skill_id,
                                    .request_sequence = req.request_sequence,
                                },
                            .reply_to = std::nullopt,
                        }),
                    });
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
                else if constexpr (std::is_same_v<T, SetMoveIntentRequest>)
                {
                    if (!std::holds_alternative<InRoomRoute>(_workflow_state))
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }
                    const auto& in_room = std::get<InRoomRoute>(_workflow_state);
                    if (in_room.room != req.room)
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target = snf::worker::ActorKey{snf::worker::ActorKind::Room, in_room.room.value},
                        .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                            .connection = msg.connection,
                            .request_id = msg.request_id,
                            .command =
                                snf::server::SetMoveIntent{
                                    .player = *player_id,
                                    .direction = req.direction,
                                    .request_sequence = req.request_sequence,
                                },
                            .reply_to = std::nullopt,
                        }),
                    });
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
                else if constexpr (std::is_same_v<T, RoomLeaveRequest>)
                {
                    if (!std::holds_alternative<InRoomRoute>(_workflow_state))
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }
                    const auto in_room = std::get<InRoomRoute>(_workflow_state);

                    auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                        .correlation_id = _correlation_sequence + 1,
                        .step = WorkflowStep::RoomReturnStep1_ZoneEnter,
                    });
                    std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                    if (_timer_admission != nullptr)
                    {
                        reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                        if (!reservation.has_value())
                        {
                            snf::worker::EffectBatch effects;
                            effects.push(snf::worker::CloseConnectionEffect{
                                .connection = msg.connection,
                                .reason = snf::worker::CloseReason::Application,
                                .graceful = true,
                            });
                            return snf::worker::CompletedTurn{.effects = std::move(effects)};
                        }
                    }

                    ++_route_epoch;
                    const auto correlation = ++_correlation_sequence;
                    _workflow_state = ReturningRoute{
                        .source_room = in_room.room,
                        .return_zone = in_room.return_zone,
                        .return_position = in_room.return_position,
                        .return_epoch = _route_epoch,
                        .request_id = msg.request_id,
                        .correlation_id = correlation,
                        .step = WorkflowStep::RoomReturnStep1_ZoneEnter,
                    };

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target = snf::worker::ActorKey{snf::worker::ActorKind::Room, in_room.room.value},
                        .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                            .connection = std::nullopt,
                            .request_id = 0,
                            .command = snf::server::LeaveRoom{.player = *player_id},
                            .reply_to = std::nullopt,
                        }),
                    });

                    effects.push(snf::worker::TellActorEffect{
                        .target = snf::worker::ActorKey{snf::worker::ActorKind::Zone, in_room.return_zone.value},
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = std::nullopt,
                            .request_id = 0,
                            .command =
                                snf::server::EnterZoneCommand{
                                    .player = *player_id,
                                    .route_epoch = _route_epoch,
                                    .position = in_room.return_position,
                                },
                            .reply_to =
                                WorkflowReplyTo{
                                    .player = *player_id,
                                    .connection_generation = msg.connection.generation,
                                    .correlation_id = correlation,
                                    .step = WorkflowStep::RoomReturnStep1_ZoneEnter,
                                    .route_epoch = _route_epoch,
                                    .request_id = 0,
                                },
                        }),
                    });

                    if (reservation.has_value())
                    {
                        effects.push(snf::worker::ScheduleTimerEffect{
                            .deadline = context.now + WORKFLOW_TIMEOUT,
                            .message = std::move(timeout_msg),
                            .reservation = std::move(*reservation),
                        });
                    }

                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
            },
            msg.request
        );
    }

    snf::worker::TurnResult PlayerActorAdapter::handleZoneOutcome(ZoneOutcomeMessage&& msg, const snf::worker::ActorTurnContext& context)
    {
        const auto player_id = _player.state().identity();
        if (!player_id.has_value() || msg.player != *player_id)
        {
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        if (!_bound_connection.has_value() || _bound_connection->generation != msg.connection_generation)
        {
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        const bool is_cross_zone_step = msg.step == WorkflowStep::CrossZoneLeaveSource || msg.step == WorkflowStep::CrossZoneEnterTarget ||
                                        msg.step == WorkflowStep::CrossZoneRestoreSource;
        if (is_cross_zone_step)
        {
            if (!std::holds_alternative<TransferringRoute>(_workflow_state))
            {
                return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
            }

            auto& transfer = std::get<TransferringRoute>(_workflow_state);
            const auto expected_zone = transfer.step == WorkflowStep::CrossZoneEnterTarget ? transfer.target_zone : transfer.source_zone;
            const auto expected_epoch = transfer.step == WorkflowStep::CrossZoneLeaveSource
                                            ? transfer.source_epoch
                                            : (transfer.step == WorkflowStep::CrossZoneEnterTarget ? transfer.target_epoch : transfer.restore_epoch);
            if (msg.correlation_id != transfer.correlation_id || msg.step != transfer.step || msg.zone != expected_zone ||
                msg.route_epoch != expected_epoch || msg.request_id != transfer.request_id ||
                (msg.result.player.has_value() && *msg.result.player != *player_id))
            {
                return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
            }

            snf::worker::EffectBatch effects;
            if (msg.result.status == snf::server::ZoneCommandStatus::StaleRoute)
            {
                // Identity was checked above. Clean the participant observed by
                // the Zone, not the epoch echoed from our rejected command.
                auto failed = transfer;
                if (transfer.step == WorkflowStep::CrossZoneLeaveSource)
                {
                    failed.source_epoch = msg.result.route_epoch;
                }
                else if (transfer.step == WorkflowStep::CrossZoneEnterTarget)
                {
                    failed.target_epoch = msg.result.route_epoch;
                }
                else
                {
                    failed.restore_epoch = msg.result.route_epoch;
                }
                _route_epoch = std::max(_route_epoch, msg.result.route_epoch);
                failCrossZoneKnownNone(effects, failed, true);
                scheduleSaveIfDirty(effects, context.now);
                return snf::worker::CompletedTurn{.effects = std::move(effects)};
            }

            if (transfer.step == WorkflowStep::CrossZoneLeaveSource)
            {
                if (msg.result.status != snf::server::ZoneCommandStatus::Applied || !msg.result.position.has_value())
                {
                    const auto failed = transfer;
                    failCrossZoneKnownNone(effects, failed, true);
                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }

                auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                    .correlation_id = transfer.correlation_id,
                    .step = WorkflowStep::CrossZoneEnterTarget,
                });
                std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                if (_timer_admission != nullptr)
                {
                    reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                    if (!reservation.has_value())
                    {
                        const auto failed = transfer;
                        failCrossZoneKnownNone(effects, failed, true);
                        scheduleSaveIfDirty(effects, context.now);
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }
                }

                transfer.source_position = *msg.result.position;
                transfer.step = WorkflowStep::CrossZoneEnterTarget;
                effects.push(snf::worker::TellActorEffect{
                    .target = snf::worker::ActorKey{snf::worker::ActorKind::Zone, transfer.target_zone.value},
                    .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                        .connection = std::nullopt,
                        .request_id = transfer.request_id,
                        .command =
                            snf::server::EnterZoneCommand{
                                .player = *player_id,
                                .route_epoch = transfer.target_epoch,
                                .position = transfer.target_position,
                            },
                        .reply_to =
                            WorkflowReplyTo{
                                .player = *player_id,
                                .connection_generation = _bound_connection->generation,
                                .correlation_id = transfer.correlation_id,
                                .step = WorkflowStep::CrossZoneEnterTarget,
                                .route_epoch = transfer.target_epoch,
                                .request_id = transfer.request_id,
                            },
                    }),
                });
                if (reservation.has_value())
                {
                    effects.push(snf::worker::ScheduleTimerEffect{
                        .deadline = context.now + WORKFLOW_TIMEOUT,
                        .message = std::move(timeout_msg),
                        .reservation = std::move(*reservation),
                    });
                }
                return snf::worker::CompletedTurn{.effects = std::move(effects)};
            }

            if (transfer.step == WorkflowStep::CrossZoneEnterTarget)
            {
                if ((msg.result.status == snf::server::ZoneCommandStatus::Applied ||
                     msg.result.status == snf::server::ZoneCommandStatus::AlreadyPresent) &&
                    msg.result.position.has_value())
                {
                    const auto target_zone = transfer.target_zone;
                    const auto target_epoch = transfer.target_epoch;
                    const auto request_id = transfer.request_id;
                    const auto final_position = *msg.result.position;
                    _workflow_state = StableRoute{.zone = target_zone};
                    _route_epoch = target_epoch;
                    _player.setLastLocation(snf::server::PlayerLocation{.zone = target_zone, .position = final_position});
                    effects.push(snf::worker::SendFrameEffect{
                        .connection = *_bound_connection,
                        .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, target_zone, msg.result, request_id),
                        .critical = false,
                    });
                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }

                const auto restore_epoch = _route_epoch + 1;
                auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                    .correlation_id = transfer.correlation_id,
                    .step = WorkflowStep::CrossZoneRestoreSource,
                });
                std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                if (_timer_admission != nullptr)
                {
                    reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                    if (!reservation.has_value())
                    {
                        const auto failed = transfer;
                        failCrossZoneKnownNone(effects, failed, true);
                        scheduleSaveIfDirty(effects, context.now);
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }
                }

                transfer.restore_epoch = restore_epoch;
                _route_epoch = restore_epoch;
                transfer.step = WorkflowStep::CrossZoneRestoreSource;
                effects.push(snf::worker::TellActorEffect{
                    .target = snf::worker::ActorKey{snf::worker::ActorKind::Zone, transfer.source_zone.value},
                    .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                        .connection = std::nullopt,
                        .request_id = transfer.request_id,
                        .command =
                            snf::server::EnterZoneCommand{
                                .player = *player_id,
                                .route_epoch = transfer.restore_epoch,
                                .position = transfer.source_position,
                            },
                        .reply_to =
                            WorkflowReplyTo{
                                .player = *player_id,
                                .connection_generation = _bound_connection->generation,
                                .correlation_id = transfer.correlation_id,
                                .step = WorkflowStep::CrossZoneRestoreSource,
                                .route_epoch = transfer.restore_epoch,
                                .request_id = transfer.request_id,
                            },
                    }),
                });
                if (reservation.has_value())
                {
                    effects.push(snf::worker::ScheduleTimerEffect{
                        .deadline = context.now + WORKFLOW_TIMEOUT,
                        .message = std::move(timeout_msg),
                        .reservation = std::move(*reservation),
                    });
                }
                return snf::worker::CompletedTurn{.effects = std::move(effects)};
            }

            if ((msg.result.status == snf::server::ZoneCommandStatus::Applied || msg.result.status == snf::server::ZoneCommandStatus::AlreadyPresent
                ) &&
                msg.result.position.has_value())
            {
                const auto source_zone = transfer.source_zone;
                const auto restore_epoch = transfer.restore_epoch;
                const auto request_id = transfer.request_id;
                const auto final_position = *msg.result.position;
                _workflow_state = StableRoute{.zone = source_zone};
                _route_epoch = restore_epoch;
                _player.setLastLocation(snf::server::PlayerLocation{.zone = source_zone, .position = final_position});
                effects.push(snf::worker::SendFrameEffect{
                    .connection = *_bound_connection,
                    .frame = encodeZoneReply(
                        ZoneReplyFrameKind::Entered,
                        source_zone,
                        snf::server::ZoneResult{
                            .status = snf::server::ZoneCommandStatus::TransferFailed,
                            .player = *player_id,
                            .position = final_position,
                            .route_epoch = restore_epoch,
                            .tick = msg.result.tick,
                            .visible_players = msg.result.visible_players,
                        },
                        request_id
                    ),
                    .critical = false,
                });
                scheduleSaveIfDirty(effects, context.now);
                return snf::worker::CompletedTurn{.effects = std::move(effects)};
            }

            const auto failed = transfer;
            failCrossZoneKnownNone(effects, failed, true);
            scheduleSaveIfDirty(effects, context.now);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (msg.step == WorkflowStep::RoomJoinStep2_LeaveZone)
        {
            if (std::holds_alternative<EnteringRoute>(_workflow_state))
            {
                auto& entering = std::get<EnteringRoute>(_workflow_state);
                if (entering.correlation_id == msg.correlation_id && entering.step == msg.step && entering.source_zone == msg.zone)
                {
                    snf::worker::EffectBatch effects;
                    if (msg.result.status == snf::server::ZoneCommandStatus::Applied)
                    {
                        const auto return_pos = msg.result.position.value_or(entering.return_position);
                        const auto room = entering.target_room;
                        const auto zone = entering.source_zone;
                        const auto request_id = entering.request_id;
                        _workflow_state = InRoomRoute{
                            .room = room,
                            .return_zone = zone,
                            .return_position = return_pos,
                        };
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = *_bound_connection,
                            .frame = encodeRoomReply(
                                RoomReplyFrameKind::Joined,
                                room,
                                snf::server::RoomResult{
                                    .status = snf::server::RoomCommandStatus::Applied,
                                    .phase = snf::server::RoomPhase::Waiting,
                                },
                                request_id
                            ),
                            .critical = false,
                        });
                    }
                    else
                    {
                        const auto room = entering.target_room;
                        const auto zone = entering.source_zone;
                        const auto request_id = entering.request_id;
                        _workflow_state = StableRoute{.zone = zone};
                        effects.push(snf::worker::TellActorEffect{
                            .target = snf::worker::ActorKey{snf::worker::ActorKind::Room, room.value},
                            .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                                .connection = std::nullopt,
                                .request_id = 0,
                                .command = snf::server::LeaveRoom{.player = *player_id},
                                .reply_to = std::nullopt,
                            }),
                        });
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = *_bound_connection,
                            .frame = encodeRoomReply(
                                RoomReplyFrameKind::Joined,
                                room,
                                snf::server::RoomResult{
                                    .status = snf::server::RoomCommandStatus::EntryFailed,
                                    .phase = snf::server::RoomPhase::Waiting,
                                },
                                request_id
                            ),
                            .critical = false,
                        });
                    }
                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
            }
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        if (msg.step == WorkflowStep::RoomReturnStep1_ZoneEnter)
        {
            if (std::holds_alternative<ReturningRoute>(_workflow_state))
            {
                auto& returning = std::get<ReturningRoute>(_workflow_state);
                if (returning.correlation_id == msg.correlation_id && returning.step == msg.step && returning.return_zone == msg.zone)
                {
                    snf::worker::EffectBatch effects;
                    if (msg.result.status == snf::server::ZoneCommandStatus::Applied ||
                        msg.result.status == snf::server::ZoneCommandStatus::AlreadyPresent)
                    {
                        const auto zone = returning.return_zone;
                        const auto epoch = returning.return_epoch;
                        const auto final_pos = msg.result.position.value_or(returning.return_position);
                        _workflow_state = StableRoute{.zone = zone};
                        _route_epoch = epoch;
                        _player.setLastLocation(snf::server::PlayerLocation{.zone = zone, .position = final_pos});

                        effects.push(snf::worker::SendFrameEffect{
                            .connection = *_bound_connection,
                            .frame = encodeReturnedToZone(zone, final_pos, snf::protocol::UNSOLICITED_REQUEST_ID),
                            .critical = false,
                        });
                    }
                    else
                    {
                        _player.setLastLocation(std::nullopt);
                        _workflow_state = StableRoute{.zone = std::nullopt};
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = *_bound_connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                    }
                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
            }
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        auto it = std::find_if(
            _pending_zone_ops.begin(),
            _pending_zone_ops.end(),
            [&msg](const auto& op) {
                return op.correlation_id == msg.correlation_id && op.step == msg.step;
            }
        );
        if (it == _pending_zone_ops.end())
        {
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        const auto op = *it;
        _pending_zone_ops.erase(it);

        snf::worker::EffectBatch effects;
        if (op.step == WorkflowStep::ZoneEnter)
        {
            if (msg.result.status == snf::server::ZoneCommandStatus::Applied)
            {
                _workflow_state = StableRoute{.zone = msg.zone};
                _route_epoch = msg.route_epoch;
                if (msg.result.position.has_value())
                {
                    _player.setLastLocation(snf::server::PlayerLocation{.zone = msg.zone, .position = *msg.result.position});
                }
            }
            else if (msg.result.status == snf::server::ZoneCommandStatus::AlreadyPresent)
            {
                _workflow_state = StableRoute{.zone = msg.zone};
                _route_epoch = msg.route_epoch;
            }
            effects.push(snf::worker::SendFrameEffect{
                .connection = *_bound_connection,
                .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, msg.zone, msg.result, op.request_id),
                .critical = false,
            });
        }
        else if (op.step == WorkflowStep::ZoneMove)
        {
            if (msg.result.status == snf::server::ZoneCommandStatus::Applied && msg.result.position.has_value())
            {
                _player.setLastLocation(snf::server::PlayerLocation{.zone = msg.zone, .position = *msg.result.position});
            }
            effects.push(snf::worker::SendFrameEffect{
                .connection = *_bound_connection,
                .frame = encodeZoneReply(ZoneReplyFrameKind::Moved, msg.zone, msg.result, op.request_id),
                .critical = false,
            });
        }
        else if (op.step == WorkflowStep::ZoneLeave)
        {
            if (msg.result.status == snf::server::ZoneCommandStatus::Applied)
            {
                _workflow_state = StableRoute{.zone = std::nullopt};
                _player.setLastLocation(std::nullopt);
            }
            effects.push(snf::worker::SendFrameEffect{
                .connection = *_bound_connection,
                .frame = encodeZoneReply(ZoneReplyFrameKind::Left, msg.zone, msg.result, op.request_id),
                .critical = false,
            });
        }

        scheduleSaveIfDirty(effects, context.now);
        return snf::worker::CompletedTurn{.effects = std::move(effects)};
    }

    snf::worker::TurnResult PlayerActorAdapter::handleRoomOutcome(RoomOutcomeMessage&& msg, const snf::worker::ActorTurnContext& context)
    {
        const auto player_id = _player.state().identity();
        if (!player_id.has_value() || msg.player != *player_id)
        {
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        if (msg.step == WorkflowStep::RoomTerminalNotification)
        {
            if (std::holds_alternative<InRoomRoute>(_workflow_state))
            {
                const auto in_room = std::get<InRoomRoute>(_workflow_state);
                if (in_room.room == msg.room)
                {
                    auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                        .correlation_id = _correlation_sequence + 1,
                        .step = WorkflowStep::RoomReturnStep1_ZoneEnter,
                    });
                    std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                    if (_timer_admission != nullptr)
                    {
                        reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                        if (!reservation.has_value())
                        {
                            _player.setLastLocation(std::nullopt);
                            _workflow_state = StableRoute{.zone = std::nullopt};
                            snf::worker::EffectBatch effects;
                            if (_bound_connection.has_value())
                            {
                                effects.push(snf::worker::CloseConnectionEffect{
                                    .connection = *_bound_connection,
                                    .reason = snf::worker::CloseReason::Application,
                                    .graceful = true,
                                });
                            }
                            scheduleSaveIfDirty(effects, context.now);
                            return snf::worker::CompletedTurn{.effects = std::move(effects)};
                        }
                    }

                    ++_route_epoch;
                    const auto correlation = ++_correlation_sequence;
                    _workflow_state = ReturningRoute{
                        .source_room = in_room.room,
                        .return_zone = in_room.return_zone,
                        .return_position = in_room.return_position,
                        .return_epoch = _route_epoch,
                        .request_id = 0,
                        .correlation_id = correlation,
                        .step = WorkflowStep::RoomReturnStep1_ZoneEnter,
                    };

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target = snf::worker::ActorKey{snf::worker::ActorKind::Zone, in_room.return_zone.value},
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = std::nullopt,
                            .request_id = 0,
                            .command =
                                snf::server::EnterZoneCommand{
                                    .player = *player_id,
                                    .route_epoch = _route_epoch,
                                    .position = in_room.return_position,
                                },
                            .reply_to =
                                WorkflowReplyTo{
                                    .player = *player_id,
                                    .connection_generation = _bound_connection ? _bound_connection->generation : snf::worker::ConnectionGeneration{},
                                    .correlation_id = correlation,
                                    .step = WorkflowStep::RoomReturnStep1_ZoneEnter,
                                    .route_epoch = _route_epoch,
                                    .request_id = 0,
                                },
                        }),
                    });

                    if (reservation.has_value())
                    {
                        effects.push(snf::worker::ScheduleTimerEffect{
                            .deadline = context.now + WORKFLOW_TIMEOUT,
                            .message = std::move(timeout_msg),
                            .reservation = std::move(*reservation),
                        });
                    }

                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
            }
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        if (!_bound_connection.has_value() || _bound_connection->generation != msg.connection_generation)
        {
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        if (std::holds_alternative<EnteringRoute>(_workflow_state))
        {
            auto& entering = std::get<EnteringRoute>(_workflow_state);
            if (entering.correlation_id == msg.correlation_id && entering.step == msg.step && entering.target_room == msg.room)
            {
                snf::worker::EffectBatch effects;
                if (msg.result.status == snf::server::RoomCommandStatus::Applied ||
                    msg.result.status == snf::server::RoomCommandStatus::AlreadyJoined)
                {
                    auto timeout_msg = GameActorPayloadRegistry::create(PlayerWorkflowTimeoutMessage{
                        .correlation_id = entering.correlation_id,
                        .step = WorkflowStep::RoomJoinStep2_LeaveZone,
                    });
                    std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                    if (_timer_admission != nullptr)
                    {
                        reservation = _timer_admission->tryReserve(timeout_msg.chargedBytes(), context.turn_id);
                        if (!reservation.has_value())
                        {
                            const auto room = entering.target_room;
                            const auto zone = entering.source_zone;
                            const auto req_id = entering.request_id;
                            _workflow_state = StableRoute{.zone = zone};

                            effects.push(snf::worker::TellActorEffect{
                                .target = snf::worker::ActorKey{snf::worker::ActorKind::Room, room.value},
                                .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                                    .connection = std::nullopt,
                                    .request_id = 0,
                                    .command = snf::server::LeaveRoom{.player = *player_id},
                                    .reply_to = std::nullopt,
                                }),
                            });

                            effects.push(snf::worker::SendFrameEffect{
                                .connection = *_bound_connection,
                                .frame = encodeRoomReply(
                                    RoomReplyFrameKind::Joined,
                                    room,
                                    snf::server::RoomResult{
                                        .status = snf::server::RoomCommandStatus::EntryFailed,
                                        .phase = snf::server::RoomPhase::Waiting,
                                    },
                                    req_id
                                ),
                                .critical = false,
                            });
                            scheduleSaveIfDirty(effects, context.now);
                            return snf::worker::CompletedTurn{.effects = std::move(effects)};
                        }
                    }

                    entering.step = WorkflowStep::RoomJoinStep2_LeaveZone;

                    effects.push(snf::worker::TellActorEffect{
                        .target = snf::worker::ActorKey{snf::worker::ActorKind::Zone, entering.source_zone.value},
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = std::nullopt,
                            .request_id = entering.request_id,
                            .command =
                                snf::server::LeaveZoneCommand{
                                    .player = *player_id,
                                    .route_epoch = entering.source_epoch,
                                },
                            .reply_to =
                                WorkflowReplyTo{
                                    .player = *player_id,
                                    .connection_generation = _bound_connection->generation,
                                    .correlation_id = entering.correlation_id,
                                    .step = WorkflowStep::RoomJoinStep2_LeaveZone,
                                    .route_epoch = entering.source_epoch,
                                    .request_id = entering.request_id,
                                },
                        }),
                    });

                    if (reservation.has_value())
                    {
                        effects.push(snf::worker::ScheduleTimerEffect{
                            .deadline = context.now + WORKFLOW_TIMEOUT,
                            .message = std::move(timeout_msg),
                            .reservation = std::move(*reservation),
                        });
                    }
                }
                else
                {
                    const auto room = entering.target_room;
                    const auto zone = entering.source_zone;
                    const auto req_id = entering.request_id;
                    _workflow_state = StableRoute{.zone = zone};

                    effects.push(snf::worker::SendFrameEffect{
                        .connection = *_bound_connection,
                        .frame = encodeRoomReply(RoomReplyFrameKind::Joined, room, msg.result, req_id),
                        .critical = false,
                    });
                }
                scheduleSaveIfDirty(effects, context.now);
                return snf::worker::CompletedTurn{.effects = std::move(effects)};
            }
        }

        return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
    }

    snf::worker::TurnResult PlayerActorAdapter::handleWorkflowTimeout(
        PlayerWorkflowTimeoutMessage&& msg,
        const snf::worker::ActorTurnContext& context
    )
    {
        auto it = std::find_if(
            _pending_zone_ops.begin(),
            _pending_zone_ops.end(),
            [&msg](const auto& op) {
                return op.correlation_id == msg.correlation_id && op.step == msg.step;
            }
        );
        if (it != _pending_zone_ops.end())
        {
            const auto op = *it;
            _pending_zone_ops.erase(it);

            snf::worker::EffectBatch effects;
            if (_bound_connection.has_value())
            {
                if (op.step == WorkflowStep::ZoneEnter)
                {
                    if (_player.state().identity().has_value())
                    {
                        effects.push(snf::worker::TellActorEffect{
                            .target =
                                snf::worker::ActorKey{
                                    .kind = snf::worker::ActorKind::Zone,
                                    .entity = op.target_zone.value,
                                },
                            .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                                .connection = std::nullopt,
                                .request_id = 0,
                                .command =
                                    snf::server::LeaveZoneCommand{
                                        .player = *_player.state().identity(),
                                        .route_epoch = op.target_epoch,
                                    },
                                .reply_to = std::nullopt,
                            }),
                        });
                    }

                    const snf::server::ZoneResult result{
                        .status = snf::server::ZoneCommandStatus::TransferFailed,
                        .player = _player.state().identity(),
                        .position = std::nullopt,
                        .route_epoch = _route_epoch,
                        .tick = 0,
                        .visible_players = {},
                    };
                    effects.push(snf::worker::SendFrameEffect{
                        .connection = *_bound_connection,
                        .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, op.target_zone, result, op.request_id),
                        .critical = false,
                    });
                }
                else if (op.step == WorkflowStep::ZoneMove)
                {
                    const snf::server::ZoneResult result{
                        .status = snf::server::ZoneCommandStatus::TransferFailed,
                        .player = _player.state().identity(),
                        .position = std::nullopt,
                        .route_epoch = _route_epoch,
                        .tick = 0,
                        .visible_players = {},
                    };
                    effects.push(snf::worker::SendFrameEffect{
                        .connection = *_bound_connection,
                        .frame = encodeZoneReply(ZoneReplyFrameKind::Moved, op.target_zone, result, op.request_id),
                        .critical = false,
                    });
                }
                else if (op.step == WorkflowStep::ZoneLeave)
                {
                    if (_player.state().identity().has_value())
                    {
                        effects.push(snf::worker::TellActorEffect{
                            .target =
                                snf::worker::ActorKey{
                                    .kind = snf::worker::ActorKind::Zone,
                                    .entity = op.target_zone.value,
                                },
                            .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                                .connection = std::nullopt,
                                .request_id = 0,
                                .command =
                                    snf::server::LeaveZoneCommand{
                                        .player = *_player.state().identity(),
                                        .route_epoch = op.target_epoch,
                                    },
                                .reply_to = std::nullopt,
                            }),
                        });
                    }

                    _workflow_state = StableRoute{.zone = std::nullopt};
                    _player.setLastLocation(std::nullopt);

                    const snf::server::ZoneResult result{
                        .status = snf::server::ZoneCommandStatus::TransferFailed,
                        .player = _player.state().identity(),
                        .position = std::nullopt,
                        .route_epoch = _route_epoch,
                        .tick = 0,
                        .visible_players = {},
                    };
                    effects.push(snf::worker::SendFrameEffect{
                        .connection = *_bound_connection,
                        .frame = encodeZoneReply(ZoneReplyFrameKind::Left, op.target_zone, result, op.request_id),
                        .critical = false,
                    });
                    effects.push(snf::worker::CloseConnectionEffect{
                        .connection = *_bound_connection,
                        .reason = snf::worker::CloseReason::Application,
                        .graceful = true,
                    });
                }
            }
            scheduleSaveIfDirty(effects, context.now);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (std::holds_alternative<TransferringRoute>(_workflow_state))
        {
            const auto& transfer = std::get<TransferringRoute>(_workflow_state);
            // A completed stage cannot cancel its already scheduled one-shot
            // timer. Correlation and current step make those older timers stale.
            if (transfer.correlation_id == msg.correlation_id && transfer.step == msg.step)
            {
                const auto failed = transfer;
                snf::worker::EffectBatch effects;
                failCrossZoneKnownNone(effects, failed, true);
                scheduleSaveIfDirty(effects, context.now);
                return snf::worker::CompletedTurn{.effects = std::move(effects)};
            }
        }

        if (std::holds_alternative<EnteringRoute>(_workflow_state))
        {
            auto& entering = std::get<EnteringRoute>(_workflow_state);
            if (entering.correlation_id == msg.correlation_id && entering.step == msg.step)
            {
                snf::worker::EffectBatch effects;
                const auto room = entering.target_room;
                const auto zone = entering.source_zone;
                const auto req_id = entering.request_id;

                if (entering.step == WorkflowStep::RoomJoinStep2_LeaveZone)
                {
                    if (_player.state().identity().has_value())
                    {
                        effects.push(snf::worker::TellActorEffect{
                            .target = snf::worker::ActorKey{snf::worker::ActorKind::Room, room.value},
                            .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                                .connection = std::nullopt,
                                .request_id = 0,
                                .command = snf::server::LeaveRoom{.player = *_player.state().identity()},
                                .reply_to = std::nullopt,
                            }),
                        });
                        effects.push(snf::worker::TellActorEffect{
                            .target = snf::worker::ActorKey{snf::worker::ActorKind::Zone, zone.value},
                            .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                                .connection = std::nullopt,
                                .request_id = 0,
                                .command = snf::server::LeaveZoneCommand{
                                    .player = *_player.state().identity(),
                                    .route_epoch = entering.source_epoch,
                                },
                                .reply_to = std::nullopt,
                            }),
                        });
                    }

                    _workflow_state = StableRoute{.zone = std::nullopt};
                    _player.setLastLocation(std::nullopt);

                    if (_bound_connection.has_value())
                    {
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = *_bound_connection,
                            .frame = encodeRoomReply(
                                RoomReplyFrameKind::Joined,
                                room,
                                snf::server::RoomResult{
                                    .status = snf::server::RoomCommandStatus::EntryFailed,
                                    .phase = snf::server::RoomPhase::Waiting,
                                },
                                req_id
                            ),
                            .critical = false,
                        });
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = *_bound_connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                    }
                }
                else
                {
                    // The join may already have been applied even though its
                    // outcome was lost. LeaveRoom is idempotent, so compensate
                    // before exposing the source Zone as stable again.
                    if (_player.state().identity().has_value())
                    {
                        effects.push(snf::worker::TellActorEffect{
                            .target = snf::worker::ActorKey{snf::worker::ActorKind::Room, room.value},
                            .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                                .connection = std::nullopt,
                                .request_id = 0,
                                .command = snf::server::LeaveRoom{.player = *_player.state().identity()},
                                .reply_to = std::nullopt,
                            }),
                        });
                    }

                    _workflow_state = StableRoute{.zone = zone};

                    if (_bound_connection.has_value())
                    {
                        effects.push(snf::worker::SendFrameEffect{
                            .connection = *_bound_connection,
                            .frame = encodeRoomReply(
                                RoomReplyFrameKind::Joined,
                                room,
                                snf::server::RoomResult{
                                    .status = snf::server::RoomCommandStatus::EntryFailed,
                                    .phase = snf::server::RoomPhase::Waiting,
                                },
                                req_id
                            ),
                            .critical = false,
                        });
                    }
                }
                scheduleSaveIfDirty(effects, context.now);
                return snf::worker::CompletedTurn{.effects = std::move(effects)};
            }
        }

        if (std::holds_alternative<ReturningRoute>(_workflow_state))
        {
            auto& returning = std::get<ReturningRoute>(_workflow_state);
            if (returning.correlation_id == msg.correlation_id && returning.step == msg.step)
            {
                snf::worker::EffectBatch effects;
                if (_player.state().identity().has_value())
                {
                    effects.push(snf::worker::TellActorEffect{
                        .target = snf::worker::ActorKey{snf::worker::ActorKind::Zone, returning.return_zone.value},
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = std::nullopt,
                            .request_id = 0,
                            .command = snf::server::LeaveZoneCommand{
                                .player = *_player.state().identity(),
                                .route_epoch = returning.return_epoch,
                            },
                            .reply_to = std::nullopt,
                        }),
                    });
                }
                _player.setLastLocation(std::nullopt);
                _workflow_state = StableRoute{.zone = std::nullopt};
                if (_bound_connection.has_value())
                {
                    effects.push(snf::worker::CloseConnectionEffect{
                        .connection = *_bound_connection,
                        .reason = snf::worker::CloseReason::Application,
                        .graceful = true,
                    });
                }
                scheduleSaveIfDirty(effects, context.now);
                return snf::worker::CompletedTurn{.effects = std::move(effects)};
            }
        }

        return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
    }
}
