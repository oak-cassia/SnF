#include "snf/adapter/player_actor_adapter.hpp"

#include "snf/adapter/game_payloads.hpp"
#include "snf/adapter/to_effects.hpp"

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
            if (std::holds_alternative<snf::server::AuthenticateCommand>(msg.command))
            {
                if (auto conflict = rejectConflictingAuthentication(msg.connection))
                {
                    return snf::worker::CompletedTurn{.effects = std::move(*conflict)};
                }
                _bound_connection = msg.connection;
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
                _bound_connection.reset();
                if (_current_zone.has_value())
                {
                    const auto leaving_zone = *_current_zone;
                    _current_zone.reset();
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
                            }),
                        });
                    }
                }
            }
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<PlayerZoneRequestMessage>())
        {
            auto msg = envelope.take<PlayerZoneRequestMessage>();
            return handleZoneRequest(std::move(msg), context);
        }

        if (envelope.is<PingMessage>())
        {
            auto msg = envelope.take<PingMessage>();
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

    snf::worker::TurnResult PlayerActorAdapter::handleZoneRequest(PlayerZoneRequestMessage&& msg, const snf::worker::ActorTurnContext& context)
    {
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

                    if (_current_zone.has_value() && *_current_zone != req.zone)
                    {
                        // Zone-to-zone transfer fails until 11F handoff saga. Keep connection alive.
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
                            .frame = encodeZoneReply(ZoneReplyFrameKind::Entered, *_current_zone, result, msg.request_id),
                            .critical = false,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    if (!_current_zone.has_value())
                    {
                        ++_route_epoch;
                        _current_zone = req.zone;
                    }

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target =
                            snf::worker::ActorKey{
                                .kind = snf::worker::ActorKind::Zone,
                                .entity = _current_zone->value,
                            },
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = msg.connection,
                            .request_id = msg.request_id,
                            .command =
                                snf::server::EnterZoneCommand{
                                    .player = player,
                                    .route_epoch = _route_epoch,
                                    .position = req.position,
                                },
                        }),
                    });
                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
                else if constexpr (std::is_same_v<T, MoveRequest>)
                {
                    if (!_current_zone.has_value())
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
                        .target =
                            snf::worker::ActorKey{
                                .kind = snf::worker::ActorKind::Zone,
                                .entity = _current_zone->value,
                            },
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = msg.connection,
                            .request_id = msg.request_id,
                            .command =
                                snf::server::MoveInZoneCommand{
                                    .player = player,
                                    .route_epoch = _route_epoch,
                                    .position = req.position,
                                },
                        }),
                    });
                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
                else if constexpr (std::is_same_v<T, LeaveRequest>)
                {
                    if (!_current_zone.has_value())
                    {
                        snf::worker::EffectBatch effects;
                        effects.push(snf::worker::CloseConnectionEffect{
                            .connection = msg.connection,
                            .reason = snf::worker::CloseReason::Application,
                            .graceful = true,
                        });
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto leaving_zone = *_current_zone;
                    _current_zone.reset();

                    snf::worker::EffectBatch effects;
                    effects.push(snf::worker::TellActorEffect{
                        .target =
                            snf::worker::ActorKey{
                                .kind = snf::worker::ActorKind::Zone,
                                .entity = leaving_zone.value,
                            },
                        .message = GameActorPayloadRegistry::create(ZoneCommandMessage{
                            .connection = msg.connection,
                            .request_id = msg.request_id,
                            .command =
                                snf::server::LeaveZoneCommand{
                                    .player = player,
                                    .route_epoch = _route_epoch,
                                },
                        }),
                    });
                    scheduleSaveIfDirty(effects, context.now);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
            },
            msg.request
        );
    }
}
