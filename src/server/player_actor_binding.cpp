#include "snf/server/player_actor_binding.hpp"

#include "snf/game/street_experience_grant.hpp"
#include "snf/server/room_join_tell.hpp"

#include "snf/game/player.hpp"

#include <cstddef>
#include <stdexcept>
#include <utility>

namespace
{
    // Withdraws the waiter whenever the await ends any way other than a grant.
    //
    // A per-operation cancel resumes the frame, so this destructor runs before the
    // frame dies and the registry loses the waiter. A runtime-wide cancel destroys the
    // frame without resuming, and on that path the channel is cancelled too, which
    // releases every waiter it holds.
    class ReservationWaiterGuard final
    {
    public:
        ReservationWaiterGuard() = default;

        ReservationWaiterGuard(const ReservationWaiterGuard&) = delete;
        ReservationWaiterGuard& operator=(const ReservationWaiterGuard&) = delete;

        ~ReservationWaiterGuard()
        {
            if (_outbound != nullptr)
            {
                _outbound->withdrawWaiter(_ticket);
            }
        }

        void arm(snf::server::OutboundSink& outbound, const snf::server::ReservationTicket& ticket) noexcept
        {
            _outbound = &outbound;
            _ticket = ticket;
        }

        void disarm() noexcept
        {
            _outbound = nullptr;
        }

    private:
        snf::server::OutboundSink* _outbound{nullptr};
        snf::server::ReservationTicket _ticket{};
    };

    // The first production suspension point. The Worker does not wait here: only this
    // actor suspends, and the reactor publishes the grant back to the Worker that owns
    // it.
    snf::runtime::ActorTask<snf::server::OutboundReservation>
    awaitOutboundReservation(snf::server::OutboundSink& outbound, snf::runtime::ActorContext& context, const snf::net::ConnectionId connection, const std::size_t slots)
    {
        ReservationWaiterGuard guard;
        auto reservation = co_await snf::runtime::awaitAsyncOperation<snf::server::OutboundReservation>(
            context,
            [&guard, &outbound, connection, slots](snf::runtime::AsyncOperationProducer<snf::server::OutboundReservation> producer)
            { guard.arm(outbound, outbound.registerWaiter(connection, slots, std::move(producer))); });

        // A granted waiter has already left the registry.
        guard.disarm();
        co_return std::move(reservation);
    }

    snf::runtime::ActorTask<snf::server::PlayerLoadResult> awaitPlayerLoad(snf::server::PlayerRepository& repository, snf::runtime::ActorContext& context, const snf::server::PlayerId player)
    {
        auto result = co_await snf::runtime::awaitAsyncOperation<snf::server::PlayerLoadResult>(
            context,
            [&repository, player](snf::runtime::AsyncOperationProducer<snf::server::PlayerLoadResult> producer)
            { repository.asyncLoad(player, [producer = std::move(producer)](snf::server::PlayerLoadResult result) mutable noexcept { producer.complete(std::move(result)); }); });
        co_return std::move(result);
    }

    snf::runtime::ActorTask<snf::server::PlayerSaveResult> awaitPlayerSave(snf::server::PlayerPersistenceService& persistence, snf::runtime::ActorContext& context, snf::server::PlayerRecord record)
    {
        auto result = co_await snf::runtime::awaitAsyncOperation<snf::server::PlayerSaveResult>(
            context,
            [&persistence, record = std::move(record)](snf::runtime::AsyncOperationProducer<snf::server::PlayerSaveResult> producer)
            { persistence.asyncSave(std::move(record), [producer = std::move(producer)](snf::server::PlayerSaveResult result) mutable noexcept { producer.complete(std::move(result)); }); });
        co_return std::move(result);
    }
}

namespace snf::server
{
    struct PlayerActorBinding::PlayerActorState final : snf::runtime::ActorState
    {
        // Applying follow-ups needs capacity the handler must not know about, so a command runs
        // in two stages: the handler's own task, then the reservation's.
        enum class Stage
        {
            Idle,
            Loading,
            Reserving,
            Saving,
        };

        PlayerActorState(
            PlayerActorId actor_id,
            std::function<void(PlayerActorId, std::optional<snf::net::ConnectionId>)> on_deactivated,
            const std::size_t max_purchase_idempotency_records
        )
            // The Actor gets only the persistent identity: which namespace it is
            // routed in stays here, where routing lives.
            : player(actor_id.playerId(), max_purchase_idempotency_records)
            , identity(actor_id)
            , on_deactivated(std::move(on_deactivated))
        {
        }

        ~PlayerActorState() override
        {
            if (on_deactivated)
            {
                on_deactivated(identity, closing_connection);
            }
        }

        Player player;
        PlayerActorId identity;
        std::function<void(PlayerActorId, std::optional<snf::net::ConnectionId>)> on_deactivated;
        bool loaded{false};
        Stage stage{Stage::Idle};
        std::optional<PlayerCommand> pending_command;
        // Held while the record loads when a tell arrived before it. A grant is not a
        // client request, so it cannot ride in pending_command.
        std::optional<StreetExperienceGrant> pending_grant;
        // True while no session command has claimed an actor that a tell brought to
        // life. A command may arrive while a snapshot retry timer is outstanding, so
        // this describes current residency rather than immutable activation history.
        bool sessionless{false};
        bool reward_snapshot_pending{false};
        bool snapshot_retry_scheduled{false};
        int snapshot_retries_remaining{0};
        snf::runtime::ActorTask<PlayerLoadResult> load_task;
        // Started only when capacity was not immediately available, and owned by the
        // state for the same reason.
        snf::runtime::ActorTask<OutboundReservation> reservation_task;
        snf::runtime::ActorTask<PlayerSaveResult> save_task;
        // Stable passivation identity captured only from ConnectionClosed. The
        // response connection below is per-command scratch and cannot identify the
        // session whose final save caused this Actor to leave.
        std::optional<snf::net::ConnectionId> closing_connection;
        // The handler's decisions, held between its normal return and the application
        // whose capacity is still being awaited.
        PlayerResult pending_result;
        // Captured at dispatch because resume() does not carry the submission, and
        // responses still have to reach the connection and answer the frame that asked.
        snf::net::ConnectionId connection{};
        std::uint32_t request_id{0};
        // Captured for the same reason, and kept out of the Player for another one: a
        // ticket and a connection are reactor identity, and the game model must not
        // name them. The handler answers with the room and the stats; this says which
        // entry saga asked.
        std::optional<RoomEntryContext> room_entry{};
    };

    struct PlayerActorBinding::CommandPayload
    {
        PlayerInboundCommand command;
        // Destroyed with this payload, which is what makes the release fire exactly once
        // per command on every path the scheduler has.
        CommandReleaseToken release;
    };

    struct PlayerActorBinding::ConnectionClosedPayload
    {
        ConnectionClosed closed;
    };

    struct PlayerActorBinding::StreetExperienceGrantPayload
    {
        StreetExperienceGrant grant;
    };

    struct PlayerActorBinding::SnapshotRetryPayload
    {
    };

    PlayerActorBinding::PlayerActorBinding(
        PlayerResponseSink& response_sink, OutboundSink& outbound, CommandLifecycleSink& lifecycle, PlayerActorBindingConfig config
    )
        : _response_sink(response_sink)
        , _outbound(outbound)
        , _lifecycle(lifecycle)
        , _kind(config.actor_kind)
        , _repository(config.repository)
        , _on_before_command(std::move(config.on_before_command))
        , _on_actor_deactivated(std::move(config.on_actor_deactivated))
        , _on_record_loaded(std::move(config.on_record_loaded))
        , _on_room_join_undelivered(std::move(config.on_room_join_undelivered))
        , _persistence_service(config.persistence_service)
        , _max_purchase_idempotency_records_per_player(config.max_purchase_idempotency_records_per_player)
        , _snapshot_retry_delay(config.snapshot_retry_delay)
        , _snapshot_retry_limit(config.snapshot_retry_limit)
    {
        if (_kind != snf::runtime::ActorKind::ProvisionalPlayer && _kind != snf::runtime::ActorKind::Player)
        {
            throw std::invalid_argument{"PlayerActorBinding requires a Player actor kind"};
        }

        if (_kind == snf::runtime::ActorKind::Player && _repository == nullptr)
        {
            throw std::invalid_argument{"Persistent PlayerActorBinding requires a repository"};
        }
        if (_max_purchase_idempotency_records_per_player == 0)
        {
            throw std::invalid_argument{"Purchase idempotency capacity must be positive"};
        }
        if (_snapshot_retry_delay <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Player snapshot retry delay must be positive"};
        }
        if (_snapshot_retry_limit < 0)
        {
            throw std::invalid_argument{"Player snapshot retry limit cannot be negative"};
        }
        if (_kind == snf::runtime::ActorKind::Player && _persistence_service == nullptr)
        {
            _owned_persistence_service = std::make_unique<PlayerPersistenceService>(*_repository);
            _persistence_service = _owned_persistence_service.get();
        }
    }

    snf::runtime::ActorKind PlayerActorBinding::kind() const noexcept
    {
        return _kind;
    }

    PlayerActorBindingStats PlayerActorBinding::stats() const noexcept
    {
        return PlayerActorBindingStats{
            .reward_snapshot_admission_rejections = _reward_snapshot_admission_rejections.load(std::memory_order_relaxed),
            .reward_snapshot_retry_giveups = _reward_snapshot_retry_giveups.load(std::memory_order_relaxed),
            .grant_load_failures = _grant_load_failures.load(std::memory_order_relaxed),
        };
    }

    snf::runtime::ActorSubmission PlayerActorBinding::makeCommand(PlayerInboundCommand command) const
    {
        const PlayerActorId actor = command.actor;
        if (actor.kind() != kind())
        {
            throw std::invalid_argument{"Player command target does not match its binding"};
        }
        const snf::net::ConnectionId connection = command.connection;
        return makeSubmission(
            snf::runtime::ActorKey{
                .kind = kind(),
                .entity = actor.value,
            },
            snf::runtime::ActorActivation::ActivateIfMissing,
            snf::runtime::ActorAccounting::Command,
            CommandPayload{
                .command = std::move(command),
                // Armed here, at the boundary that admits a command. A frame the protocol
                // rejected earlier never became a command, so it takes no credit and
                // reports nothing. A refused post does release, because it took credit
                // here before the runtime turned it away.
                .release = CommandReleaseToken{_lifecycle, connection},
            });
    }

    snf::runtime::ActorSubmission PlayerActorBinding::makeConnectionClosed(const PlayerActorId actor, ConnectionClosed closed) const
    {
        if (actor.kind() != kind())
        {
            throw std::invalid_argument{"Player close target does not match its binding"};
        }

        return makeSubmission(
            snf::runtime::ActorKey{
                .kind = kind(),
                .entity = actor.value,
            },
            snf::runtime::ActorActivation::ExistingOnly,
            snf::runtime::ActorAccounting::Control,
            ConnectionClosedPayload{
                .closed = std::move(closed),
            });
    }

    std::optional<snf::runtime::ActorSubmission> PlayerActorBinding::makeTell(const snf::runtime::ActorKey target, snf::runtime::TellPayload payload)
    {
        if (_kind != snf::runtime::ActorKind::Player)
        {
            // Only the persistent namespace has a record to grant against.
            return std::nullopt;
        }

        auto grant = payload.take<StreetExperienceGrant>();
        if (!grant)
        {
            // A refused take leaves the carrier intact, and the runtime reports the
            // mismatch as the wiring bug it is rather than delivering a wrong command.
            return std::nullopt;
        }

        if (grant->player.value != target.entity)
        {
            // The grant names its player and the key names the actor. They can only
            // disagree through a routing bug, and delivering it anyway would credit
            // the wrong account.
            return std::nullopt;
        }

        // ActivateIfMissing: a reward has to reach a player who logged out between the
        // battle and the clear. ExistingOnly would report Accepted and drop it.
        return makeSubmission(target, snf::runtime::ActorActivation::ActivateIfMissing, snf::runtime::ActorAccounting::Command, StreetExperienceGrantPayload{.grant = *grant});
    }

    std::unique_ptr<snf::runtime::ActorState> PlayerActorBinding::activate(const snf::runtime::EntityId entity)
    {
        const PlayerActorId identity = kind() == snf::runtime::ActorKind::Player ? PlayerActorId{PlayerId{.value = entity}} : PlayerActorId{ProvisionalActorId{.value = entity}};
        return std::make_unique<PlayerActorState>(identity, _on_actor_deactivated, _max_purchase_idempotency_records_per_player);
    }

    snf::runtime::ActorDispatchResult
    PlayerActorBinding::dispatch(snf::runtime::ActorState& state, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        auto& player_state = dynamic_cast<PlayerActorState&>(state);
        if (submission.accounting() == snf::runtime::ActorAccounting::Control)
        {
            const ConnectionClosedPayload& payload = payloadAs<ConnectionClosedPayload>(submission);
            if (kind() == snf::runtime::ActorKind::ProvisionalPlayer)
            {
                return snf::runtime::ActorDispatchResult::Evict;
            }

            if (player_state.stage != PlayerActorState::Stage::Idle)
            {
                throw std::logic_error{"Persistent Player close arrived before load completed"};
            }

            player_state.closing_connection = payload.closed.connection;
            if (!player_state.loaded)
            {
                return snf::runtime::ActorDispatchResult::Evict;
            }

            if (payload.closed.has_location_snapshot)
            {
                player_state.player.setLastLocation(payload.closed.last_location);
            }

            player_state.stage = PlayerActorState::Stage::Saving;
            player_state.save_task = awaitPlayerSave(*_persistence_service, context, player_state.player.snapshot());
            return advance(player_state, context, stop_token);
        }

        if (player_state.stage != PlayerActorState::Stage::Idle)
        {
            throw std::logic_error{"PlayerActorBinding dispatched a command while one was in flight"};
        }

        if (tryPayloadAs<SnapshotRetryPayload>(submission) != nullptr)
        {
            const SnapshotPublishOutcome outcome = publishDirtySnapshot(player_state, context, true);
            return outcome == SnapshotPublishOutcome::RetryScheduled ? snf::runtime::ActorDispatchResult::KeepActive
                                                                     : snapshotTerminalResult(player_state);
        }

        if (const auto* grant = tryPayloadAs<StreetExperienceGrantPayload>(submission))
        {
            if (!player_state.loaded)
            {
                // Applying to a default-constructed actor would then save zeros over the
                // stored currency, so the record loads first exactly as a command makes
                // it. Evicting instead -- what a close does -- would lose the reward.
                player_state.pending_grant = grant->grant;
                player_state.sessionless = true;
                player_state.stage = PlayerActorState::Stage::Loading;
                player_state.load_task = awaitPlayerLoad(*_repository, context, *player_state.identity.playerId());
                return advance(player_state, context, stop_token);
            }

            // A tell carries no connection, so nothing here emits a response or takes
            // outbound capacity.
            player_state.player.grantStreetExperience(grant->grant.experience);
            player_state.reward_snapshot_pending = true;
            const SnapshotPublishOutcome outcome = publishDirtySnapshot(player_state, context, false);
            return outcome == SnapshotPublishOutcome::RetryScheduled ? snf::runtime::ActorDispatchResult::KeepActive
                                                                     : snapshotTerminalResult(player_state);
        }

        const CommandPayload& payload = payloadAs<CommandPayload>(submission);
        player_state.sessionless = false;
        if (_on_before_command)
        {
            _on_before_command(payload.command.actor, payload.command.command);
        }

        player_state.connection = payload.command.connection;
        player_state.request_id = payload.command.request_id;
        player_state.room_entry = payload.command.room_entry;

        if (kind() == snf::runtime::ActorKind::Player && !player_state.loaded)
        {
            player_state.pending_command = payload.command.command;
            player_state.stage = PlayerActorState::Stage::Loading;
            player_state.load_task = awaitPlayerLoad(*_repository, context, *player_state.identity.playerId());
            return advance(player_state, context, stop_token);
        }

        if (std::holds_alternative<PurchaseCommand>(payload.command.command))
        {
            if (kind() != snf::runtime::ActorKind::Player)
            {
                throw std::logic_error{"PurchaseCommand reached a provisional Player actor"};
            }
        }

        runHandler(player_state, payload.command.command, context);
        return advance(player_state, context, stop_token);
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::resume(snf::runtime::ActorState& state, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        auto& player_state = dynamic_cast<PlayerActorState&>(state);
        if (player_state.stage == PlayerActorState::Stage::Idle)
        {
            throw std::logic_error{"PlayerActorBinding resumed without a command in flight"};
        }

        return advance(player_state, context, stop_token);
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::advance(PlayerActorState& state, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        if (state.stage == PlayerActorState::Stage::Loading)
        {
            if (state.load_task.resume() == snf::runtime::ActorTaskStatus::Suspended)
            {
                return snf::runtime::ActorDispatchResult::Suspended;
            }

            PlayerLoadResult loaded = state.load_task.takeResult();
            state.load_task = {};
            if (loaded.status != PlayerRepositoryStatus::Success)
            {
                const bool discarded_grant = state.pending_grant.has_value();
                state.pending_command.reset();
                state.pending_grant.reset();
                state.stage = PlayerActorState::Stage::Idle;
                if (discarded_grant)
                {
                    _grant_load_failures.fetch_add(1, std::memory_order_relaxed);
                    return snapshotTerminalResult(state);
                }
                _outbound.reportAdmissionFailure(state.connection);
                return snf::runtime::ActorDispatchResult::KeepActive;
            }
            if (loaded.record)
            {
                state.player.restore(*loaded.record);
            }
            state.loaded = true;

            if (_on_record_loaded)
            {
                _on_record_loaded(state.connection, loaded.record ? loaded.record->last_location : std::nullopt);
            }

            if (state.pending_grant)
            {
                state.player.grantStreetExperience(state.pending_grant->experience);
                state.reward_snapshot_pending = true;
                state.pending_grant.reset();
                state.stage = PlayerActorState::Stage::Idle;
                const SnapshotPublishOutcome outcome = publishDirtySnapshot(state, context, false);
                return outcome == SnapshotPublishOutcome::RetryScheduled ? snf::runtime::ActorDispatchResult::KeepActive
                                                                         : snapshotTerminalResult(state);
            }

            if (!state.pending_command)
            {
                throw std::logic_error{"Player load completed without a pending command"};
            }
            const PlayerCommand command = *state.pending_command;
            runHandler(state, command, context);
        }

        if (state.stage == PlayerActorState::Stage::Reserving && !state.reservation_task.valid())
        {
            const std::size_t required_slots = _response_sink.requiredSlots(state.pending_result);
            if (!_outbound.canEverReserve(required_slots))
            {
                // More than one connection may ever hold. Waiting would never end and
                // throwing would take down every actor this Worker owns, so the command
                // ends here and the backend closes the connection.
                return abandonResponses(state);
            }

            if (auto reservation = _outbound.tryReserve(state.connection, required_slots))
            {
                // Outside saturation this is the whole story: no operation is begun, so
                // no in-flight slot, no continuation and no suspension.
                return applyResponses(state, *reservation, stop_token);
            }

            state.reservation_task = awaitOutboundReservation(_outbound, context, state.connection, required_slots);
        }

        if (state.stage == PlayerActorState::Stage::Saving)
        {
            if (state.save_task.resume() == snf::runtime::ActorTaskStatus::Suspended)
            {
                return snf::runtime::ActorDispatchResult::Suspended;
            }

            const PlayerSaveResult saved = state.save_task.takeResult();
            state.save_task = {};
            state.stage = PlayerActorState::Stage::Idle;
            if (!saved.saved())
            {
                throw std::runtime_error{"Player repository refused a save"};
            }
            return snf::runtime::ActorDispatchResult::Evict;
        }

        if (!state.reservation_task.valid())
        {
            throw std::logic_error{"PlayerActorBinding has no reservation task to advance"};
        }

        if (state.reservation_task.resume() == snf::runtime::ActorTaskStatus::Suspended)
        {
            return snf::runtime::ActorDispatchResult::Suspended;
        }

        OutboundReservation reservation;
        try
        {
            reservation = state.reservation_task.takeResult();
        }
        catch (const snf::runtime::AsyncOperationRejected&)
        {
            // Outbound is saturated and this Worker's in-flight budget is exhausted, so
            // the response cannot be emitted. It is not dropped in silence: the backend
            // closes the connection under the same overflow policy inbound uses.
            return abandonResponses(state);
        }
        catch (const snf::runtime::AsyncOperationCancelled&)
        {
            return abandonResponses(state);
        }

        state.reservation_task = {};
        if (!reservation.valid())
        {
            // Only a cancelled outbound backend hands back an invalid grant.
            resetPendingCommand(state);
            if (stop_token.stop_requested())
            {
                return snf::runtime::ActorDispatchResult::Stopped;
            }

            throw std::runtime_error{"Outbound channel was cancelled while the logic runtime was active"};
        }

        return applyResponses(state, reservation, stop_token);
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::applyResponses(PlayerActorState& state, OutboundReservation& reservation, const std::stop_token stop_token)
    {
        PlayerResult result = std::move(state.pending_result);
        const snf::net::ConnectionId connection = state.connection;
        const std::uint32_t request_id = state.request_id;
        resetPendingCommand(state);

        if (_response_sink.applyResponses(connection, request_id, std::move(result), reservation))
        {
            return snf::runtime::ActorDispatchResult::KeepActive;
        }

        if (stop_token.stop_requested())
        {
            return snf::runtime::ActorDispatchResult::Stopped;
        }

        throw std::runtime_error{"Player follow-up application failed while logic runtime was active"};
    }

    void PlayerActorBinding::runHandler(PlayerActorState& state, const PlayerCommand& command, snf::runtime::ActorContext& context)
    {
        state.pending_result = state.player.handle(command);
        if (state.pending_result.room_join)
        {
            // Applied after the handler returned, like every other follow-up, and not
            // priced into the outbound reservation below: a mailbox and a socket are
            // different resources. A full Room mailbox therefore drops the join rather
            // than blocking this Player.
            const RoomId room = state.pending_result.room_join->room;
            const snf::runtime::PostResult posted = context.tryTell(
                snf::runtime::ActorKey{
                    .kind = snf::runtime::ActorKind::Room,
                    .entity = room.value,
                },
                snf::runtime::TellPayload::of(RoomJoinTell{
                    .player = *state.identity.playerId(),
                    .request = *state.pending_result.room_join,
                    .reply =
                        RoomReplyContext{
                            .connection = state.connection,
                            .request_id = state.request_id,
                            .kind = RoomReplyKind::Joined,
                        },
                    .entry = state.room_entry,
                })
            );
            // A dropped join is only harmless when nobody is waiting for it. An entry
            // saga is, and it has no timeout: without this the connection would sit in
            // a hidden route forever, so the refusal is reported and becomes the
            // saga's terminal outcome.
            if (posted != snf::runtime::PostResult::Accepted && state.room_entry && _on_room_join_undelivered)
            {
                _on_room_join_undelivered(*state.room_entry, room);
            }
            state.pending_result.room_join.reset();
            state.room_entry.reset();
        }
        if (state.reward_snapshot_pending)
        {
            static_cast<void>(publishDirtySnapshot(state, context, false));
        }
        else
        {
            static_cast<void>(tryPublishDirtySnapshot(state));
        }
        state.pending_command.reset();
        state.stage = PlayerActorState::Stage::Reserving;
    }

    PlayerActorBinding::SnapshotPublishOutcome
    PlayerActorBinding::publishDirtySnapshot(PlayerActorState& state, snf::runtime::ActorContext& context, const bool retry_attempt) noexcept
    {
        if (retry_attempt)
        {
            state.snapshot_retry_scheduled = false;
            if (state.snapshot_retries_remaining > 0)
            {
                --state.snapshot_retries_remaining;
            }
        }

        if (tryPublishDirtySnapshot(state))
        {
            state.reward_snapshot_pending = false;
            state.snapshot_retries_remaining = 0;
            return SnapshotPublishOutcome::CleanOrAccepted;
        }

        _reward_snapshot_admission_rejections.fetch_add(1, std::memory_order_relaxed);
        if (!retry_attempt && state.snapshot_retries_remaining == 0)
        {
            state.snapshot_retries_remaining = _snapshot_retry_limit;
        }

        if (state.snapshot_retry_scheduled)
        {
            return SnapshotPublishOutcome::RetryScheduled;
        }
        if (state.snapshot_retries_remaining == 0 || !tryScheduleSnapshotRetry(state, context))
        {
            state.reward_snapshot_pending = false;
            state.snapshot_retries_remaining = 0;
            _reward_snapshot_retry_giveups.fetch_add(1, std::memory_order_relaxed);
            return SnapshotPublishOutcome::GaveUp;
        }
        return SnapshotPublishOutcome::RetryScheduled;
    }

    bool PlayerActorBinding::tryPublishDirtySnapshot(PlayerActorState& state) noexcept
    {
        if (_persistence_service == nullptr || !state.player.hasFlushableDirtyState())
        {
            return true;
        }

        PlayerStateComponentMask cleared_components = state.player.dirtyComponents();
        try
        {
            auto snapshot = state.player.takeDirtySnapshot(&cleared_components);
            if (!snapshot)
            {
                return true;
            }
            if (!_persistence_service->tryEnqueue(std::move(*snapshot)))
            {
                state.player.restoreDirtyComponents(cleared_components);
                return false;
            }
            return true;
        }
        catch (...)
        {
            state.player.restoreDirtyComponents(cleared_components);
            return false;
        }
    }

    bool PlayerActorBinding::tryScheduleSnapshotRetry(PlayerActorState& state, snf::runtime::ActorContext& context) noexcept
    {
        try
        {
            auto submission = makeSubmission(
                snf::runtime::ActorKey{
                    .kind = kind(),
                    .entity = state.identity.value,
                },
                snf::runtime::ActorActivation::ExistingOnly,
                snf::runtime::ActorAccounting::Command,
                SnapshotRetryPayload{}
            );
            if (!context.trySchedule(_snapshot_retry_delay, std::move(submission)))
            {
                return false;
            }
            state.snapshot_retry_scheduled = true;
            return true;
        }
        catch (...)
        {
            return false;
        }
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::snapshotTerminalResult(const PlayerActorState& state) noexcept
    {
        return state.sessionless ? snf::runtime::ActorDispatchResult::PassivateIfIdle : snf::runtime::ActorDispatchResult::KeepActive;
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::abandonResponses(PlayerActorState& state) noexcept
    {
        state.reservation_task = {};
        resetPendingCommand(state);
        _outbound.reportAdmissionFailure(state.connection);
        return snf::runtime::ActorDispatchResult::KeepActive;
    }

    void PlayerActorBinding::resetPendingCommand(PlayerActorState& state) noexcept
    {
        state.pending_command.reset();
        state.pending_result = PlayerResult{};
        state.room_entry.reset();
        state.stage = PlayerActorState::Stage::Idle;
    }
}
