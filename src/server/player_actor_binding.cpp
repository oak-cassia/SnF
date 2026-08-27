#include "snf/server/player_actor_binding.hpp"

#include "snf/game/street_experience_grant.hpp"
#include "snf/server/room_join_tell.hpp"

#include "snf/game/player.hpp"

#include <cstddef>
#include <stdexcept>
#include <utility>

namespace
{
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

    snf::runtime::ActorTask<snf::server::OutboundReservation>
    awaitOutboundReservation(snf::server::OutboundSink& outbound, snf::runtime::ActorContext& context, const snf::net::ConnectionId connection, const std::size_t slots)
    {
        ReservationWaiterGuard guard;
        auto reservation = co_await snf::runtime::awaitAsyncOperation<snf::server::OutboundReservation>(
            context,
            [&guard, &outbound, connection, slots](snf::runtime::AsyncOperationProducer<snf::server::OutboundReservation> producer)
            { guard.arm(outbound, outbound.registerWaiter(connection, slots, std::move(producer))); });

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
        std::optional<StreetExperienceGrant> pending_grant;
        bool sessionless{false};
        bool reward_snapshot_pending{false};
        bool snapshot_retry_scheduled{false};
        int snapshot_retries_remaining{0};
        snf::runtime::ActorTask<PlayerLoadResult> load_task;
        snf::runtime::ActorTask<OutboundReservation> reservation_task;
        snf::runtime::ActorTask<PlayerSaveResult> save_task;
        std::optional<snf::net::ConnectionId> closing_connection;
        PlayerResult pending_result;
        snf::net::ConnectionId connection{};
        std::uint32_t request_id{0};
        std::optional<RoomEntryContext> room_entry{};
    };

    struct PlayerActorBinding::CommandPayload
    {
        PlayerInboundCommand command;
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
            return std::nullopt;
        }

        auto grant = payload.take<StreetExperienceGrant>();
        if (!grant)
        {
            return std::nullopt;
        }

        if (grant->player.value != target.entity)
        {
            return std::nullopt;
        }

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
                player_state.pending_grant = grant->grant;
                player_state.sessionless = true;
                player_state.stage = PlayerActorState::Stage::Loading;
                player_state.load_task = awaitPlayerLoad(*_repository, context, *player_state.identity.playerId());
                return advance(player_state, context, stop_token);
            }

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

        if (std::holds_alternative<PurchaseCommand>(payload.command.command) ||
            std::holds_alternative<EquipSkillCommand>(payload.command.command))
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
                return abandonResponses(state);
            }

            if (auto reservation = _outbound.tryReserve(state.connection, required_slots))
            {
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
            return abandonResponses(state);
        }
        catch (const snf::runtime::AsyncOperationCancelled&)
        {
            return abandonResponses(state);
        }

        state.reservation_task = {};
        if (!reservation.valid())
        {
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
