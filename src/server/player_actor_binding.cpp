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
    struct PlayerActorBinding::PlayerActorSlot final : snf::runtime::ActorSlot
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

        PlayerActorSlot(PlayerActorId actor_id, std::function<void(PlayerActorId)> on_deactivated, const std::size_t max_purchase_idempotency_records)
            // The Actor gets only the persistent identity: which namespace it is
            // routed in stays here, where routing lives.
            : actor(actor_id.playerId(), max_purchase_idempotency_records)
            , identity(actor_id)
            , on_deactivated(std::move(on_deactivated))
        {
        }

        ~PlayerActorSlot() override
        {
            if (on_deactivated)
            {
                on_deactivated(identity);
            }
        }

        Player actor;
        PlayerActorId identity;
        std::function<void(PlayerActorId)> on_deactivated;
        bool loaded{false};
        Stage stage{Stage::Idle};
        std::optional<PlayerCommand> pending_command;
        // Held while the record loads when a tell arrived before it. A grant is not a
        // client request, so it cannot ride in pending_command.
        std::optional<StreetExperienceGrant> pending_grant;
        // True when a tell, not a connection, brought this slot to life. Such a slot
        // has no session keeping it resident, so it passivates once the grant is saved.
        bool activated_by_tell{false};
        snf::runtime::ActorTask<PlayerLoadResult> load_task;
        // Started only when capacity was not immediately available, and owned by the
        // slot for the same reason.
        snf::runtime::ActorTask<OutboundReservation> reservation_task;
        snf::runtime::ActorTask<PlayerSaveResult> save_task;
        // The handler's decisions, held between its normal return and the application
        // whose capacity is still being awaited.
        PlayerResult pending_result;
        // Captured at dispatch because resume() does not carry the submission, and
        // responses still have to reach the connection and answer the frame that asked.
        snf::net::ConnectionId connection{};
        std::uint32_t request_id{0};
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

    PlayerActorBinding::PlayerActorBinding(PlayerResponseSink& response_sink, OutboundSink& outbound, CommandLifecycleSink& lifecycle, PlayerActorBindingConfig config)
        : _response_sink(response_sink)
        , _outbound(outbound)
        , _lifecycle(lifecycle)
        , _kind(config.actor_kind)
        , _repository(config.repository)
        , _on_before_command(std::move(config.on_before_command))
        , _on_actor_deactivated(std::move(config.on_actor_deactivated))
        , _on_record_loaded(std::move(config.on_record_loaded))
        , _persistence_service(config.persistence_service)
        , _max_purchase_idempotency_records_per_player(config.max_purchase_idempotency_records_per_player)
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

    std::unique_ptr<snf::runtime::ActorSlot> PlayerActorBinding::activate(const snf::runtime::EntityId entity)
    {
        const PlayerActorId identity = kind() == snf::runtime::ActorKind::Player ? PlayerActorId{PlayerId{.value = entity}} : PlayerActorId{ProvisionalActorId{.value = entity}};
        return std::make_unique<PlayerActorSlot>(identity, _on_actor_deactivated, _max_purchase_idempotency_records_per_player);
    }

    snf::runtime::ActorDispatchResult
    PlayerActorBinding::dispatch(snf::runtime::ActorSlot& slot, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        auto& player_slot = dynamic_cast<PlayerActorSlot&>(slot);
        if (submission.accounting() == snf::runtime::ActorAccounting::Control)
        {
            const ConnectionClosedPayload& payload = payloadAs<ConnectionClosedPayload>(submission);
            if (kind() == snf::runtime::ActorKind::ProvisionalPlayer)
            {
                return snf::runtime::ActorDispatchResult::Evict;
            }

            if (player_slot.stage != PlayerActorSlot::Stage::Idle)
            {
                throw std::logic_error{"Persistent Player close arrived before load completed"};
            }

            if (!player_slot.loaded)
            {
                return snf::runtime::ActorDispatchResult::Evict;
            }

            if (payload.closed.has_location_snapshot)
            {
                player_slot.actor.setLastLocation(payload.closed.last_location);
            }

            player_slot.stage = PlayerActorSlot::Stage::Saving;
            player_slot.save_task = awaitPlayerSave(*_persistence_service, context, player_slot.actor.snapshot());
            return advance(player_slot, context, stop_token);
        }

        if (player_slot.stage != PlayerActorSlot::Stage::Idle)
        {
            throw std::logic_error{"PlayerActorBinding dispatched a command while one was in flight"};
        }

        if (const auto* grant = tryPayloadAs<StreetExperienceGrantPayload>(submission))
        {
            if (!player_slot.loaded)
            {
                // Applying to a default-constructed actor would then save zeros over the
                // stored currency, so the record loads first exactly as a command makes
                // it. Evicting instead -- what a close does -- would lose the reward.
                player_slot.pending_grant = grant->grant;
                player_slot.activated_by_tell = true;
                player_slot.stage = PlayerActorSlot::Stage::Loading;
                player_slot.load_task = awaitPlayerLoad(*_repository, context, *player_slot.identity.playerId());
                return advance(player_slot, context, stop_token);
            }

            // A tell carries no connection, so nothing here emits a response or takes
            // outbound capacity.
            player_slot.actor.grantStreetExperience(grant->grant.experience);
            publishDirtySnapshot(player_slot);
            return snf::runtime::ActorDispatchResult::KeepActive;
        }

        const CommandPayload& payload = payloadAs<CommandPayload>(submission);
        if (_on_before_command)
        {
            _on_before_command(payload.command.actor, payload.command.command);
        }

        player_slot.connection = payload.command.connection;
        player_slot.request_id = payload.command.request_id;

        if (kind() == snf::runtime::ActorKind::Player && !player_slot.loaded)
        {
            player_slot.pending_command = payload.command.command;
            player_slot.stage = PlayerActorSlot::Stage::Loading;
            player_slot.load_task = awaitPlayerLoad(*_repository, context, *player_slot.identity.playerId());
            return advance(player_slot, context, stop_token);
        }

        if (std::holds_alternative<PurchaseCommand>(payload.command.command))
        {
            if (kind() != snf::runtime::ActorKind::Player)
            {
                throw std::logic_error{"PurchaseCommand reached a provisional Player actor"};
            }
        }

        runHandler(player_slot, payload.command.command, context);
        return advance(player_slot, context, stop_token);
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::resume(snf::runtime::ActorSlot& slot, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        auto& player_slot = dynamic_cast<PlayerActorSlot&>(slot);
        if (player_slot.stage == PlayerActorSlot::Stage::Idle)
        {
            throw std::logic_error{"PlayerActorBinding resumed without a command in flight"};
        }

        return advance(player_slot, context, stop_token);
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::advance(PlayerActorSlot& slot, snf::runtime::ActorContext& context, const std::stop_token stop_token)
    {
        if (slot.stage == PlayerActorSlot::Stage::Loading)
        {
            if (slot.load_task.resume() == snf::runtime::ActorTaskStatus::Suspended)
            {
                return snf::runtime::ActorDispatchResult::Suspended;
            }

            PlayerLoadResult loaded = slot.load_task.takeResult();
            slot.load_task = {};
            if (loaded.status != PlayerRepositoryStatus::Success)
            {
                slot.pending_command.reset();
                slot.pending_grant.reset();
                slot.stage = PlayerActorSlot::Stage::Idle;
                _outbound.reportAdmissionFailure(slot.connection);
                return snf::runtime::ActorDispatchResult::KeepActive;
            }
            if (loaded.record)
            {
                slot.actor.restore(*loaded.record);
            }
            slot.loaded = true;

            if (_on_record_loaded)
            {
                _on_record_loaded(slot.connection, loaded.record ? loaded.record->last_location : std::nullopt);
            }

            if (slot.pending_grant)
            {
                slot.actor.grantStreetExperience(slot.pending_grant->experience);
                slot.pending_grant.reset();
                publishDirtySnapshot(slot);
                slot.stage = PlayerActorSlot::Stage::Idle;
                // Nothing else is keeping this slot alive, and the snapshot is already
                // queued, so it may go once the mailbox is empty.
                return slot.activated_by_tell ? snf::runtime::ActorDispatchResult::PassivateIfIdle : snf::runtime::ActorDispatchResult::KeepActive;
            }

            if (!slot.pending_command)
            {
                throw std::logic_error{"Player load completed without a pending command"};
            }
            const PlayerCommand command = *slot.pending_command;
            runHandler(slot, command, context);
        }

        if (slot.stage == PlayerActorSlot::Stage::Reserving && !slot.reservation_task.valid())
        {
            const std::size_t required_slots = _response_sink.requiredSlots(slot.pending_result);
            if (!_outbound.canEverReserve(required_slots))
            {
                // More than one connection may ever hold. Waiting would never end and
                // throwing would take down every actor this Worker owns, so the command
                // ends here and the backend closes the connection.
                return abandonResponses(slot);
            }

            if (auto reservation = _outbound.tryReserve(slot.connection, required_slots))
            {
                // Outside saturation this is the whole story: no operation is begun, so
                // no in-flight slot, no continuation and no suspension.
                return applyResponses(slot, *reservation, stop_token);
            }

            slot.reservation_task = awaitOutboundReservation(_outbound, context, slot.connection, required_slots);
        }

        if (slot.stage == PlayerActorSlot::Stage::Saving)
        {
            if (slot.save_task.resume() == snf::runtime::ActorTaskStatus::Suspended)
            {
                return snf::runtime::ActorDispatchResult::Suspended;
            }

            const PlayerSaveResult saved = slot.save_task.takeResult();
            slot.save_task = {};
            slot.stage = PlayerActorSlot::Stage::Idle;
            if (!saved.saved())
            {
                throw std::runtime_error{"Player repository refused a save"};
            }
            return snf::runtime::ActorDispatchResult::Evict;
        }

        if (!slot.reservation_task.valid())
        {
            throw std::logic_error{"PlayerActorBinding has no reservation task to advance"};
        }

        if (slot.reservation_task.resume() == snf::runtime::ActorTaskStatus::Suspended)
        {
            return snf::runtime::ActorDispatchResult::Suspended;
        }

        OutboundReservation reservation;
        try
        {
            reservation = slot.reservation_task.takeResult();
        }
        catch (const snf::runtime::AsyncOperationRejected&)
        {
            // Outbound is saturated and this Worker's in-flight budget is exhausted, so
            // the response cannot be emitted. It is not dropped in silence: the backend
            // closes the connection under the same overflow policy inbound uses.
            return abandonResponses(slot);
        }
        catch (const snf::runtime::AsyncOperationCancelled&)
        {
            return abandonResponses(slot);
        }

        slot.reservation_task = {};
        if (!reservation.valid())
        {
            // Only a cancelled outbound backend hands back an invalid grant.
            resetPendingCommand(slot);
            if (stop_token.stop_requested())
            {
                return snf::runtime::ActorDispatchResult::Stopped;
            }

            throw std::runtime_error{"Outbound channel was cancelled while the logic runtime was active"};
        }

        return applyResponses(slot, reservation, stop_token);
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::applyResponses(PlayerActorSlot& slot, OutboundReservation& reservation, const std::stop_token stop_token)
    {
        PlayerResult result = std::move(slot.pending_result);
        const snf::net::ConnectionId connection = slot.connection;
        const std::uint32_t request_id = slot.request_id;
        resetPendingCommand(slot);

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

    void PlayerActorBinding::runHandler(PlayerActorSlot& slot, const PlayerCommand& command, snf::runtime::ActorContext& context)
    {
        slot.pending_result = slot.actor.handle(command);
        if (slot.pending_result.room_join)
        {
            // Applied after the handler returned, like every other follow-up, and not
            // priced into the outbound reservation below: a mailbox and a socket are
            // different resources. Best effort -- a full Room mailbox drops the join
            // rather than blocking this Player, and the client simply sees no answer.
            static_cast<void>(context.tryTell(
                snf::runtime::ActorKey{
                    .kind = snf::runtime::ActorKind::Room,
                    .entity = slot.pending_result.room_join->room.value,
                },
                snf::runtime::TellPayload::of(RoomJoinTell{
                    .player = *slot.identity.playerId(),
                    .request = *slot.pending_result.room_join,
                    .reply =
                        RoomReplyContext{
                            .connection = slot.connection,
                            .request_id = slot.request_id,
                            .kind = RoomReplyKind::Joined,
                        },
                })
            ));
            slot.pending_result.room_join.reset();
        }
        publishDirtySnapshot(slot);
        slot.pending_command.reset();
        slot.stage = PlayerActorSlot::Stage::Reserving;
    }

    void PlayerActorBinding::publishDirtySnapshot(PlayerActorSlot& slot) noexcept
    {
        if (_persistence_service == nullptr || !slot.actor.hasFlushableDirtyState())
        {
            return;
        }

        PlayerStateComponentMask cleared_components = slot.actor.dirtyComponents();
        try
        {
            auto snapshot = slot.actor.takeDirtySnapshot(&cleared_components);
            if (!snapshot)
            {
                return;
            }
            if (!_persistence_service->tryEnqueue(std::move(*snapshot)))
            {
                slot.actor.restoreDirtyComponents(cleared_components);
            }
        }
        catch (...)
        {
            slot.actor.restoreDirtyComponents(cleared_components);
        }
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::abandonResponses(PlayerActorSlot& slot) noexcept
    {
        slot.reservation_task = {};
        resetPendingCommand(slot);
        _outbound.reportAdmissionFailure(slot.connection);
        return snf::runtime::ActorDispatchResult::KeepActive;
    }

    void PlayerActorBinding::resetPendingCommand(PlayerActorSlot& slot) noexcept
    {
        slot.pending_command.reset();
        slot.pending_result = PlayerResult{};
        slot.stage = PlayerActorSlot::Stage::Idle;
    }
}
