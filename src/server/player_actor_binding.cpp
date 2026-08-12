#include "snf/server/player_actor_binding.hpp"

#include "snf/server/player_actor.hpp"

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

        void arm(snf::server::OutboundSink& outbound,
                 const snf::server::ReservationTicket& ticket) noexcept
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
    awaitOutboundReservation(snf::server::OutboundSink& outbound,
                             snf::runtime::ActorContext& context,
                             const snf::net::ConnectionId connection,
                             const std::size_t slots)
    {
        ReservationWaiterGuard guard;
        auto reservation =
            co_await snf::runtime::awaitAsyncOperation<snf::server::OutboundReservation>(
                context,
                [&guard, &outbound, connection, slots](
                    snf::runtime::AsyncOperationProducer<snf::server::OutboundReservation> producer)
                {
                    guard.arm(outbound,
                              outbound.registerWaiter(connection, slots, std::move(producer)));
                });

        // A granted waiter has already left the registry.
        guard.disarm();
        co_return std::move(reservation);
    }

    snf::runtime::ActorTask<snf::server::PlayerLoadResult>
    awaitPlayerLoad(snf::server::PlayerRepository& repository,
                    snf::runtime::ActorContext& context,
                    const snf::server::PlayerId player)
    {
        auto result = co_await snf::runtime::awaitAsyncOperation<snf::server::PlayerLoadResult>(
            context,
            [&repository,
             player](snf::runtime::AsyncOperationProducer<snf::server::PlayerLoadResult> producer)
            {
                repository.asyncLoad(player,
                                     [producer = std::move(producer)](
                                         snf::server::PlayerLoadResult result) mutable noexcept
                                     { producer.complete(std::move(result)); });
            });
        co_return std::move(result);
    }

    snf::runtime::ActorTask<snf::server::PlayerSaveResult>
    awaitPlayerSave(snf::server::PlayerRepository& repository,
                    snf::runtime::ActorContext& context,
                    snf::server::PlayerRecord record)
    {
        auto result = co_await snf::runtime::awaitAsyncOperation<snf::server::PlayerSaveResult>(
            context,
            [&repository, record = std::move(record)](
                snf::runtime::AsyncOperationProducer<snf::server::PlayerSaveResult> producer)
            {
                repository.asyncSave(record,
                                     [producer = std::move(producer)](
                                         snf::server::PlayerSaveResult result) mutable noexcept
                                     { producer.complete(std::move(result)); });
            });
        co_return std::move(result);
    }
}

namespace snf::server
{
    struct PlayerActorBinding::PlayerActorSlot final : snf::runtime::ActorSlot
    {
        // Emission needs capacity the handler must not know about, so a command runs
        // in two stages: the handler's own task, then the reservation's.
        enum class Stage
        {
            Idle,
            Loading,
            Handling,
            Reserving,
            Saving,
        };

        PlayerActorSlot(PlayerActorId actor_id, std::function<void(PlayerActorId)> on_deactivated)
            : actor(actor_id)
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

        PlayerActor actor;
        PlayerActorId identity;
        std::function<void(PlayerActorId)> on_deactivated;
        bool loaded{false};
        Stage stage{Stage::Idle};
        std::optional<PlayerCommand> pending_command;
        snf::runtime::ActorTask<PlayerLoadResult> load_task;
        // Holds the handler's task while it runs, including across a suspension.
        // Keeping the frame in the slot is what confines resume and destruction to
        // the owning Worker.
        snf::runtime::ActorTask<PlayerResult> task;
        // Started only when capacity was not immediately available, and owned by the
        // slot for the same reason.
        snf::runtime::ActorTask<OutboundReservation> reservation_task;
        snf::runtime::ActorTask<PlayerSaveResult> save_task;
        // The handler's decisions, held between its normal return and the emission
        // whose capacity is still being awaited.
        PlayerResult pending_result;
        // Captured at dispatch because resume() does not carry the submission, and
        // effects still have to reach the connection that issued the command.
        snf::net::ConnectionId connection{};
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

    PlayerActorBinding::PlayerActorBinding(PlayerEffectSink& effects,
                                           OutboundSink& outbound,
                                           CommandLifecycleSink& lifecycle,
                                           PlayerActorBindingConfig config)
        : _effects(effects)
        , _outbound(outbound)
        , _lifecycle(lifecycle)
        , _kind(config.actor_kind)
        , _repository(config.repository)
        , _on_before_command(std::move(config.on_before_command))
        , _on_actor_deactivated(std::move(config.on_actor_deactivated))
    {
        if (_kind != snf::runtime::ActorKind::ProvisionalPlayer &&
            _kind != snf::runtime::ActorKind::Player)
        {
            throw std::invalid_argument{"PlayerActorBinding requires a Player actor kind"};
        }

        if (_kind == snf::runtime::ActorKind::Player && _repository == nullptr)
        {
            throw std::invalid_argument{"Persistent PlayerActorBinding requires a repository"};
        }
    }

    snf::runtime::ActorKind PlayerActorBinding::kind() const noexcept
    {
        return _kind;
    }

    snf::runtime::ActorSubmission
    PlayerActorBinding::makeCommand(PlayerInboundCommand command) const
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

    snf::runtime::ActorSubmission
    PlayerActorBinding::makeConnectionClosed(const PlayerActorId actor,
                                             ConnectionClosed closed) const
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

    std::unique_ptr<snf::runtime::ActorSlot>
    PlayerActorBinding::activate(const snf::runtime::EntityId entity)
    {
        const PlayerActorId identity = kind() == snf::runtime::ActorKind::Player
                                           ? PlayerActorId{PlayerId{.value = entity}}
                                           : PlayerActorId{ProvisionalActorId{.value = entity}};
        return std::make_unique<PlayerActorSlot>(identity, _on_actor_deactivated);
    }

    snf::runtime::ActorDispatchResult
    PlayerActorBinding::dispatch(snf::runtime::ActorSlot& slot,
                                 const snf::runtime::ActorSubmission& submission,
                                 snf::runtime::ActorContext& context,
                                 const std::stop_token stop_token)
    {
        auto& player_slot = dynamic_cast<PlayerActorSlot&>(slot);
        if (submission.accounting() == snf::runtime::ActorAccounting::Control)
        {
            static_cast<void>(payloadAs<ConnectionClosedPayload>(submission));
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

            player_slot.stage = PlayerActorSlot::Stage::Saving;
            player_slot.save_task =
                awaitPlayerSave(*_repository, context, player_slot.actor.snapshot());
            return advance(player_slot, context, stop_token);
        }

        if (player_slot.stage != PlayerActorSlot::Stage::Idle)
        {
            throw std::logic_error{
                "PlayerActorBinding dispatched a command while one was in flight"};
        }

        const CommandPayload& payload = payloadAs<CommandPayload>(submission);
        if (_on_before_command)
        {
            _on_before_command(payload.command.actor, payload.command.command);
        }

        player_slot.connection = payload.command.connection;

        if (kind() == snf::runtime::ActorKind::Player && !player_slot.loaded)
        {
            player_slot.pending_command = payload.command.command;
            player_slot.stage = PlayerActorSlot::Stage::Loading;
            player_slot.load_task =
                awaitPlayerLoad(*_repository, context, *player_slot.identity.playerId());
            return advance(player_slot, context, stop_token);
        }

        player_slot.stage = PlayerActorSlot::Stage::Handling;
        player_slot.task = player_slot.actor.handle(payload.command.command);
        return advance(player_slot, context, stop_token);
    }

    snf::runtime::ActorDispatchResult
    PlayerActorBinding::resume(snf::runtime::ActorSlot& slot,
                               snf::runtime::ActorContext& context,
                               const std::stop_token stop_token)
    {
        auto& player_slot = dynamic_cast<PlayerActorSlot&>(slot);
        if (player_slot.stage == PlayerActorSlot::Stage::Idle)
        {
            throw std::logic_error{"PlayerActorBinding resumed without a command in flight"};
        }

        return advance(player_slot, context, stop_token);
    }

    snf::runtime::ActorDispatchResult
    PlayerActorBinding::advance(PlayerActorSlot& slot,
                                snf::runtime::ActorContext& context,
                                const std::stop_token stop_token)
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
                slot.stage = PlayerActorSlot::Stage::Idle;
                _outbound.reportAdmissionFailure(slot.connection);
                return snf::runtime::ActorDispatchResult::KeepActive;
            }
            if (loaded.record)
            {
                slot.actor.restore(*loaded.record);
            }
            slot.loaded = true;

            if (!slot.pending_command)
            {
                throw std::logic_error{"Player load completed without a pending command"};
            }
            slot.stage = PlayerActorSlot::Stage::Handling;
            slot.task = slot.actor.handle(*slot.pending_command);
        }

        if (slot.stage == PlayerActorSlot::Stage::Handling)
        {
            if (slot.task.resume() == snf::runtime::ActorTaskStatus::Suspended)
            {
                return snf::runtime::ActorDispatchResult::Suspended;
            }

            // Effects are applied only after the handler has returned normally, which
            // is the same ordering the synchronous handler had. takeResult rethrows a
            // handler exception, and the frame is then destroyed with the slot on this
            // same Worker.
            slot.pending_result = slot.task.takeResult();
            slot.task = {};
            slot.pending_command.reset();
            slot.stage = PlayerActorSlot::Stage::Reserving;

            const std::size_t required_slots = _effects.requiredSlots(slot.pending_result);
            if (!_outbound.canEverReserve(required_slots))
            {
                // More than one connection may ever hold. Waiting would never end and
                // throwing would take down every actor this Worker owns, so the command
                // ends here and the backend closes the connection.
                return abandonEmission(slot);
            }

            if (auto reservation = _outbound.tryReserve(slot.connection, required_slots))
            {
                // Outside saturation this is the whole story: no operation is begun, so
                // no in-flight slot, no continuation and no suspension.
                return emit(slot, *reservation, stop_token);
            }

            slot.reservation_task =
                awaitOutboundReservation(_outbound, context, slot.connection, required_slots);
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
            return abandonEmission(slot);
        }
        catch (const snf::runtime::AsyncOperationCancelled&)
        {
            return abandonEmission(slot);
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

            throw std::runtime_error{
                "Outbound channel was cancelled while the logic runtime was active"};
        }

        return emit(slot, reservation, stop_token);
    }

    snf::runtime::ActorDispatchResult PlayerActorBinding::emit(PlayerActorSlot& slot,
                                                               OutboundReservation& reservation,
                                                               const std::stop_token stop_token)
    {
        PlayerResult result = std::move(slot.pending_result);
        const snf::net::ConnectionId connection = slot.connection;
        resetPendingCommand(slot);

        if (_effects.commit(connection, std::move(result), reservation))
        {
            return snf::runtime::ActorDispatchResult::KeepActive;
        }

        if (stop_token.stop_requested())
        {
            return snf::runtime::ActorDispatchResult::Stopped;
        }

        throw std::runtime_error{"Player effect emission failed while logic runtime was active"};
    }

    snf::runtime::ActorDispatchResult
    PlayerActorBinding::abandonEmission(PlayerActorSlot& slot) noexcept
    {
        slot.reservation_task = {};
        resetPendingCommand(slot);
        _outbound.reportAdmissionFailure(slot.connection);
        return snf::runtime::ActorDispatchResult::KeepActive;
    }

    void PlayerActorBinding::resetPendingCommand(PlayerActorSlot& slot) noexcept
    {
        slot.pending_result = PlayerResult{};
        slot.stage = PlayerActorSlot::Stage::Idle;
    }
}
