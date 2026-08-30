#include "snf/worker/worker.hpp"

#include "snf/net/socket_options.hpp"
#include "snf/net/system_error.hpp"
#include "snf/worker/stale.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <sys/socket.h>
#include <system_error>
#include <thread>
#include <utility>
#include <variant>

namespace
{
    constexpr std::size_t MAX_RECEIVE_CHUNK = 64ull * 1024;

    void discardFrame(snf::protocol::Frame&& frame)
    {
        snf::protocol::Frame discarded = std::move(frame);
        static_cast<void>(discarded);
    }

    [[nodiscard]] bool budgetExpired(const std::chrono::steady_clock::time_point started_at, const std::chrono::nanoseconds maximum_duration) noexcept
    {
        return std::chrono::steady_clock::now() - started_at >= maximum_duration;
    }

    [[nodiscard]] bool exceedsByteLimit(
        const std::uint64_t current,
        const std::uint64_t addition,
        const std::uint64_t limit
    ) noexcept
    {
        return addition > limit || current > limit - addition;
    }
}

namespace snf::worker
{
    Worker::Worker(const WorkerId id, const std::uint16_t worker_count, const WorkerBudgets budgets, const WorkerInboxConfig inbox_config)
        : _id(id)
        , _worker_count(worker_count)
        , _budgets(budgets)
        , _poller(budgets.poll.max_poll_events)
        , _inbox(worker_count, _wakeup, inbox_config)
        , _remote_ports(worker_count)
    {
        if (!isValid(_budgets))
        {
            throw std::invalid_argument{"Invalid Worker budget configuration"};
        }
        _poller.add(_wakeup.descriptor(), PollToken{PollTargetKind::Wakeup, 0, 0}, PollInterest{.read = true, .write = false});
    }

    Worker::Worker(
        const WorkerId id,
        const std::uint16_t worker_count,
        const WorkerBudgets budgets,
        const WorkerInboxConfig inbox_config,
        const WorkerNetworkConfig network_config,
        RequestSink& request_sink
    )
        : Worker(id, worker_count, budgets, inbox_config)
    {
        configureNetwork(network_config, request_sink);
    }

    Worker::Worker(
        const WorkerId id,
        const std::uint16_t worker_count,
        const WorkerBudgets budgets,
        const WorkerInboxConfig inbox_config,
        const WorkerActorConfig actor_config,
        ActorFactory& actor_factory
    )
        : Worker(id, worker_count, budgets, inbox_config)
    {
        configureActors(actor_config, actor_factory);
    }

    Worker::Worker(
        const WorkerId id,
        const std::uint16_t worker_count,
        const WorkerBudgets budgets,
        const WorkerInboxConfig inbox_config,
        const WorkerNetworkConfig network_config,
        RequestSink& request_sink,
        const WorkerActorConfig actor_config,
        ActorFactory& actor_factory
    )
        : Worker(id, worker_count, budgets, inbox_config, network_config, request_sink)
    {
        configureActors(actor_config, actor_factory);
    }

    void Worker::run()
    {
        bindOwnerThread();

        while (!_stop_requested.load(std::memory_order_acquire))
        {
            ++_metrics.loop_iterations;
            const auto timeout = hasRunnableWork() ? std::chrono::milliseconds(0) : pollTimeout();
            const auto events = _poller.wait(timeout);

            processPollEvents(events, _budgets.poll);
            drainInbox(_budgets.inbox);
            expireTimers(std::chrono::steady_clock::now(), _budgets.timers);
            runReadyActors(_budgets.actors);
            flushWrites(_budgets.writes);
        }

        runUnifiedShutdown();
    }

    void Worker::requestStop() noexcept
    {
        _stop_requested.store(true, std::memory_order_release);
        _wakeup.notify();
    }

    void Worker::configureNetwork(const WorkerNetworkConfig& config, RequestSink& request_sink)
    {
        assertOwnerThread();
        if (_connections != nullptr)
        {
            throw std::logic_error{"Worker network can only be configured once"};
        }
        if (!isValid(config))
        {
            throw std::invalid_argument{"Invalid worker network configuration"};
        }

        _connections = std::make_unique<ConnectionTable>(config.table);
        _registrations = std::make_unique<PollRegistrationTable>(config.poll_registration_capacity);
        _read_work_queue = std::make_unique<ReadWorkQueue>(config.table.capacity);
        _write_work_queue = std::make_unique<WriteWorkQueue>(config.table.capacity);
        _connection_registrations.resize(config.table.capacity);
        _network_config = config;
        _request_sink = &request_sink;
    }

    void Worker::configureActors(const WorkerActorConfig& config, ActorFactory& factory)
    {
        assertOwnerThread();
        if (_actors != nullptr)
        {
            throw std::logic_error{"Worker actors can only be configured once"};
        }
        if (!isValid(config))
        {
            throw std::invalid_argument{"Invalid worker actor configuration"};
        }

        _actors = std::make_unique<ActorTable>(config.actor_table_capacity);
        _ready_queue = std::make_unique<ReadyActorQueue>(config.actor_table_capacity);
        _actor_factory = &factory;
        _actor_config = config;
        _timers.setMaxApplicationTimerBytes(config.max_application_timer_bytes_total);
    }

    void Worker::attachListener(snf::net::UniqueFileDescriptor listener)
    {
        assertOwnerThread();
        if (!networkEnabled() || !listener.isValid() || _listener.isValid() || _listener_registration.has_value())
        {
            throw std::logic_error{"Invalid worker listener attachment"};
        }

        auto reservation = _registrations->tryReserve(listener.getDescriptor(), PollTargetKind::Listener);
        if (!reservation)
        {
            throw std::invalid_argument{"Poll registration capacity cannot hold the worker listener"};
        }

        const PollToken token = reservation->token();
        _poller.add(listener.getDescriptor(), token, PollInterest{.read = true, .write = false});
        const PollRegistrationHandle handle = reservation->handle();
        reservation->commit();
        _listener = std::move(listener);
        _listener_registration = handle;
    }

    void Worker::bindRemoteTarget(const WorkerId target, WorkerInboxPort port)
    {
        assertOwnerThread();
        if (target.value >= _worker_count || !port.isBound())
        {
            throw std::invalid_argument{"Invalid remote Worker inbox target"};
        }
        if (_remote_ports[target.value].isBound())
        {
            throw std::invalid_argument{"Remote Worker inbox target is already bound"};
        }
        _remote_ports[target.value] = std::move(port);
    }

    void Worker::attachBarrier(WorkerQuiescenceBarrier* barrier) noexcept
    {
        _barrier = barrier;
    }

    bool Worker::hasBlockedActors() const noexcept
    {
        if (_actors == nullptr)
        {
            return false;
        }
        const auto handles = _actors->activeHandles();
        for (const auto handle : handles)
        {
            const ActorSlot* slot = _actors->find(handle);
            if (slot != nullptr && slot->hasBlocked())
            {
                return true;
            }
        }
        return false;
    }

    SendResult Worker::send(const ConnectionRef connection, snf::protocol::Frame&& frame, const bool critical)
    {
        assertOwnerThread();
        if (!networkEnabled() || connection.owner.value >= _worker_count)
        {
            discardFrame(std::move(frame));
            return SendResult::Rejected;
        }

        if (connection.owner == _id)
        {
            return sendLocal(connection, std::move(frame), critical);
        }

        if (frame.payload.size() > snf::protocol::MAX_PAYLOAD_SIZE)
        {
            discardFrame(std::move(frame));
            return SendResult::Rejected;
        }

        WorkerInboxPort& port = _remote_ports[connection.owner.value];
        if (!port.isBound())
        {
            discardFrame(std::move(frame));
            return SendResult::Rejected;
        }

        RemoteConnectionSend remote{
            .connection = connection,
            .frame = std::move(frame),
            .critical = critical,
        };
        const std::uint32_t charge = remoteSendCharge(remote.frame);
        WorkerEnvelope envelope{
            .event = std::move(remote),
            .charged_bytes = charge,
        };
        const InboxPushResult result = port.tryPush(std::move(envelope));
        if (result == InboxPushResult::Accepted)
        {
            if (_barrier != nullptr)
            {
                _barrier->notePublished(connection.owner);
            }
            return SendResult::Accepted;
        }
        return result == InboxPushResult::Closed ? SendResult::Closing : SendResult::Rejected;
    }

    bool Worker::closeConnection(const ConnectionRef connection, const CloseReason reason, const bool graceful)
    {
        assertOwnerThread();
        if (!networkEnabled() || connection.owner.value >= _worker_count)
        {
            return false;
        }

        if (connection.owner != _id)
        {
            WorkerInboxPort& port = _remote_ports[connection.owner.value];
            if (!port.isBound())
            {
                return false;
            }
            WorkerEnvelope envelope{
                .event = RemoteConnectionClose{.connection = connection, .reason = reason, .graceful = graceful},
                .charged_bytes = static_cast<std::uint32_t>(sizeof(RemoteConnectionClose)),
            };
            const bool accepted = port.tryPush(std::move(envelope)) == InboxPushResult::Accepted;
            if (accepted && _barrier != nullptr)
            {
                _barrier->notePublished(connection.owner);
            }
            return accepted;
        }

        const ConnectionHandle handle{.id = connection.id, .generation = connection.generation};
        if (!isCurrent(handle))
        {
            return false;
        }
        return graceful ? beginGracefulClose(handle, reason) : (forceClose(handle, reason), true);
    }

    DeliveryResult Worker::tell(const ActorKey key, ActorEnvelope envelope)
    {
        assertOwnerThread();
        if (_shutting_down || _stop_requested.load(std::memory_order_acquire) || !actorsConfigured())
        {
            return DeliveryResult::Closed;
        }
        return tellInternal(key, std::move(envelope), false);
    }

    DeliveryResult Worker::tellInternal(const ActorKey key, ActorEnvelope envelope, const bool allow_quiescing)
    {
        assertOwnerThread();
        if (!actorsConfigured())
        {
            return DeliveryResult::Closed;
        }

        if (_shutting_down && !allow_quiescing)
        {
            return DeliveryResult::Closed;
        }

        const WorkerId owner = ownerOf(key, _worker_count, _actor_config.placement_seed);
        if (owner == _id)
        {
            return tryDeliverLocalInternal(key, std::move(envelope));
        }

        if (owner.value >= _remote_ports.size() || !_remote_ports[owner.value].isBound())
        {
            ++_metrics.actor.remote_tell_rejections;
            return DeliveryResult::Closed;
        }

        const std::uint64_t charge = envelope.chargedBytes();
        if (charge > std::numeric_limits<std::uint32_t>::max())
        {
            ++_metrics.actor.remote_tell_rejections;
            return DeliveryResult::RemoteInboxFull;
        }

        WorkerEnvelope worker_envelope{
            .event = RemoteActorMessage{
                .target = key,
                .message = std::move(envelope),
            },
            .charged_bytes = static_cast<std::uint32_t>(charge),
        };

        WorkerInboxPort& port = _remote_ports[owner.value];
        const InboxPushResult push_result = port.tryPush(std::move(worker_envelope));
        if (push_result == InboxPushResult::Accepted)
        {
            ++_metrics.actor.remote_tells_sent;
            if (_barrier != nullptr)
            {
                _barrier->notePublished(owner);
            }
            return DeliveryResult::Accepted;
        }
        if (push_result == InboxPushResult::Full)
        {
            ++_metrics.actor.remote_tell_rejections;
            return DeliveryResult::RemoteInboxFull;
        }
        ++_metrics.actor.remote_tell_rejections;
        return DeliveryResult::Closed;
    }

    std::optional<TimerReservation> Worker::tryReserve(const std::uint64_t charged_bytes, const std::uint64_t turn_id) noexcept
    {
        assertOwnerThread();
        if (_shutting_down || _network_stopping)
        {
            return std::nullopt;
        }
        if (!_timers.tryReserveApplicationTimer(charged_bytes))
        {
            return std::nullopt;
        }
        return TimerReservation(this, charged_bytes, turn_id);
    }

    void Worker::releaseReservation(const std::uint64_t charged_bytes) noexcept
    {
        _timers.releaseApplicationTimerReservation(charged_bytes);
    }

    DeliveryResult Worker::tryDeliverLocal(const ActorKey key, ActorEnvelope envelope)
    {
        assertOwnerThread();
        if (_shutting_down || _stop_requested.load(std::memory_order_acquire) || !actorsConfigured())
        {
            return DeliveryResult::Closed;
        }
        return tryDeliverLocalInternal(key, std::move(envelope));
    }

    DeliveryResult Worker::tryDeliverLocalInternal(const ActorKey key, ActorEnvelope envelope)
    {
        assertOwnerThread();
        if (!actorsConfigured())
        {
            return DeliveryResult::Closed;
        }

        if (ownerOf(key, _worker_count, _actor_config.placement_seed) != _id)
        {
            ++_metrics.actor.wrong_owner_tells;
            return DeliveryResult::WrongOwner;
        }

        ActorSlot* slot = _actors->find(key);
        const std::uint64_t charge = envelope.chargedBytes();
        if (slot != nullptr)
        {
            if (slot->state() == ActorState::Stopping)
            {
                return DeliveryResult::Stopping;
            }

            if (slot->mailbox().size() >= _actor_config.max_mailbox_messages_per_actor ||
                exceedsByteLimit(slot->mailbox().chargedBytes(), charge, _actor_config.max_mailbox_bytes_per_actor) ||
                _total_mailbox_messages >= _actor_config.max_mailbox_messages_total ||
                exceedsByteLimit(_total_mailbox_bytes, charge, _actor_config.max_mailbox_bytes_total))
            {
                return DeliveryResult::MailboxFull;
            }

            const bool was_idle = (slot->state() == ActorState::Idle);
            slot->mailbox().push(std::move(envelope));
            _total_mailbox_messages += 1;
            _total_mailbox_bytes += charge;

            if (was_idle)
            {
                slot->setState(ActorState::Queued);
                _ready_queue->push(slot->handle());
            }
            return DeliveryResult::Accepted;
        }

        // Missing actor immediate synchronous activation
        if (!_actors->hasCapacity())
        {
            return DeliveryResult::ActorTableFull;
        }

        if (charge > _actor_config.max_mailbox_bytes_per_actor ||
            _total_mailbox_messages >= _actor_config.max_mailbox_messages_total ||
            exceedsByteLimit(_total_mailbox_bytes, charge, _actor_config.max_mailbox_bytes_total))
        {
            return DeliveryResult::MailboxFull;
        }

        auto reservation = _actors->tryReserve(key);
        if (!reservation)
        {
            return DeliveryResult::ActorTableFull;
        }

        ActorConstructionResult construction_result;
        try
        {
            construction_result = _actor_factory->construct(key);
        }
        catch (...)
        {
            reservation->rollback();
            throw;
        }

        if (construction_result.isRejected())
        {
            ++_metrics.actor.construction_rejections;
            reservation->rollback();
            return DeliveryResult::ConstructionRejected;
        }
        if (!construction_result.isReady())
        {
            throw std::logic_error{"Actor factory returned an invalid Ready result"};
        }

        ActorSlot& reserved_slot = reservation->slot();
        reserved_slot.setInstance(std::move(construction_result.instance));
        reserved_slot.mailbox().push(std::move(envelope));
        _total_mailbox_messages += 1;
        _total_mailbox_bytes += charge;
        reserved_slot.setState(ActorState::Queued);

        const ActorHandle handle = reservation->handle();
        reservation->commit();
        _ready_queue->push(handle);
        return DeliveryResult::Accepted;
    }

    bool Worker::networkEnabled() const noexcept
    {
        return _connections != nullptr;
    }

    std::size_t Worker::connectionCount() const noexcept
    {
        return _connections == nullptr ? 0 : _connections->activeCount();
    }

    bool Worker::listenerPaused() const noexcept
    {
        return _listener_paused;
    }

    bool Worker::actorsConfigured() const noexcept
    {
        return _actors != nullptr;
    }

    std::size_t Worker::actorCount() const noexcept
    {
        return _actors == nullptr ? 0 : _actors->activeCount();
    }

    std::size_t Worker::loadingCount() const noexcept
    {
        return _loading_count;
    }

    std::size_t Worker::totalMailboxMessages() const noexcept
    {
        return _total_mailbox_messages;
    }

    std::uint64_t Worker::totalMailboxBytes() const noexcept
    {
        return _total_mailbox_bytes;
    }

    WorkerInboxPort Worker::bindInboxSource(const WorkerId source) noexcept
    {
        return _inbox.bindSource(source);
    }

    void Worker::setEventHandler(EventHandler handler)
    {
        _event_handler = std::move(handler);
    }

    void Worker::setTimerHandler(TimerHandler handler)
    {
        _timer_handler = std::move(handler);
    }

    bool Worker::trySchedule(const TimePoint deadline, TimerPayload payload)
    {
        assertOwnerThread();
        return _timers.trySchedule(deadline, std::move(payload));
    }

    const WorkerMetrics& Worker::metrics() const noexcept
    {
        return _metrics;
    }

    WorkerId Worker::id() const noexcept
    {
        return _id;
    }

    void Worker::bindOwnerThread() noexcept
    {
        _owner_thread = std::this_thread::get_id();
    }

    void Worker::assertOwnerThread() const noexcept
    {
#ifndef NDEBUG
        assert(_owner_thread == std::thread::id{} || _owner_thread == std::this_thread::get_id());
#endif
    }

    bool Worker::hasRunnableWork() const noexcept
    {
        return _inbox_has_more || _timers_have_due || (_read_work_queue != nullptr && !_read_work_queue->empty()) ||
               (_write_work_queue != nullptr && !_write_work_queue->empty()) ||
               (_ready_queue != nullptr && !_ready_queue->empty());
    }

    std::optional<std::chrono::milliseconds> Worker::pollTimeout() const
    {
        const auto next_deadline = _timers.nextDeadline();
        if (!next_deadline.has_value())
        {
            return _budgets.max_poll_timeout;
        }

        const auto now = std::chrono::steady_clock::now();
        if (*next_deadline <= now)
        {
            return std::chrono::milliseconds(0);
        }

        const auto diff = std::chrono::ceil<std::chrono::milliseconds>(*next_deadline - now);
        return std::min(diff, _budgets.max_poll_timeout);
    }

    void Worker::processPollEvents(const std::span<const PollEvent> events, const IoBudget& budget)
    {
        assertOwnerThread();
        _metrics.poll_events += events.size();

        const auto started_at = std::chrono::steady_clock::now();
        std::size_t processed = 0;
        std::size_t accepted = 0;
        for (const PollEvent& event : events)
        {
            if (processed >= budget.max_poll_events || budgetExpired(started_at, budget.max_duration))
            {
                break;
            }
            ++processed;

            if (event.token.kind == PollTargetKind::Wakeup)
            {
                _wakeup.consume();
                ++_metrics.wakeups_consumed;
                continue;
            }

            if (!networkEnabled())
            {
                continue;
            }

            if (event.token.kind == PollTargetKind::Listener)
            {
                if (!_network_stopping && event.readable)
                {
                    acceptPendingClients(budget, started_at, accepted);
                }
                continue;
            }

            if (event.token.kind != PollTargetKind::ClientConnection)
            {
                continue;
            }

            const auto registration = _registrations->lookup(event.token);
            if (!registration || !registration->connection)
            {
                ++_metrics.network.stale_poll_events;
                continue;
            }

            const ConnectionHandle handle = *registration->connection;
            ConnectionSlot* slot = _connections->find(handle);
            if (slot == nullptr || slot->descriptor() != registration->descriptor)
            {
                ++_metrics.network.stale_poll_events;
                continue;
            }

            if (event.error || event.hangup)
            {
                forceClose(handle, event.fatal_error ? CloseReason::IoError : CloseReason::PeerClosed);
                continue;
            }

            if (event.writable && slot->waitingEpollout())
            {
                slot->setWaitingEpollout(false);
                ++_metrics.network.epollout_resumes;
                updateConnectionInterest(*slot);
                if (!slot->writeQueued() && !enqueueWrite(*slot))
                {
                    forceClose(handle, CloseReason::SlowConsumer);
                    continue;
                }
            }

            slot = _connections->find(handle);
            if (slot == nullptr)
            {
                continue;
            }

            if (!_network_stopping && slot->isOpen() && event.readable && !slot->readQueued() && !enqueueRead(*slot))
            {
                forceClose(handle, CloseReason::Overload);
            }
        }

        if (!budgetExpired(started_at, budget.max_duration))
        {
            processReadQueue(budget, started_at);
        }
    }

    void Worker::processReadQueue(const IoBudget& budget, const TimePoint phase_started_at)
    {
        if (!networkEnabled() || _network_stopping)
        {
            return;
        }

        std::uint64_t bytes_read = 0;
        std::size_t frames_decoded = 0;
        std::size_t processed = 0;
        bool phase_budget_exhausted = false;

        while (!_read_work_queue->empty())
        {
            if (processed >= budget.max_read_connections || frames_decoded >= budget.max_frames || bytes_read >= budget.max_read_bytes ||
                budgetExpired(phase_started_at, budget.max_duration))
            {
                phase_budget_exhausted = true;
                break;
            }

            const auto item = _read_work_queue->tryPop();
            if (!item)
            {
                break;
            }

            const ConnectionHandle handle = *item;
            ConnectionSlot* slot = _connections->find(handle);
            if (slot == nullptr)
            {
                ++_metrics.network.stale_work_items;
                continue;
            }

            slot->setReadQueued(false);
            bool item_budget_exhausted = false;
            const bool needs_requeue = readConnection(handle, budget, phase_started_at, bytes_read, frames_decoded, item_budget_exhausted);
            ++processed;

            if (needs_requeue)
            {
                ConnectionSlot* current = _connections->find(handle);
                if (current != nullptr && current->isOpen() && !enqueueRead(*current))
                {
                    forceClose(handle, CloseReason::Overload);
                }
            }

            if (item_budget_exhausted)
            {
                phase_budget_exhausted = true;
                break;
            }
        }

        if (phase_budget_exhausted)
        {
            ++_metrics.network.read_budget_stops;
        }
    }

    bool Worker::readConnection(
        const ConnectionHandle handle,
        const IoBudget& budget,
        const TimePoint phase_started_at,
        std::uint64_t& bytes_read,
        std::size_t& frames_decoded,
        bool& budget_exhausted
    )
    {
        std::array<std::byte, MAX_RECEIVE_CHUNK> receive_buffer{};

        while (true)
        {
            ConnectionSlot* slot = _connections->find(handle);
            if (slot == nullptr || !slot->isOpen())
            {
                return false;
            }

            if (frames_decoded >= budget.max_frames || budgetExpired(phase_started_at, budget.max_duration))
            {
                budget_exhausted = true;
                return true;
            }

            auto decoded = slot->decoder().tryDecodeNext();
            if (decoded.error)
            {
                ++_metrics.network.protocol_errors;
                forceClose(handle, CloseReason::ProtocolViolation);
                return false;
            }
            if (decoded.frame)
            {
                ++frames_decoded;
                ++_metrics.network.received_frames;
                const ConnectionRef reference = slot->reference();
                const RequestPostResult post_result = _request_sink->tryPost(reference, std::move(*decoded.frame));
                if (post_result == RequestPostResult::Accepted)
                {
                    continue;
                }

                ++_metrics.network.rejected_requests;
                if (post_result == RequestPostResult::Invalid)
                {
                    ++_metrics.network.protocol_errors;
                    forceClose(handle, CloseReason::ProtocolViolation);
                }
                else
                {
                    forceClose(handle, CloseReason::Overload);
                }
                return false;
            }

            if (bytes_read >= budget.max_read_bytes)
            {
                budget_exhausted = true;
                return true;
            }

            if (slot->bufferedByteCount() >= slot->maxReadBufferBytes())
            {
                ++_metrics.network.protocol_errors;
                forceClose(handle, CloseReason::ProtocolViolation);
                return false;
            }

            const std::size_t room = slot->maxReadBufferBytes() - slot->bufferedByteCount();
            const std::size_t remaining_budget = static_cast<std::size_t>(budget.max_read_bytes - bytes_read);
            const std::size_t receive_size = std::min({room, remaining_budget, _network_config.receive_chunk_bytes, receive_buffer.size()});
            if (receive_size == 0)
            {
                budget_exhausted = true;
                return true;
            }

            const ssize_t received = ::recv(slot->descriptor(), receive_buffer.data(), receive_size, 0);
            if (received > 0)
            {
                bytes_read += static_cast<std::uint64_t>(received);
                slot->decoder().push(std::span<const std::byte>{receive_buffer.data(), static_cast<std::size_t>(received)});
                continue;
            }

            if (received == 0)
            {
                forceClose(handle, CloseReason::PeerClosed);
                return false;
            }

            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return false;
            }

            forceClose(handle, CloseReason::IoError);
            return false;
        }
    }

    void Worker::acceptPendingClients(const IoBudget& budget, const TimePoint phase_started_at, std::size_t& accepted)
    {
        if (!networkEnabled() || !_listener.isValid() || _network_stopping || _listener_paused)
        {
            return;
        }

        const std::size_t accept_limit = std::min(_network_config.max_accepts_per_poll, budget.max_accepts);

        while (accepted < accept_limit)
        {
            if (budgetExpired(phase_started_at, budget.max_duration))
            {
                return;
            }

            if (!_connections->hasCapacity() || !_registrations->hasCapacity())
            {
                pauseListener();
                return;
            }

            const int client_descriptor = ::accept4(_listener.getDescriptor(), nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (client_descriptor == -1)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    return;
                }
                if (errno == EINTR)
                {
                    continue;
                }
                snf::net::throw_system_error("accept4");
            }

            snf::net::UniqueFileDescriptor client_socket{client_descriptor};
            snf::net::enable_tcp_no_delay(client_descriptor);
            if (_network_config.client_send_buffer_size)
            {
                snf::net::set_socket_send_buffer_size(client_descriptor, *_network_config.client_send_buffer_size);
            }

            auto connection_reservation = _connections->tryReserve(std::move(client_socket), _id);
            if (!connection_reservation)
            {
                pauseListener();
                return;
            }

            const ConnectionHandle handle = connection_reservation->handle();
            auto registration_reservation = _registrations->tryReserve(client_descriptor, PollTargetKind::ClientConnection, handle);
            if (!registration_reservation)
            {
                pauseListener();
                return;
            }

            bool epoll_added = false;
            try
            {
                _poller.add(client_descriptor, registration_reservation->token(), PollInterest{.read = true, .write = false});
                epoll_added = true;
                const PollRegistrationHandle registration_handle = registration_reservation->handle();
                registration_reservation->commit();
                connection_reservation->commit();
                _connection_registrations[handle.id.value] = registration_handle;
            }
            catch (...)
            {
                if (epoll_added)
                {
                    try
                    {
                        _poller.remove(client_descriptor);
                    }
                    catch (...)
                    {
                    }
                }
                throw;
            }

            ++accepted;
            ++_metrics.network.accepted_connections;

            if (budgetExpired(phase_started_at, budget.max_duration))
            {
                break;
            }
        }
    }

    void Worker::drainInbox(const InboxBudget& budget)
    {
        assertOwnerThread();
        const auto res = _inbox.drain(
            budget,
            [this](WorkerEvent&& ev)
            {
                onEvent(std::move(ev));
            }
        );
        _metrics.inbox_events += res.processed;
        _inbox_has_more = res.has_more;
        if (res.budget_exhausted)
        {
            ++_metrics.inbox_budget_stops;
        }
    }

    void Worker::expireTimers(const TimePoint now, const CountTimeBudget& budget)
    {
        assertOwnerThread();
        const auto res = _timers.expire(
            now,
            budget,
            [this](TimerPayload&& payload)
            {
                onTimer(std::move(payload));
            }
        );
        _metrics.timers_fired += res.expired;
        _timers_have_due = res.due_items_remain;
        if (res.budget_exhausted)
        {
            ++_metrics.timer_budget_stops;
        }
    }

    void Worker::runReadyActors(const CountTimeBudget& budget)
    {
        assertOwnerThread();
        if (!actorsConfigured() || _ready_queue->empty())
        {
            return;
        }

        const auto phase_started_at = std::chrono::steady_clock::now();
        std::size_t turns_executed = 0;
        bool phase_budget_exhausted = false;

        while (!_ready_queue->empty())
        {
            if (turns_executed >= budget.max_count || budgetExpired(phase_started_at, budget.max_duration))
            {
                phase_budget_exhausted = true;
                break;
            }

            const ActorHandle handle = _ready_queue->pop();
            ActorSlot* slot = _actors->find(handle);
            if (slot == nullptr || slot->incarnation() != handle.incarnation)
            {
                ++_metrics.actor.stale_ready_handles;
                continue;
            }

            if (slot->state() != ActorState::Queued)
            {
                continue;
            }

            // Check if this slot has a ready suspended continuation
            if (slot->hasBlocked() && std::holds_alternative<SyntheticSuspendedCommand>(*slot->blocked()))
            {
                auto& suspended_cmd = std::get<SyntheticSuspendedCommand>(*slot->blocked());
                if (!suspended_cmd.completion.has_value())
                {
                    // Queued always implies a ready completion: tryMarkSyntheticCommandReady() is the
                    // only path that queues a blocked actor, and it stores the completion first.
                    // Skipping here would consume the ready-queue entry and strand the actor in Queued.
                    throw std::logic_error{"Queued actor is blocked without a ready completion"};
                }

                // 1. Move SyntheticSuspendedCommand to local
                SyntheticSuspendedCommand command = std::move(suspended_cmd);
                const SyntheticAwaitOutcome outcome = *command.completion;

                // 2. Clear blocked to prepare slot for potential re-suspension
                slot->clearBlocked();

                // 3. Set Running
                slot->setState(ActorState::Running);

                // 4. Resume coroutine
                ActorTask task = std::move(command.task);
                const auto slice_started_at = std::chrono::steady_clock::now();
                ++turns_executed;
                ++_metrics.actor.actor_turns;
                ++_metrics.actor.resumed_turns;

                // A throwing continuation propagates out of the Worker loop, matching step 4's
                // dispatch policy. The slot is deliberately left Running and unreferenced: the
                // Worker is terminating, and re-queueing a faulted actor would hide the fault.
                const ActorTaskStatus status = task.resume(outcome);

                const auto slice_duration =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - slice_started_at);
                _metrics.actor.total_slice_duration_ns += static_cast<std::uint64_t>(slice_duration.count());
                if (slice_duration > _metrics.actor.max_slice_duration)
                {
                    _metrics.actor.max_slice_duration = slice_duration;
                }

                if (status == ActorTaskStatus::Suspended)
                {
                    const OperationId op = _operation_ids.next();
                    const AwaitKey await_key{
                        .actor = slot->key(),
                        .incarnation = slot->incarnation(),
                        .operation = op,
                    };
                    const auto deadline = std::chrono::steady_clock::now() + _actor_config.await_timeout;

                    if (_shutting_down)
                    {
                        slot->setBlocked(SyntheticSuspendedCommand{
                            .key = await_key,
                            .deadline = deadline,
                            .task = std::move(task),
                            .completion = SyntheticAwaitOutcome::Cancelled,
                        });
                        slot->setState(ActorState::Queued);
                        _ready_queue->push(slot->handle());
                        ++_metrics.actor.suspended_turns;
                        ++_metrics.actor.cancelled_blocked_actors;
                    }
                    else if (!_timers.tryReserve())
                    {
                        slot->setBlocked(SyntheticSuspendedCommand{
                            .key = await_key,
                            .deadline = deadline,
                            .task = std::move(task),
                            .completion = SyntheticAwaitOutcome::Rejected,
                        });
                        slot->setState(ActorState::Queued);
                        _ready_queue->push(slot->handle());
                        ++_metrics.actor.suspended_turns;
                    }
                    else
                    {
                        slot->setBlocked(SyntheticSuspendedCommand{
                            .key = await_key,
                            .deadline = deadline,
                            .task = std::move(task),
                            .completion = std::nullopt,
                        });
                        slot->setState(ActorState::Suspended);
                        _timers.commitReserved(deadline, AwaitTimeout{await_key});
                        ++_metrics.actor.suspended_turns;
                    }
                }
                else
                {
                    CompletedTurn completed = task.takeCompleted();

                    // Destroy the coroutine frame before applying effects. A StopActorEffect removes
                    // the ActorInstance, and a frame may still reference the instance it ran on, so
                    // the frame must never outlive it.
                    task = ActorTask{};

                    bool stopped = false;
                    applyEffectBatch(*slot, std::move(completed.effects), stopped);

                    if (stopped)
                    {
                        removeActor(handle, ActorRemovalReason::Stopped);
                    }
                    else
                    {
                        if (!slot->mailbox().empty())
                        {
                            slot->setState(ActorState::Queued);
                            _ready_queue->push(slot->handle());
                        }
                        else
                        {
                            slot->setState(ActorState::Idle);
                        }
                    }
                }

                if (phase_budget_exhausted)
                {
                    break;
                }
                continue;
            }

            // Suspended actor must never dispatch mailbox messages (INV-04)
            if (slot->hasBlocked())
            {
                continue;
            }

            // A Loading slot has no instance and is never Queued, so it cannot reach dispatch.
            assert(slot->hasInstance());

            slot->setState(ActorState::Running);

            const std::size_t remaining_phase_turns = budget.max_count - turns_executed;
            const std::size_t slice_limit = std::min(_actor_config.max_turns_per_actor_slice, remaining_phase_turns);

            std::size_t slice_turns = 0;
            bool stopped = false;
            bool suspended = false;
            const auto slice_started_at = std::chrono::steady_clock::now();

            while (slice_turns < slice_limit && !slot->mailbox().empty())
            {
                if (budgetExpired(phase_started_at, budget.max_duration))
                {
                    phase_budget_exhausted = true;
                    break;
                }

                ActorEnvelope envelope = slot->mailbox().pop();
                _total_mailbox_messages -= 1;
                _total_mailbox_bytes -= envelope.chargedBytes();

                const ActorTurnContext context{
                    .activation = slot->activationRef(),
                    .now = std::chrono::steady_clock::now(),
                };

                TurnResult result = slot->instance()->dispatch(std::move(envelope), context);
                ++turns_executed;
                ++slice_turns;
                ++_metrics.actor.actor_turns;

                if (std::holds_alternative<SuspendedTurn>(result))
                {
                    auto& suspended_turn = std::get<SuspendedTurn>(result);
                    ActorTask task = std::move(suspended_turn.task);
                    const OperationId op = _operation_ids.next();
                    const AwaitKey await_key{
                        .actor = slot->key(),
                        .incarnation = slot->incarnation(),
                        .operation = op,
                    };
                    const auto deadline = std::chrono::steady_clock::now() + _actor_config.await_timeout;

                    if (_shutting_down)
                    {
                        slot->setBlocked(SyntheticSuspendedCommand{
                            .key = await_key,
                            .deadline = deadline,
                            .task = std::move(task),
                            .completion = SyntheticAwaitOutcome::Cancelled,
                        });
                        slot->setState(ActorState::Queued);
                        _ready_queue->push(slot->handle());
                        ++_metrics.actor.suspended_turns;
                        ++_metrics.actor.cancelled_blocked_actors;
                    }
                    else if (!_timers.tryReserve())
                    {
                        slot->setBlocked(SyntheticSuspendedCommand{
                            .key = await_key,
                            .deadline = deadline,
                            .task = std::move(task),
                            .completion = SyntheticAwaitOutcome::Rejected,
                        });
                        slot->setState(ActorState::Queued);
                        _ready_queue->push(slot->handle());
                        ++_metrics.actor.suspended_turns;
                    }
                    else
                    {
                        slot->setBlocked(SyntheticSuspendedCommand{
                            .key = await_key,
                            .deadline = deadline,
                            .task = std::move(task),
                            .completion = std::nullopt,
                        });
                        slot->setState(ActorState::Suspended);
                        _timers.commitReserved(deadline, AwaitTimeout{await_key});
                        ++_metrics.actor.suspended_turns;
                    }
                    suspended = true;
                    break;
                }

                auto& completed = std::get<CompletedTurn>(result);
                applyEffectBatch(*slot, std::move(completed.effects), stopped);

                if (stopped)
                {
                    break;
                }
            }

            const auto slice_duration =
                std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - slice_started_at);
            _metrics.actor.total_slice_duration_ns += static_cast<std::uint64_t>(slice_duration.count());
            if (slice_duration > _metrics.actor.max_slice_duration)
            {
                _metrics.actor.max_slice_duration = slice_duration;
            }

            if (stopped)
            {
                removeActor(handle, ActorRemovalReason::Stopped);
            }
            else if (!suspended)
            {
                if (!slot->mailbox().empty())
                {
                    slot->setState(ActorState::Queued);
                    _ready_queue->push(slot->handle());
                }
                else
                {
                    slot->setState(ActorState::Idle);
                }
            }

            if (phase_budget_exhausted)
            {
                break;
            }
        }

        if (phase_budget_exhausted)
        {
            ++_metrics.actor.budget_stops;
        }
    }

    void Worker::applyEffectBatch(ActorSlot& current_slot, EffectBatch&& batch, bool& stopped)
    {
        if (batch.size() > EffectBatch::MAX_EFFECTS)
        {
            throw std::logic_error{"EffectBatch capacity exceeded maximum limit of 64"};
        }

        bool has_stop = false;
        bool has_schedule_timer = false;
        for (const auto& effect : batch.effects())
        {
            if (std::holds_alternative<StopActorEffect>(effect))
            {
                has_stop = true;
            }
            else if (std::holds_alternative<ScheduleTimerEffect>(effect))
            {
                has_schedule_timer = true;
            }
        }
        if (has_stop && has_schedule_timer)
        {
            throw std::logic_error{"StopActorEffect and ScheduleTimerEffect cannot coexist in the same EffectBatch"};
        }

        for (const auto& effect : batch.effects())
        {
            if (std::holds_alternative<ScheduleTimerEffect>(effect))
            {
                const auto& timer_effect = std::get<ScheduleTimerEffect>(effect);
                if (timer_effect.reservation.has_value())
                {
                    const auto& res = *timer_effect.reservation;
                    if (!res.isValid() || res.admission() != this)
                    {
                        throw std::logic_error{"TimerReservation is invalid or does not belong to this Worker"};
                    }
                    if (res.chargedBytes() != timer_effect.message.chargedBytes())
                    {
                        throw std::logic_error{"TimerReservation charged bytes does not match message charged bytes"};
                    }
                }
            }
        }

        for (auto& effect : batch.mutableEffects())
        {
            bool halt_batch = false;
            std::visit(
                [this, &current_slot, &stopped, &halt_batch](auto&& concrete_effect)
                {
                    using T = std::decay_t<decltype(concrete_effect)>;
                    if constexpr (std::is_same_v<T, SendFrameEffect>)
                    {
                        const SendResult result = send(concrete_effect.connection, std::move(concrete_effect.frame), concrete_effect.critical);
                        if (result != SendResult::Accepted)
                        {
                            ++_metrics.actor.effect_send_failures;
                        }
                    }
                    else if constexpr (std::is_same_v<T, CloseConnectionEffect>)
                    {
                        const bool closed = closeConnection(concrete_effect.connection, concrete_effect.reason, concrete_effect.graceful);
                        if (!closed)
                        {
                            ++_metrics.actor.effect_close_failures;
                        }
                    }
                    else if constexpr (std::is_same_v<T, TellActorEffect>)
                    {
                        const DeliveryResult result = tellInternal(concrete_effect.target, std::move(concrete_effect.message), true);
                        if (result != DeliveryResult::Accepted)
                        {
                            ++_metrics.actor.effect_tell_failures;
                        }
                    }
                    else if constexpr (std::is_same_v<T, StopActorEffect>)
                    {
                        current_slot.setState(ActorState::Stopping);
                        stopped = true;
                    }
                    else if constexpr (std::is_same_v<T, ScheduleTimerEffect>)
                    {
                        const ActivationRef target = current_slot.activationRef();
                        if (concrete_effect.reservation.has_value())
                        {
                            auto res = std::move(*concrete_effect.reservation);
                            const auto charge = res._charged_bytes;
                            res._admission = nullptr;
                            res._charged_bytes = 0;
                            res._turn_id = 0;
                            _timers.commitReservedApplicationTimer(
                                concrete_effect.deadline,
                                target,
                                std::move(concrete_effect.message),
                                charge
                            );
                            ++_metrics.actor.application_timers_scheduled;
                        }
                        else
                        {
                            if (_shutting_down || _network_stopping)
                            {
                                ++_metrics.actor.timer_schedule_failures;
                                halt_batch = true;
                                return;
                            }
                            if (!_timers.tryScheduleApplicationTimer(
                                    concrete_effect.deadline,
                                    target,
                                    std::move(concrete_effect.message)))
                            {
                                ++_metrics.actor.timer_schedule_failures;
                                halt_batch = true;
                                return;
                            }
                            ++_metrics.actor.application_timers_scheduled;
                        }
                    }
                },
                effect
            );

            if (halt_batch)
            {
                break;
            }
        }
    }

    bool Worker::completeSyntheticCommand(const AwaitKey key, const SyntheticAwaitOutcome outcome)
    {
        if (tryMarkSyntheticCommandReady(key, outcome))
        {
            return true;
        }
        ++_metrics.actor.stale_completions;
        return false;
    }

    bool Worker::tryMarkSyntheticCommandReady(const AwaitKey key, const SyntheticAwaitOutcome outcome)
    {
        assertOwnerThread();
        if (!actorsConfigured())
        {
            return false;
        }

        ActorSlot* slot = _actors->find(key.actor);
        if (slot == nullptr)
        {
            return false;
        }

        if (slot->state() != ActorState::Suspended || !slot->hasBlocked())
        {
            return false;
        }

        if (!std::holds_alternative<SyntheticSuspendedCommand>(*slot->blocked()))
        {
            return false;
        }

        auto& command = std::get<SyntheticSuspendedCommand>(*slot->blocked());
        if (!acceptsCompletion(key, command.key))
        {
            return false;
        }

        if (command.completion.has_value())
        {
            return false;
        }

        command.completion = outcome;
        slot->setState(ActorState::Queued);
        _ready_queue->push(slot->handle());
        return true;
    }

    DeliveryResult Worker::beginActivationLoad(const ActorKey key, ActorEnvelope&& first_message)
    {
        assertOwnerThread();
        if (_shutting_down || !actorsConfigured())
        {
            return DeliveryResult::Closed;
        }

        if (!_actors->hasCapacity())
        {
            return DeliveryResult::ActorTableFull;
        }

        if (_loading_count >= _actor_config.max_concurrent_loading)
        {
            ++_metrics.actor.loading_limit_rejections;
            return DeliveryResult::ActivationLimit;
        }

        // Same admission arithmetic as tryDeliverLocalInternal(): the helper keeps the byte
        // comparison from wrapping, so both entry points enforce the cap identically.
        const std::uint64_t charge = first_message.chargedBytes();
        if (charge > _actor_config.max_mailbox_bytes_per_actor ||
            _total_mailbox_messages >= _actor_config.max_mailbox_messages_total ||
            exceedsByteLimit(_total_mailbox_bytes, charge, _actor_config.max_mailbox_bytes_total))
        {
            return DeliveryResult::MailboxFull;
        }

        // The await timeout slot is part of admitting an activation, so exhausting it rejects the
        // activation rather than reporting a full actor table.
        if (!_timers.tryReserve())
        {
            return DeliveryResult::ActivationLimit;
        }

        auto reservation = _actors->tryReserve(key);
        if (!reservation.has_value())
        {
            _timers.releaseReservation();
            return DeliveryResult::ActorTableFull;
        }

        ActorSlot& slot = reservation->slot();
        slot.setInstance(nullptr);
        slot.setState(ActorState::Loading);

        const AwaitKey await_key{
            .actor = key,
            .incarnation = slot.incarnation(),
            .operation = _operation_ids.next(),
        };
        const auto deadline = std::chrono::steady_clock::now() + _actor_config.await_timeout;
        slot.setBlocked(ActivationLoad{
            .key = await_key,
            .deadline = deadline,
        });

        // Confirm the aggregate accounting only once the enqueue has succeeded. A throwing push
        // would otherwise leave the counters inflated while the Reservation rolls the slot back,
        // and would strand the timer reservation for the lifetime of the Worker.
        try
        {
            slot.mailbox().push(std::move(first_message));
        }
        catch (...)
        {
            _timers.releaseReservation();
            throw;
        }
        _total_mailbox_messages += 1;
        _total_mailbox_bytes += charge;

        reservation->commit();
        _timers.commitReserved(deadline, AwaitTimeout{await_key});
        ++_loading_count;
        ++_metrics.actor.activation_loads_started;
        return DeliveryResult::Accepted;
    }

    void Worker::completeSyntheticActivation(const AwaitKey key, const SyntheticActivationOutcome outcome)
    {
        assertOwnerThread();
        if (!actorsConfigured())
        {
            return;
        }

        ActorSlot* slot = _actors->find(key.actor);
        if (slot == nullptr || slot->state() != ActorState::Loading || !slot->hasBlocked())
        {
            ++_metrics.actor.stale_activation_completions;
            return;
        }

        if (!std::holds_alternative<ActivationLoad>(*slot->blocked()))
        {
            ++_metrics.actor.stale_activation_completions;
            return;
        }

        const auto& load = std::get<ActivationLoad>(*slot->blocked());
        if (!acceptsCompletion(key, load.key))
        {
            ++_metrics.actor.stale_activation_completions;
            return;
        }

        if (outcome == SyntheticActivationOutcome::Ready)
        {
            ActorConstructionResult result;
            try
            {
                result = _actor_factory->construct(key.actor);
            }
            catch (...)
            {
                ++_metrics.actor.activation_load_failures;
                removeActor(slot->handle(), ActorRemovalReason::ActivationRejected);
                throw;
            }

            if (result.status == ActorConstructionResult::Status::Ready && !result.isReady())
            {
                ++_metrics.actor.activation_load_failures;
                removeActor(slot->handle(), ActorRemovalReason::ActivationRejected);
                throw std::logic_error{"Actor factory returned an invalid Ready result"};
            }

            if (result.isReady())
            {
                slot->setInstance(std::move(result.instance));
                slot->clearBlocked();
                assert(_loading_count > 0);
                --_loading_count;

                if (slot->mailbox().empty())
                {
                    slot->setState(ActorState::Idle);
                }
                else
                {
                    slot->setState(ActorState::Queued);
                    _ready_queue->push(slot->handle());
                }
            }
            else
            {
                ++_metrics.actor.activation_load_failures;
                removeActor(slot->handle(), ActorRemovalReason::ActivationRejected);
            }
        }
        else
        {
            ++_metrics.actor.activation_load_failures;
            ActorRemovalReason reason = ActorRemovalReason::ActivationRejected;
            if (outcome == SyntheticActivationOutcome::TimedOut)
            {
                reason = ActorRemovalReason::ActivationTimedOut;
            }
            else if (outcome == SyntheticActivationOutcome::Cancelled)
            {
                reason = ActorRemovalReason::ActivationCancelled;
            }
            removeActor(slot->handle(), reason);
        }
    }

    MailboxUsage Worker::discardMailbox(ActorSlot& slot)
    {
        const MailboxUsage usage = slot.mailboxUsage();
        _total_mailbox_messages -= usage.message_count;
        _total_mailbox_bytes -= usage.charged_bytes;
        _metrics.actor.discarded_mailbox_messages += usage.message_count;
        _metrics.actor.discarded_mailbox_bytes += usage.charged_bytes;
        slot.clearMailbox();
        return usage;
    }

    void Worker::removeActor(const ActorHandle handle, const ActorRemovalReason reason)
    {
        if (_actors == nullptr)
        {
            return;
        }
        ActorSlot* slot = _actors->find(handle);
        if (slot == nullptr || slot->incarnation() != handle.incarnation)
        {
            return;
        }

        if (slot->hasBlocked() && reason == ActorRemovalReason::ShutdownForced)
        {
            ++_metrics.actor.forced_blocked_destructions;
        }

        if (slot->state() == ActorState::Loading)
        {
            assert(_loading_count > 0);
            --_loading_count;
        }

        // 1. slot.clearBlocked() (coroutine frame / activation load destroyed first)
        slot->clearBlocked();

        // 2. discardMailbox(slot)
        static_cast<void>(discardMailbox(*slot));

        // 3. slot.setInstance(nullptr)
        slot->setInstance(nullptr);

        // 4. release
        static_cast<void>(_actors->release(handle));

        if (reason == ActorRemovalReason::Stopped || reason == ActorRemovalReason::ShutdownForced)
        {
            ++_metrics.actor.stopped_actors;
        }
    }

    void Worker::flushWrites(const ByteTimeBudget& budget)
    {
        if (!networkEnabled())
        {
            return;
        }

        const auto started_at = std::chrono::steady_clock::now();
        std::uint64_t bytes_written = 0;
        bool phase_budget_exhausted = false;

        while (!_write_work_queue->empty())
        {
            if (bytes_written >= budget.max_bytes || budgetExpired(started_at, budget.max_duration))
            {
                phase_budget_exhausted = true;
                break;
            }

            const auto item = _write_work_queue->tryPop();
            if (!item)
            {
                break;
            }

            const ConnectionHandle handle = *item;
            ConnectionSlot* slot = _connections->find(handle);
            if (slot == nullptr)
            {
                ++_metrics.network.stale_work_items;
                continue;
            }

            slot->setWriteQueued(false);
            slot->setWaitingEpollout(false);
            bool would_block = false;
            bool io_error = false;

            while (slot->hasPendingWrite())
            {
                if (bytes_written >= budget.max_bytes || budgetExpired(started_at, budget.max_duration))
                {
                    phase_budget_exhausted = true;
                    break;
                }

                const auto pending = slot->pendingWriteBytes();
                const std::uint64_t remaining_budget = budget.max_bytes - bytes_written;
                const std::size_t send_size =
                    static_cast<std::size_t>(std::min<std::uint64_t>(remaining_budget, static_cast<std::uint64_t>(pending.size())));
                const ssize_t sent = ::send(slot->descriptor(), pending.data(), send_size, MSG_NOSIGNAL);
                if (sent > 0)
                {
                    bytes_written += static_cast<std::uint64_t>(sent);
                    if (slot->consumeWrittenBytes(static_cast<std::size_t>(sent)))
                    {
                        ++_metrics.network.sent_frames;
                    }
                    continue;
                }

                if (sent == -1 && errno == EINTR)
                {
                    continue;
                }

                if (sent == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
                {
                    would_block = true;
                    break;
                }

                io_error = true;
                break;
            }

            if (io_error)
            {
                forceClose(handle, CloseReason::IoError);
                continue;
            }

            slot = _connections->find(handle);
            if (slot == nullptr)
            {
                ++_metrics.network.stale_work_items;
                continue;
            }

            if (!slot->hasPendingWrite())
            {
                if (slot->isClosing())
                {
                    forceClose(handle, slot->closeReason());
                }
                else
                {
                    updateConnectionInterest(*slot);
                }
                continue;
            }

            if (would_block)
            {
                ++_metrics.network.epollout_waits;
                slot->setWaitingEpollout(true);
                updateConnectionInterest(*slot);
            }
            else if (phase_budget_exhausted)
            {
                if (!enqueueWrite(*slot))
                {
                    forceClose(handle, CloseReason::SlowConsumer);
                }
            }

            if (phase_budget_exhausted)
            {
                break;
            }
        }

        if (phase_budget_exhausted)
        {
            ++_metrics.network.write_budget_stops;
        }
    }

    void Worker::onEvent(WorkerEvent&& event)
    {
        assertOwnerThread();

        if (std::holds_alternative<RemoteActorMessage>(event))
        {
            auto& remote_actor_msg = std::get<RemoteActorMessage>(event);
            ++_metrics.actor.remote_tells_received;

            if (!actorsConfigured())
            {
                ++_metrics.actor.remote_tell_delivery_failures;
                ++_metrics.actor.actor_events_without_runtime;
                return;
            }

            if (ownerOf(remote_actor_msg.target, _worker_count, _actor_config.placement_seed) != _id)
            {
                ++_metrics.actor.remote_tell_delivery_failures;
                ++_metrics.actor.misrouted_actor_events;
                return;
            }

            const DeliveryResult delivery_result = tryDeliverLocalInternal(remote_actor_msg.target, std::move(remote_actor_msg.message));
            if (delivery_result == DeliveryResult::Accepted)
            {
                ++_metrics.actor.remote_tells_delivered;
            }
            else
            {
                ++_metrics.actor.remote_tell_delivery_failures;
            }
            return;
        }

        if (networkEnabled())
        {
            bool recognized = false;
            std::visit(
                [this, &recognized](auto&& network_event)
                {
                    using Event = std::decay_t<decltype(network_event)>;
                    if constexpr (std::is_same_v<Event, RemoteConnectionSend>)
                    {
                        recognized = true;
                        if (network_event.connection.owner != _id)
                        {
                            ++_metrics.network.misrouted_events;
                            return;
                        }
                        static_cast<void>(sendLocal(network_event.connection, std::move(network_event.frame), network_event.critical));
                    }
                    else if constexpr (std::is_same_v<Event, RemoteConnectionClose>)
                    {
                        recognized = true;
                        if (network_event.connection.owner != _id)
                        {
                            ++_metrics.network.misrouted_events;
                            return;
                        }
                        static_cast<void>(closeConnection(network_event.connection, network_event.reason, network_event.graceful));
                    }
                },
                event
            );

            if (recognized)
            {
                return;
            }
        }

        if (_event_handler)
        {
            _event_handler(std::move(event));
        }
    }

    void Worker::onTimer(TimerPayload&& payload)
    {
        assertOwnerThread();
        if (networkEnabled() && std::holds_alternative<ConnectionCloseDeadline>(payload))
        {
            const ConnectionHandle handle = std::get<ConnectionCloseDeadline>(payload).connection;
            ConnectionSlot* slot = _connections->find(handle);
            if (slot != nullptr && slot->isClosing())
            {
                ++_metrics.network.close_deadline_expirations;
                forceClose(handle, CloseReason::Timeout);
            }
            return;
        }

        if (actorsConfigured() && std::holds_alternative<AwaitTimeout>(payload))
        {
            const AwaitKey key = std::get<AwaitTimeout>(payload).key;
            ActorSlot* slot = _actors->find(key.actor);
            if (slot != nullptr && slot->hasBlocked())
            {
                if (std::holds_alternative<SyntheticSuspendedCommand>(*slot->blocked()))
                {
                    if (!tryMarkSyntheticCommandReady(key, SyntheticAwaitOutcome::TimedOut))
                    {
                        ++_metrics.actor.stale_await_timeouts;
                    }
                }
                else if (std::holds_alternative<ActivationLoad>(*slot->blocked()))
                {
                    // Screen the identity here so a stale timer is counted against the timer
                    // source instead of completeSyntheticActivation()'s completion counter.
                    if (acceptsCompletion(key, std::get<ActivationLoad>(*slot->blocked()).key))
                    {
                        completeSyntheticActivation(key, SyntheticActivationOutcome::TimedOut);
                    }
                    else
                    {
                        ++_metrics.actor.stale_await_timeouts;
                    }
                }
                else
                {
                    ++_metrics.actor.stale_await_timeouts;
                }
            }
            else
            {
                ++_metrics.actor.stale_await_timeouts;
            }
            return;
        }

        if (actorsConfigured() && std::holds_alternative<ApplicationTimer>(payload))
        {
            auto app_timer = std::move(std::get<ApplicationTimer>(payload));
            ActorSlot* slot = _actors->find(app_timer.target.actor);
            if (slot == nullptr || slot->incarnation() != app_timer.target.incarnation)
            {
                ++_metrics.actor.stale_application_timers;
                return;
            }

            if (slot->state() == ActorState::Stopping)
            {
                ++_metrics.actor.application_timer_delivery_failures;
                return;
            }

            const std::uint64_t charge = app_timer.message.chargedBytes();
            if (slot->mailbox().size() >= _actor_config.max_mailbox_messages_per_actor ||
                slot->mailbox().chargedBytes() > _actor_config.max_mailbox_bytes_per_actor ||
                charge > _actor_config.max_mailbox_bytes_per_actor - slot->mailbox().chargedBytes() ||
                _total_mailbox_messages >= _actor_config.max_mailbox_messages_total ||
                _total_mailbox_bytes > _actor_config.max_mailbox_bytes_total ||
                charge > _actor_config.max_mailbox_bytes_total - _total_mailbox_bytes)
            {
                ++_metrics.actor.application_timer_delivery_failures;
                return;
            }

            slot->mailbox().push(std::move(app_timer.message));
            _total_mailbox_messages += 1;
            _total_mailbox_bytes += charge;
            if (slot->state() == ActorState::Idle)
            {
                slot->setState(ActorState::Queued);
                _ready_queue->push(slot->handle());
            }
            ++_metrics.actor.application_timers_delivered;
            return;
        }

        if (_timer_handler)
        {
            _timer_handler(std::move(payload));
        }
    }

    SendResult Worker::sendLocal(const ConnectionRef connection, snf::protocol::Frame&& frame, const bool critical)
    {
        snf::protocol::Frame owned_frame = std::move(frame);
        const ConnectionHandle handle{.id = connection.id, .generation = connection.generation};
        ConnectionSlot* slot = _connections->find(handle);
        if (slot == nullptr)
        {
            return SendResult::Stale;
        }
        if (slot->reference().owner != connection.owner)
        {
            return SendResult::Stale;
        }

        const SendResult result = slot->appendFrame(std::move(owned_frame), critical);
        if (result == SendResult::HardLimit)
        {
            ++_metrics.network.hard_limit_sends;
            forceClose(handle, CloseReason::SlowConsumer);
            return result;
        }
        if (result == SendResult::SoftLimit)
        {
            ++_metrics.network.soft_limit_sends;
            return result;
        }
        if (result != SendResult::Accepted)
        {
            return result;
        }

        if (!slot->writeQueued() && !slot->waitingEpollout() && !enqueueWrite(*slot))
        {
            forceClose(handle, CloseReason::SlowConsumer);
            return SendResult::Rejected;
        }
        return result;
    }

    bool Worker::enqueueRead(ConnectionSlot& slot)
    {
        if (slot.readQueued())
        {
            return true;
        }
        if (!_read_work_queue->tryPush(slot.handle()))
        {
            return false;
        }
        slot.setReadQueued(true);
        return true;
    }

    bool Worker::enqueueWrite(ConnectionSlot& slot)
    {
        if (slot.writeQueued())
        {
            return true;
        }
        if (!_write_work_queue->tryPush(slot.handle()))
        {
            return false;
        }
        slot.setWriteQueued(true);
        return true;
    }

    void Worker::updateConnectionInterest(const ConnectionSlot& slot)
    {
        const auto registration = registrationFor(slot.handle());
        if (!registration)
        {
            networkInvariantViolation("Active connection has no poll registration");
        }
        _poller.modify(slot.descriptor(), registration->token, PollInterest{.read = slot.isOpen(), .write = slot.waitingEpollout()});
    }

    void Worker::forceClose(const ConnectionHandle handle, const CloseReason reason)
    {
        ConnectionSlot* slot = _connections->find(handle);
        if (slot == nullptr)
        {
            return;
        }

        const ConnectionRef reference = slot->reference();
        const bool immediate = slot->isOpen();
        releaseConnectionRegistration(handle);
        static_cast<void>(_connections->release(handle));
        ++_metrics.network.closed_connections;
        if (immediate)
        {
            ++_metrics.network.immediate_closes;
        }

        if (_request_sink != nullptr)
        {
            _request_sink->onConnectionClosed(reference, reason);
        }
        maybeResumeListener();
    }

    bool Worker::beginGracefulClose(
        const ConnectionHandle handle,
        const CloseReason reason,
        const std::optional<TimePoint> max_deadline
    )
    {
        ConnectionSlot* slot = _connections->find(handle);
        if (slot == nullptr)
        {
            return false;
        }
        if (slot->isClosing())
        {
            return true;
        }

        const auto standard_deadline = std::chrono::steady_clock::now() + _network_config.table.limits.close_drain_deadline;
        const auto deadline = max_deadline ? std::min(standard_deadline, *max_deadline) : standard_deadline;
        slot->beginClosing(deadline, reason);
        ++_metrics.network.graceful_closes;
        updateConnectionInterest(*slot);

        if (!slot->hasPendingWrite())
        {
            forceClose(handle, reason);
            return true;
        }

        if (!slot->writeQueued() && !slot->waitingEpollout() && !enqueueWrite(*slot))
        {
            forceClose(handle, CloseReason::SlowConsumer);
            return true;
        }

        if (!_timers.trySchedule(deadline, TimerPayload{ConnectionCloseDeadline{.connection = handle}}))
        {
            forceClose(handle, CloseReason::Timeout);
        }
        return true;
    }

    void Worker::maybeResumeListener()
    {
        if (!_listener_paused || _network_stopping || !_listener.isValid() || !_connections->hasCapacity() || !_registrations->hasCapacity())
        {
            return;
        }

        if (!_listener_registration)
        {
            networkInvariantViolation("Listener has no poll registration while resuming");
        }

        const auto registration = _registrations->lookup(*_listener_registration);
        if (!registration)
        {
            networkInvariantViolation("Listener poll registration is not active while resuming");
        }
        _poller.modify(_listener.getDescriptor(), registration->token, PollInterest{.read = true, .write = false});
        _listener_paused = false;
        ++_metrics.network.listener_resumes;
    }

    void Worker::pauseListener()
    {
        if (_listener_paused || !_listener.isValid())
        {
            return;
        }

        if (!_listener_registration)
        {
            networkInvariantViolation("Listener has no poll registration while pausing");
        }

        const auto registration = _registrations->lookup(*_listener_registration);
        if (!registration)
        {
            networkInvariantViolation("Listener poll registration is not active while pausing");
        }
        _poller.modify(_listener.getDescriptor(), registration->token, PollInterest{.read = false, .write = false});
        _listener_paused = true;
        ++_metrics.network.listener_pauses;
    }

    [[noreturn]] void Worker::networkInvariantViolation(const char* message)
    {
        ++_metrics.network.invariant_violations;
        assert(false && "Worker network invariant violated");
        throw std::logic_error{message};
    }

    void Worker::runUnifiedShutdown()
    {
        const auto shutdown_timeout = (_actors != nullptr)
            ? _actor_config.worker_shutdown_timeout
            : (networkEnabled() ? _network_config.table.limits.close_drain_deadline : std::chrono::seconds(2));
        const auto shutdown_deadline = std::chrono::steady_clock::now() + shutdown_timeout;

        beginShutdownPhaseA();
        runShutdownPhaseB(shutdown_deadline);
        runShutdownPhaseC(shutdown_deadline);
        runShutdownPhaseD(shutdown_deadline);
    }

    void Worker::beginShutdownPhaseA()
    {
        _shutting_down = true;
        _network_stopping = true;

        if (_listener_registration)
        {
            if (_listener.isValid())
            {
                try
                {
                    _poller.remove(_listener.getDescriptor());
                }
                catch (...)
                {
                }
            }
            static_cast<void>(_registrations->release(*_listener_registration));
            _listener_registration.reset();
            _listener.init();
        }

        if (networkEnabled())
        {
            const auto handles = _connections->activeHandles();
            for (const ConnectionHandle handle : handles)
            {
                const ConnectionSlot* slot = _connections->find(handle);
                if (slot == nullptr)
                {
                    continue;
                }
                const auto registration = registrationFor(handle);
                if (registration)
                {
                    try
                    {
                        _poller.modify(slot->descriptor(), registration->token, PollInterest{.read = false, .write = slot->waitingEpollout()});
                    }
                    catch (...)
                    {
                    }
                }
            }

            if (_read_work_queue != nullptr)
            {
                while (const auto read_handle = _read_work_queue->tryPop())
                {
                    ConnectionSlot* slot = _connections->find(*read_handle);
                    if (slot != nullptr)
                    {
                        slot->setReadQueued(false);
                    }
                }
            }
        }
    }

    void Worker::runShutdownPhaseB(const TimePoint deadline)
    {
        const std::size_t cancelled_app_timers = _timers.cancelApplicationTimers();
        _metrics.actor.cancelled_application_timers += cancelled_app_timers;

        if (_barrier == nullptr)
        {
            bool first_iteration = true;
            bool logical_cancel_performed = false;

            while (std::chrono::steady_clock::now() < deadline)
            {
                ++_metrics.shutdown_quiescence_rounds;
                const auto now = std::chrono::steady_clock::now();
                const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - now);
                const auto regular_timeout = (first_iteration || hasRunnableWork())
                    ? std::chrono::milliseconds(0)
                    : pollTimeout().value_or(remaining);
                const auto timeout = std::min(regular_timeout, remaining);
                const auto events = _poller.wait(timeout);

                processPollEvents(events, _budgets.poll);
                drainInbox(_budgets.inbox);
                expireTimers(std::chrono::steady_clock::now(), _budgets.timers);
                runReadyActors(_budgets.actors);
                flushWrites(_budgets.writes);

                first_iteration = false;

                const bool runnable_work_empty =
                    (_ready_queue == nullptr || _ready_queue->empty()) && _inbox.isEmpty() && !_inbox_has_more && !_timers_have_due &&
                    _timers.applicationTimerCount() == 0 && _timers.reservedApplicationTimerBytes() == 0;

                if (runnable_work_empty)
                {
                    if (!logical_cancel_performed && _actors != nullptr && hasBlockedActors())
                    {
                        logical_cancel_performed = true;
                        bool any_cancelled = false;
                        const auto handles = _actors->activeHandles();
                        for (const auto handle : handles)
                        {
                            ActorSlot* slot = _actors->find(handle);
                            if (slot != nullptr && slot->hasBlocked())
                            {
                                if (std::holds_alternative<SyntheticSuspendedCommand>(*slot->blocked()))
                                {
                                    auto& cmd = std::get<SyntheticSuspendedCommand>(*slot->blocked());
                                    if (!cmd.completion.has_value())
                                    {
                                        cmd.completion = SyntheticAwaitOutcome::Cancelled;
                                        slot->setState(ActorState::Queued);
                                        _ready_queue->push(slot->handle());
                                        ++_metrics.actor.cancelled_blocked_actors;
                                        any_cancelled = true;
                                    }
                                }
                                else if (std::holds_alternative<ActivationLoad>(*slot->blocked()))
                                {
                                    ++_metrics.actor.cancelled_blocked_actors;
                                    removeActor(handle, ActorRemovalReason::ActivationCancelled);
                                }
                            }
                        }

                        if (any_cancelled)
                        {
                            continue;
                        }
                    }

                    if (!hasBlockedActors())
                    {
                        break;
                    }
                }
            }
        }
        else
        {
            enum class ShutdownBState { Active, Waiting };
            ShutdownBState state = ShutdownBState::Active;
            bool logical_cancel_performed = false;

            while (std::chrono::steady_clock::now() < deadline)
            {
                ++_metrics.shutdown_quiescence_rounds;
                const auto now = std::chrono::steady_clock::now();

                if (state == ShutdownBState::Active)
                {
                    const auto events = _poller.wait(std::chrono::milliseconds(0));
                    if (!events.empty())
                    {
                        processPollEvents(events, _budgets.poll);
                    }

                    drainInbox(_budgets.inbox);
                    expireTimers(now, _budgets.timers);
                    runReadyActors(_budgets.actors);
                    flushWrites(_budgets.writes);

                    const bool runnable_work =
                        (_ready_queue != nullptr && !_ready_queue->empty()) ||
                        (!_inbox.isEmpty()) ||
                        _inbox_has_more;

                    if (runnable_work)
                    {
                        continue;
                    }

                    if (_actors != nullptr && hasBlockedActors())
                    {
                        if (!logical_cancel_performed)
                        {
                            logical_cancel_performed = true;
                            bool any_cancelled = false;
                            const auto handles = _actors->activeHandles();
                            for (const auto handle : handles)
                            {
                                ActorSlot* slot = _actors->find(handle);
                                if (slot != nullptr && slot->hasBlocked())
                                {
                                    if (std::holds_alternative<SyntheticSuspendedCommand>(*slot->blocked()))
                                    {
                                        auto& cmd = std::get<SyntheticSuspendedCommand>(*slot->blocked());
                                        if (!cmd.completion.has_value())
                                        {
                                            cmd.completion = SyntheticAwaitOutcome::Cancelled;
                                            slot->setState(ActorState::Queued);
                                            _ready_queue->push(slot->handle());
                                            ++_metrics.actor.cancelled_blocked_actors;
                                            any_cancelled = true;
                                        }
                                    }
                                    else if (std::holds_alternative<ActivationLoad>(*slot->blocked()))
                                    {
                                        ++_metrics.actor.cancelled_blocked_actors;
                                        removeActor(handle, ActorRemovalReason::ActivationCancelled);
                                    }
                                }
                            }

                            if (any_cancelled)
                            {
                                continue;
                            }
                        }
                        else
                        {
                            continue;
                        }
                    }

                    state = ShutdownBState::Waiting;
                }

                if (state == ShutdownBState::Waiting)
                {
                    const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
                    const auto wait_dur = std::min(std::chrono::milliseconds(1), std::max(std::chrono::milliseconds(0), remaining));
                    const auto events = _poller.wait(wait_dur);

                    if (!events.empty())
                    {
                        _barrier->markActive(_id);
                        state = ShutdownBState::Active;
                        processPollEvents(events, _budgets.poll);
                        continue;
                    }

                    const auto snap = _barrier->snapshot();
                    if (snap.aborted)
                    {
                        ++_metrics.shutdown_barrier_aborts;
                        break;
                    }

                    const bool has_work =
                        (!_inbox.isEmpty()) ||
                        (_ready_queue != nullptr && !_ready_queue->empty()) ||
                        (_actors != nullptr && hasBlockedActors()) ||
                        (_timers.applicationTimerCount() > 0) ||
                        (_timers.reservedApplicationTimerBytes() > 0);

                    if (has_work)
                    {
                        _barrier->markActive(_id);
                        state = ShutdownBState::Active;
                        continue;
                    }

                    const bool marked = _barrier->tryMarkQuiescent(_id, snap.epoch);
                    if (!marked)
                    {
                        continue;
                    }

                    const auto snap_after = _barrier->snapshot();
                    if (snap_after.aborted)
                    {
                        ++_metrics.shutdown_barrier_aborts;
                        break;
                    }

                    if (snap_after.all_quiescent)
                    {
                        const auto snap_verify = _barrier->snapshot();
                        if (snap_verify.all_quiescent && snap_verify.epoch == snap_after.epoch && !snap_verify.aborted)
                        {
                            break;
                        }
                    }
                }
            }

            if (_barrier != nullptr && std::chrono::steady_clock::now() >= deadline)
            {
                const auto snap = _barrier->snapshot();
                if (!snap.all_quiescent)
                {
                    ++_metrics.shutdown_barrier_timeouts;
                }
            }
        }

        if (_actors != nullptr)
        {
            const auto handles = _actors->activeHandles();
            for (const auto handle : handles)
            {
                ActorSlot* slot = _actors->find(handle);
                if (slot != nullptr && (slot->state() == ActorState::Idle || slot->state() == ActorState::Queued))
                {
                    slot->setState(ActorState::Stopping);
                    removeActor(handle, ActorRemovalReason::Stopped);
                }
            }
        }
    }

    void Worker::runShutdownPhaseC(const TimePoint deadline)
    {
        if (!networkEnabled() || _connections->activeCount() == 0)
        {
            return;
        }

        _network_stopping = true;
        const auto handles = _connections->activeHandles();
        for (const ConnectionHandle handle : handles)
        {
            static_cast<void>(beginGracefulClose(handle, CloseReason::Shutdown, deadline));
        }

        while (_connections->activeCount() != 0 && std::chrono::steady_clock::now() < deadline)
        {
            const auto now = std::chrono::steady_clock::now();
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - now);
            const auto regular_timeout = hasRunnableWork() ? std::chrono::milliseconds(0) : pollTimeout().value_or(remaining);
            const auto timeout = std::min(regular_timeout, remaining);
            const auto events = _poller.wait(timeout);

            processPollEvents(events, _budgets.poll);
            drainInbox(_budgets.inbox);
            expireTimers(std::chrono::steady_clock::now(), _budgets.timers);
            flushWrites(_budgets.writes);
        }
    }

    void Worker::runShutdownPhaseD(const TimePoint deadline)
    {
        if (networkEnabled())
        {
            const auto handles = _connections->activeHandles();
            for (const ConnectionHandle handle : handles)
            {
                forceClose(handle, CloseReason::Timeout);
            }
        }

        if (_actors != nullptr)
        {
            const auto handles = _actors->activeHandles();
            for (const auto handle : handles)
            {
                removeActor(handle, ActorRemovalReason::ShutdownForced);
            }
            _ready_queue->clear();
        }

        _inbox.close();

        while (std::chrono::steady_clock::now() < deadline)
        {
            const auto drain_res = _inbox.drain(
                _budgets.inbox,
                [this](WorkerEvent&& ev)
                {
                    onEvent(std::move(ev));
                }
            );
            _metrics.shutdown_inbox_events += drain_res.processed;

            const auto expire_res = _timers.expire(
                std::chrono::steady_clock::now(),
                _budgets.timers,
                [this](TimerPayload&& payload)
                {
                    onTimer(std::move(payload));
                }
            );
            _metrics.shutdown_timers_fired += expire_res.expired;

            if (!drain_res.has_more && !expire_res.due_items_remain)
            {
                break;
            }
        }
    }

    void Worker::releaseConnectionRegistration(const ConnectionHandle handle) noexcept
    {
        if (_registrations == nullptr || handle.id.value >= _connection_registrations.size())
        {
            return;
        }

        auto& registration_handle = _connection_registrations[handle.id.value];
        if (!registration_handle)
        {
            return;
        }

        const auto registration = _registrations->lookup(*registration_handle);
        if (registration)
        {
            try
            {
                _poller.remove(registration->descriptor);
            }
            catch (...)
            {
            }
        }
        static_cast<void>(_registrations->release(*registration_handle));
        registration_handle.reset();
    }

    std::optional<PollRegistrationView> Worker::registrationFor(const ConnectionHandle handle) const noexcept
    {
        if (_registrations == nullptr || handle.id.value >= _connection_registrations.size())
        {
            return std::nullopt;
        }

        const auto& registration_handle = _connection_registrations[handle.id.value];
        if (!registration_handle)
        {
            return std::nullopt;
        }

        const auto registration = _registrations->lookup(*registration_handle);
        if (!registration || !registration->connection || *registration->connection != handle)
        {
            return std::nullopt;
        }
        return registration;
    }

    bool Worker::isCurrent(const ConnectionHandle handle) const noexcept
    {
        return _connections != nullptr && _connections->find(handle) != nullptr;
    }
}
