#include "snf/worker/worker.hpp"

#include "snf/net/socket_options.hpp"
#include "snf/net/system_error.hpp"

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

        if (networkEnabled())
        {
            runNetworkShutdown();
        }

        // stop 이후: inbox.close() 뒤 남은 accepted event와 만료된 timer를
        // hard deadline(초기값 2s)까지 정리하고 반환
        _inbox.close();
        const auto shutdown_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);

        while (std::chrono::steady_clock::now() < shutdown_deadline)
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
            throw std::logic_error{"A remote Worker inbox target is already bound"};
        }
        _remote_ports[target.value] = std::move(port);
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
            return port.tryPush(std::move(envelope)) == InboxPushResult::Accepted;
        }

        const ConnectionHandle handle{.id = connection.id, .generation = connection.generation};
        if (!isCurrent(handle))
        {
            return false;
        }
        return graceful ? beginGracefulClose(handle, reason) : (forceClose(handle, reason), true);
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
               (_write_work_queue != nullptr && !_write_work_queue->empty());
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

        // duration_cast 는 0 으로 절단되어 1ms 미만 deadline 에서 timeout 0 인 바쁜 대기를 만든다.
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

            // EPOLLERR and peer hangup are immediate-close paths. In
            // particular, they never enter the graceful write-drain state.
            if (event.error || event.hangup)
            {
                forceClose(handle, event.fatal_error ? CloseReason::IoError : CloseReason::PeerClosed);
                continue;
            }

            if (event.writable && slot->waitingEpollout())
            {
                // A level-triggered socket can appear more than once in a
                // batch. Clearing this bit before the requeue makes later
                // duplicate EPOLLOUT events harmless.
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

            if (slot->isOpen() && event.readable && !slot->readQueued() && !enqueueRead(*slot))
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
        if (!networkEnabled())
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

    void Worker::runReadyActors(const CountTimeBudget&)
    {
        // Actor table and ready queue are introduced by the next vertical
        // slice. Network I/O does not run actor code inline.
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

        if (_timer_handler)
        {
            _timer_handler(std::move(payload));
        }
    }

    SendResult Worker::sendLocal(const ConnectionRef connection, snf::protocol::Frame&& frame, const bool critical)
    {
        // Take ownership before any lookup or encoding can fail. This keeps
        // Worker::send's consume-on-failure contract true even for rejected
        // stale handles and allocation exceptions in the encoder.
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

    bool Worker::beginGracefulClose(const ConnectionHandle handle, const CloseReason reason)
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

        const auto deadline = std::chrono::steady_clock::now() + _network_config.table.limits.close_drain_deadline;
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

    void Worker::beginNetworkShutdown()
    {
        if (!networkEnabled() || _network_stopping)
        {
            return;
        }
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

        const auto handles = _connections->activeHandles();
        for (const ConnectionHandle handle : handles)
        {
            static_cast<void>(beginGracefulClose(handle, CloseReason::Shutdown));
        }
    }

    void Worker::runNetworkShutdown()
    {
        beginNetworkShutdown();
        const auto shutdown_deadline = std::chrono::steady_clock::now() + _network_config.table.limits.close_drain_deadline;

        while (_connections->activeCount() != 0 && std::chrono::steady_clock::now() < shutdown_deadline)
        {
            const auto now = std::chrono::steady_clock::now();
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(shutdown_deadline - now);
            const auto regular_timeout = hasRunnableWork() ? std::chrono::milliseconds(0) : pollTimeout().value_or(remaining);
            const auto timeout = std::min(regular_timeout, remaining);
            const auto events = _poller.wait(timeout);

            processPollEvents(events, _budgets.poll);
            drainInbox(_budgets.inbox);
            expireTimers(std::chrono::steady_clock::now(), _budgets.timers);
            runReadyActors(_budgets.actors);
            flushWrites(_budgets.writes);
        }

        const auto handles = _connections->activeHandles();
        for (const ConnectionHandle handle : handles)
        {
            forceClose(handle, CloseReason::Timeout);
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
