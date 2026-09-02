#include "snf/adapter/game_actor_factory.hpp"
#include "snf/adapter/game_payloads.hpp"
#include "snf/net/tcp_listener.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/worker.hpp"
#include "snf/worker/worker_group.hpp"

#include "socket_test_support.hpp"
#include "worker_metrics_report.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace snf::load_test
{
    struct CrossMessage
    {
        std::uint32_t remaining_hops{0};
    };
}

namespace snf::worker
{
    template <> struct ActorPayloadTraits<snf::load_test::CrossMessage>
    {
        static constexpr std::uint32_t TAG = 1;
        [[nodiscard]] static constexpr std::uint64_t calculateCharge(const snf::load_test::CrossMessage&) noexcept
        {
            return sizeof(snf::load_test::CrossMessage);
        }
    };

    struct WorkerGroupTestAccess
    {
        static Worker& worker(WorkerGroup& group, const std::size_t index)
        {
            return *group._workers[index];
        }
    };
}

namespace
{
    using namespace std::chrono_literals;
    using Clock = std::chrono::steady_clock;
    using namespace snf::worker;

    constexpr std::size_t LOAD_CLIENT_COUNT = 32;
    constexpr std::size_t SLOW_CLIENT_COUNT = 4;
    constexpr std::uint32_t PROBE_REQUEST_ID = 0xF0000001U;
    constexpr std::uint32_t SLOW_REQUEST_ID = 0xE0000001U;
    constexpr ActorKey PROBE_KEY{.kind = ActorKind::Player, .entity = 1};
    constexpr ActorKey SLOW_KEY{.kind = ActorKind::Player, .entity = 2};
    constexpr ActorKey HOT_KEY{.kind = ActorKind::Player, .entity = 3};
    constexpr bool CONTEXT_SWITCH_GATE_AUTHORITATIVE = SNF_WORKER_CONTEXT_SWITCH_GATE != 0;
    constexpr std::size_t GATE_MULTIPLIER = SNF_WORKER_GATE_MULTIPLIER;

    [[nodiscard]] std::optional<std::string> environment(const char* name)
    {
        const char* value = std::getenv(name);
        if (value == nullptr || *value == '\0')
        {
            return std::nullopt;
        }
        return std::string{value};
    }

    [[nodiscard]] std::uint16_t mysqlPort()
    {
        const auto text = environment("SNF_MYSQL_TEST_PORT");
        if (!text)
        {
            return 3306;
        }
        std::uint32_t value = 0;
        const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), value);
        if (error != std::errc{} || end != text->data() + text->size() || value == 0 || value > 65535)
        {
            throw std::invalid_argument{"SNF_MYSQL_TEST_PORT is invalid"};
        }
        return static_cast<std::uint16_t>(value);
    }

    [[nodiscard]] std::optional<DbClientConfig> mysqlConfig()
    {
        const auto host = environment("SNF_MYSQL_TEST_HOST");
        if (!host)
        {
            return std::nullopt;
        }
        return DbClientConfig{
            .host_ip = *host,
            .port = mysqlPort(),
            .user = environment("SNF_MYSQL_TEST_USER").value_or("snf"),
            .password = environment("SNF_MYSQL_TEST_PASSWORD").value_or("snf-test"),
            .database = environment("SNF_MYSQL_TEST_DATABASE").value_or("snf_test"),
            .ssl_mode = DbSslMode::Disabled,
            .connection_count = 1,
            .max_queued_operations = 8,
            .max_queued_bytes = 4096,
            .operation_timeout = 200ms,
            .shutdown_timeout = 500ms,
            .reconnect_backoff = 10s,
        };
    }

    [[nodiscard]] WorkerNetworkConfig loadNetworkConfig()
    {
        WorkerNetworkConfig config{};
        config.table.capacity = 64;
        config.table.limits.max_read_buffer_bytes = 64 * 1024;
        config.table.limits.write_soft_watermark_bytes = 32 * 1024;
        config.table.limits.write_hard_limit_bytes = 64 * 1024;
        config.table.limits.close_drain_deadline = 500ms;
        config.poll_registration_capacity = 65;
        config.max_accepts_per_poll = 1;
        config.receive_chunk_bytes = 16 * 1024;
        config.client_send_buffer_size = 4096;
        return config;
    }

    [[nodiscard]] WorkerActorConfig loadActorConfig()
    {
        WorkerActorConfig config{};
        config.actor_table_capacity = 64;
        config.max_mailbox_messages_per_actor = 256;
        config.max_mailbox_bytes_per_actor = 2 * 1024 * 1024;
        config.max_mailbox_messages_total = 512;
        config.max_mailbox_bytes_total = 8 * 1024 * 1024;
        config.max_turns_per_actor_slice = 32;
        config.worker_shutdown_timeout = 2000ms;
        config.await_timeout = 200ms;
        config.max_concurrent_loading = 32;
        config.max_application_timer_bytes_total = 2 * 1024 * 1024;
        return config;
    }

    [[nodiscard]] snf::protocol::Frame pingFrame(const std::uint32_t request_id, const std::size_t payload_size)
    {
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Ping,
            .request_id = request_id,
            .payload = std::vector<std::byte>(payload_size, std::byte{0x42}),
        };
    }

    class RoutingFactory final : public ActorFactory
    {
    public:
        void setTimerAdmission(TimerAdmission& admission) noexcept
        {
            _delegate.setTimerAdmission(admission);
        }

        [[nodiscard]] ActorConstructionResult construct(const ActorKey key) override
        {
            if (key == PROBE_KEY || key == SLOW_KEY)
            {
                return _delegate.construct(key);
            }
            return ActorConstructionResult::needsLoad();
        }

        [[nodiscard]] ActorConstructionResult constructLoaded(const ActorKey key, const LoadPlayerResult& loaded) override
        {
            return _delegate.constructLoaded(key, loaded);
        }

    private:
        snf::adapter::GameActorFactory _delegate;
    };

    class RoutingSink final : public RequestSink
    {
    public:
        void setWorker(Worker& worker) noexcept
        {
            _worker = &worker;
        }

        [[nodiscard]] RequestPostResult tryPost(ConnectionRef connection, snf::protocol::Frame&& frame) override
        {
            assert(_worker != nullptr);
            if (frame.type != snf::protocol::MessageType::Ping)
            {
                return RequestPostResult::Rejected;
            }

            ActorKey target = HOT_KEY;
            if (frame.request_id == PROBE_REQUEST_ID)
            {
                target = PROBE_KEY;
            }
            else if (frame.request_id == SLOW_REQUEST_ID)
            {
                target = SLOW_KEY;
            }
            auto envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PingMessage{
                .connection = connection,
                .request_id = frame.request_id,
                .payload = std::move(frame.payload),
            });
            return _worker->tell(target, std::move(envelope)) == DeliveryResult::Accepted ? RequestPostResult::Accepted : RequestPostResult::Rejected;
        }

    private:
        Worker* _worker{nullptr};
    };

    [[nodiscard]] bool waitDescriptor(const int descriptor, const short events, const Clock::time_point deadline)
    {
        while (Clock::now() < deadline)
        {
            const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - Clock::now());
            pollfd item{.fd = descriptor, .events = events, .revents = 0};
            const int result = ::poll(&item, 1, static_cast<int>(std::max<std::int64_t>(0, remaining.count())));
            if (result > 0)
            {
                return (item.revents & events) != 0;
            }
            if (result == 0)
            {
                return false;
            }
            if (errno != EINTR)
            {
                return false;
            }
        }
        return false;
    }

    [[nodiscard]] bool sendUntil(const int descriptor, const std::vector<std::byte>& bytes, const Clock::time_point deadline)
    {
        std::size_t offset = 0;
        while (offset < bytes.size() && Clock::now() < deadline)
        {
            const ssize_t sent = ::send(descriptor, bytes.data() + offset, bytes.size() - offset, MSG_DONTWAIT | MSG_NOSIGNAL);
            if (sent > 0)
            {
                offset += static_cast<std::size_t>(sent);
                continue;
            }
            if (sent == -1 && errno == EINTR)
            {
                continue;
            }
            if (sent == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                if (!waitDescriptor(descriptor, POLLOUT, deadline))
                {
                    return false;
                }
                continue;
            }
            return false;
        }
        return offset == bytes.size();
    }

    [[nodiscard]] bool receiveUntil(const int descriptor, std::vector<std::byte>& bytes, const Clock::time_point deadline)
    {
        std::size_t offset = 0;
        while (offset < bytes.size() && Clock::now() < deadline)
        {
            const ssize_t received = ::recv(descriptor, bytes.data() + offset, bytes.size() - offset, MSG_DONTWAIT);
            if (received > 0)
            {
                offset += static_cast<std::size_t>(received);
                continue;
            }
            if (received == -1 && errno == EINTR)
            {
                continue;
            }
            if (received == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                if (!waitDescriptor(descriptor, POLLIN, deadline))
                {
                    return false;
                }
                continue;
            }
            return false;
        }
        return offset == bytes.size();
    }

    void assertActiveLoopBounds(const WorkerMetrics& metrics, const WorkerBudgets& budgets)
    {
        constexpr auto multiplier = GATE_MULTIPLIER;
        const auto phase_sum = budgets.poll.max_duration + budgets.inbox.max_duration + budgets.timers.max_duration + budgets.db.max_duration +
                               budgets.actors.max_duration + budgets.writes.max_duration;
        const auto fairness = (phase_sum + budgets.max_poll_timeout) * multiplier;
        const auto phase_limit = [&metrics, fairness](const WorkerPhase phase, const std::chrono::nanoseconds cpu_threshold)
        {
            const auto& observed = metrics.phases[static_cast<std::size_t>(phase)];
            assert(observed.entries > 0);
            if (observed.max_cpu_residence >= cpu_threshold)
            {
                std::cerr << "phase CPU bound exceeded: phase=" << static_cast<std::size_t>(phase)
                          << " cpu_residence_ns=" << observed.max_cpu_residence.count() << " threshold_ns=" << cpu_threshold.count() << '\n';
            }
            assert(observed.max_cpu_residence < cpu_threshold);
            if constexpr (CONTEXT_SWITCH_GATE_AUTHORITATIVE)
            {
                assert(observed.voluntary_context_switches == 0);
            }
            assert(observed.max_residence < fairness);
        };
        phase_limit(WorkerPhase::Poll, budgets.poll.max_duration * multiplier + WorkerGateThresholds::POLL_ITEM_ALLOWANCE);
        phase_limit(WorkerPhase::Inbox, budgets.inbox.max_duration * multiplier + WorkerGateThresholds::INBOX_ITEM_ALLOWANCE);
        phase_limit(WorkerPhase::Timers, budgets.timers.max_duration * multiplier + WorkerGateThresholds::TIMER_ITEM_ALLOWANCE);
        phase_limit(WorkerPhase::Db, budgets.db.max_duration * multiplier + WorkerGateThresholds::DB_ITEM_ALLOWANCE);
        phase_limit(WorkerPhase::Actors, budgets.actors.max_duration * multiplier + WorkerGateThresholds::ACTOR_ITEM_ALLOWANCE);
        phase_limit(WorkerPhase::Writes, budgets.writes.max_duration * multiplier + WorkerGateThresholds::WRITE_ITEM_ALLOWANCE);

        const auto poll_wait = metrics.phases[static_cast<std::size_t>(WorkerPhase::PollWait)];
        assert(poll_wait.entries > 0);
        if constexpr (CONTEXT_SWITCH_GATE_AUTHORITATIVE)
        {
            assert(poll_wait.max_residence < budgets.max_poll_timeout * 2);
        }

        for (const WorkerPhase phase :
             {WorkerPhase::Poll, WorkerPhase::Inbox, WorkerPhase::Timers, WorkerPhase::Db, WorkerPhase::Actors, WorkerPhase::Writes})
        {
            assert(metrics.phases[static_cast<std::size_t>(phase)].max_entry_gap < fairness);
        }
    }

    void assertCleanShutdown(const WorkerMetrics& metrics)
    {
        const auto& shutdown = metrics.shutdown;
        assert(!shutdown.actor_deadline_exceeded);
        assert(!shutdown.db_deadline_exceeded);
        assert(!shutdown.phase_a.deadline_hit);
        assert(!shutdown.phase_b.deadline_hit);
        assert(!shutdown.phase_c.deadline_hit);
        assert(!shutdown.phase_d.deadline_hit);
        assert(shutdown.actor_phases_duration <= shutdown.actor_configured_timeout);
        if (shutdown.db_configured_timeout > std::chrono::nanoseconds::zero())
        {
            assert(shutdown.db_shutdown_duration <= shutdown.db_configured_timeout);
        }
        assert(shutdown.phase_d.remaining.connections == 0);
        assert(shutdown.phase_d.remaining.actors == 0);
        assert(shutdown.phase_d.remaining.blocked_actors == 0);
        assert(shutdown.phase_d.remaining.loading == 0);
        assert(shutdown.phase_d.remaining.inbox_bytes == 0);
        assert(shutdown.phase_d.remaining.application_timer_bytes == 0);
        assert(shutdown.final_resources.db_in_flight == 0);
        assert(shutdown.final_resources.db_queued == 0);
        assert(metrics.actor.forced_blocked_destructions == 0);
    }

    void runSingleWorkerLoad(const bool mysql)
    {
        snf::net::UniqueFileDescriptor stalled_db_listener;
        DbClientConfig db_config;
        if (mysql)
        {
            db_config = *mysqlConfig();
        }
        else
        {
            stalled_db_listener = snf::net::create_tcp_listener(0);
            db_config = DbClientConfig{
                .host_ip = "127.0.0.1",
                .port = snf::test::portOf(stalled_db_listener.getDescriptor()),
                .user = "snf",
                .password = "snf-test",
                .database = "snf_test",
                .ssl_mode = DbSslMode::Disabled,
                .connection_count = 1,
                .max_queued_operations = 8,
                .max_queued_bytes = 4096,
                .operation_timeout = 200ms,
                .shutdown_timeout = 500ms,
                .reconnect_backoff = 10s,
            };
        }

        const WorkerBudgets budgets = WorkerBudgets::defaults();
        const WorkerNetworkConfig network = loadNetworkConfig();
        const WorkerActorConfig actors = loadActorConfig();
        RoutingSink sink;
        RoutingFactory factory;
        Worker worker(WorkerId{0}, 1, budgets, WorkerInboxConfig{}, network, sink, actors, factory);
        worker.configureDb(db_config);
        sink.setWorker(worker);
        factory.setTimerAdmission(worker);

        std::size_t preload_accepted = 0;
        std::size_t preload_rejected = 0;
        for (std::uint64_t entity = 100; entity < 132; ++entity)
        {
            const auto result = worker.tryDeliverLocal(
                ActorKey{.kind = ActorKind::Player, .entity = entity},
                snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PingMessage{
                    .connection = ConnectionRef{ConnectionId{999}, ConnectionGeneration{1}, WorkerId{0}},
                    .request_id = static_cast<std::uint32_t>(entity),
                    .payload = {std::byte{0x01}},
                })
            );
            if (result == DeliveryResult::Accepted)
            {
                ++preload_accepted;
            }
            else
            {
                ++preload_rejected;
            }
        }
        assert(preload_accepted > 0);
        assert(preload_rejected > 0);

        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));

        // Build the entire in-process client harness before the Worker starts.
        // Otherwise thread/vector allocation can contend with the owner thread's
        // preallocated accept path and manufacture a blocking sample that cannot
        // occur when the clients are separate processes.
        std::vector<snf::net::UniqueFileDescriptor> clients;
        clients.reserve(LOAD_CLIENT_COUNT);
        for (std::size_t index = 0; index < LOAD_CLIENT_COUNT; ++index)
        {
            clients.push_back(snf::test::connectClient(port, index < SLOW_CLIENT_COUNT ? 4096 : 0));
        }
        auto probe = snf::test::connectClient(port);

        if (!mysql)
        {
            // Exercise an unavailable backend through an immediate refused
            // connect. A completed TCP handshake with no MySQL peer would make
            // mysql_close() perform driver teardown during the active Db phase.
            stalled_db_listener.init();
        }

        const auto slow_request = snf::protocol::encode_frame(pingFrame(SLOW_REQUEST_ID, 4096));
        const auto normal_request = snf::protocol::encode_frame(pingFrame(1000, 32));
        const auto probe_request = snf::protocol::encode_frame(pingFrame(PROBE_REQUEST_ID, 2));
        const auto probe_expected = snf::protocol::encode_frame(snf::protocol::Frame{
            .type = snf::protocol::MessageType::Pong,
            .request_id = PROBE_REQUEST_ID,
            .payload = {std::byte{0x42}, std::byte{0x42}},
        });

        std::atomic<bool> start_clients{false};
        std::atomic<bool> stop_clients{false};
        std::atomic<std::size_t> writer_exits{0};
        std::atomic<bool> probe_exited{false};
        std::atomic<bool> probe_failed{false};
        std::atomic<std::uint64_t> probe_sent{0};
        std::atomic<std::uint64_t> probe_received{0};
        std::vector<std::thread> writers;
        writers.reserve(LOAD_CLIENT_COUNT);
        for (std::size_t index = 0; index < clients.size(); ++index)
        {
            const int descriptor = clients[index].getDescriptor();
            const bool slow = index < SLOW_CLIENT_COUNT;
            writers.emplace_back(
                [descriptor, slow, &start_clients, &stop_clients, &writer_exits, &slow_request, &normal_request]
                {
                    start_clients.wait(false, std::memory_order_acquire);
                    const auto& encoded = slow ? slow_request : normal_request;
                    while (!stop_clients.load(std::memory_order_acquire))
                    {
                        if (!sendUntil(descriptor, encoded, Clock::now() + 100ms))
                        {
                            break;
                        }
                        std::this_thread::sleep_for(10ms);
                    }
                    writer_exits.fetch_add(1, std::memory_order_release);
                }
            );
        }

        const int probe_descriptor = probe.getDescriptor();
        std::vector<std::byte> probe_response(probe_expected.size());
        std::thread probe_thread(
            [probe_descriptor,
             &start_clients,
             &stop_clients,
             &probe_exited,
             &probe_failed,
             &probe_sent,
             &probe_received,
             &probe_request,
             &probe_expected,
             response = std::move(probe_response)]() mutable
            {
                start_clients.wait(false, std::memory_order_acquire);
                while (!stop_clients.load(std::memory_order_acquire))
                {
                    const auto deadline = Clock::now() + 750ms;
                    if (!sendUntil(probe_descriptor, probe_request, deadline))
                    {
                        if (!stop_clients.load(std::memory_order_acquire))
                        {
                            probe_failed.store(true, std::memory_order_release);
                        }
                        break;
                    }
                    probe_sent.fetch_add(1, std::memory_order_relaxed);
                    if (!receiveUntil(probe_descriptor, response, deadline) || response != probe_expected)
                    {
                        if (!stop_clients.load(std::memory_order_acquire))
                        {
                            probe_failed.store(true, std::memory_order_release);
                        }
                        break;
                    }
                    probe_received.fetch_add(1, std::memory_order_relaxed);
                    std::this_thread::sleep_for(100ms);
                }
                probe_exited.store(true, std::memory_order_release);
            }
        );

        WorkerWatchdog watchdog(WorkerWatchdogConfig{}, budgets, {{.worker = WorkerId{0}, .progress = &worker.progress()}});
        watchdog.start();
        std::thread runner(
            [&worker]
            {
                worker.run();
            }
        );
        start_clients.store(true, std::memory_order_release);
        start_clients.notify_all();

        std::this_thread::sleep_for(3s);
        stop_clients.store(true, std::memory_order_release);
        for (const auto& client : clients)
        {
            static_cast<void>(::shutdown(client.getDescriptor(), SHUT_RDWR));
        }
        static_cast<void>(::shutdown(probe.getDescriptor(), SHUT_RDWR));

        const auto harness_deadline = Clock::now() + 2s;
        while ((writer_exits.load(std::memory_order_acquire) != LOAD_CLIENT_COUNT || !probe_exited.load(std::memory_order_acquire)) &&
               Clock::now() < harness_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        const bool harness_bounded =
            writer_exits.load(std::memory_order_acquire) == LOAD_CLIENT_COUNT && probe_exited.load(std::memory_order_acquire);
        for (auto& writer : writers)
        {
            writer.join();
        }
        probe_thread.join();
        assert(harness_bounded);
        clients.clear();
        probe.init();

        std::this_thread::sleep_for(2s);
        worker.requestStop();
        runner.join();
        watchdog.stop();

        assert(!probe_failed.load(std::memory_order_acquire));
        assert(probe_sent.load(std::memory_order_relaxed) > 0);
        assert(probe_received.load(std::memory_order_relaxed) == probe_sent.load(std::memory_order_relaxed));
        assert(watchdog.metrics().samples_taken.load(std::memory_order_relaxed) > 0);
        assert(watchdog.metrics().active_stall_episodes.load(std::memory_order_relaxed) == 0);

        const WorkerMetrics& metrics = worker.metrics();
        const DbClientMetrics& db = worker.dbMetrics();
        std::cerr << "worker_report.context_switch_gate_authoritative=" << CONTEXT_SWITCH_GATE_AUTHORITATIVE << '\n';
        snf::test::printWorkerReport(std::cerr, metrics, db);
        assert(metrics.network.protocol_errors == 0);
        assert(metrics.network.invariant_violations == 0);
        assert(metrics.thread_execution_sample_failures == 0);
        assertActiveLoopBounds(metrics, budgets);

        assert(metrics.high_water_marks.sampled_connections <= network.table.capacity);
        assert(metrics.high_water_marks.sampled_actors <= actors.actor_table_capacity);
        assert(metrics.high_water_marks.sampled_mailbox_messages_total <= actors.max_mailbox_messages_total);
        assert(metrics.high_water_marks.sampled_mailbox_bytes_total <= actors.max_mailbox_bytes_total);
        assert(metrics.high_water_marks.db_queued_operations <= db_config.max_queued_operations);
        assert(metrics.high_water_marks.db_queued_bytes <= db_config.max_queued_bytes);
        const std::array<bool, 7> exercised{
            metrics.high_water_marks.connection_read_buffer_bytes > 0,
            metrics.high_water_marks.connection_write_queued_bytes > 0,
            metrics.high_water_marks.sampled_connections > 0,
            metrics.high_water_marks.sampled_actors > 0,
            metrics.high_water_marks.sampled_mailbox_messages_total > 0,
            metrics.high_water_marks.db_queued_operations > 0,
            metrics.high_water_marks.db_queued_bytes > 0,
        };
        assert(std::ranges::count(exercised, true) >= 3);
        assert(db.submit_rejections > 0);
        if (!mysql)
        {
            const auto operation = db.operation_latency_ns.snapshot();
            const auto phase_sum = budgets.poll.max_duration + budgets.inbox.max_duration + budgets.timers.max_duration + budgets.db.max_duration +
                                   budgets.actors.max_duration + budgets.writes.max_duration;
            const auto fairness = (phase_sum + budgets.max_poll_timeout) * GATE_MULTIPLIER;
            assert(db.queued_timeouts + db.in_flight_timeouts > 0);
            assert(operation.max >= 160ms / 1ns);
            assert(operation.max <= static_cast<std::uint64_t>((db_config.operation_timeout + fairness) / 1ns));
        }
        assertCleanShutdown(metrics);
        std::cout << "worker_report.context_switch_gate_authoritative=" << CONTEXT_SWITCH_GATE_AUTHORITATIVE << '\n';
        snf::test::printWorkerReport(std::cout, metrics, db);
        std::cout << "worker_report.watchdog=samples:" << watchdog.metrics().samples_taken.load(std::memory_order_relaxed)
                  << ",active_stalls:" << watchdog.metrics().active_stall_episodes.load(std::memory_order_relaxed)
                  << ",shutdown_stalls:" << watchdog.metrics().shutdown_stall_episodes.load(std::memory_order_relaxed)
                  << ",longest_ns:" << watchdog.metrics().longest_stall_ns.load(std::memory_order_relaxed) << '\n';
    }

    using CrossRegistry = ActorPayloadRegistry<snf::load_test::CrossMessage>;

    class CrossActor final : public ActorInstance
    {
    public:
        explicit CrossActor(const ActorKey peer)
            : _peer(peer)
        {
        }

        [[nodiscard]] TurnResult dispatch(ActorEnvelope&& envelope, const ActorTurnContext&) override
        {
            auto message = envelope.take<snf::load_test::CrossMessage>();
            EffectBatch effects;
            if (message.remaining_hops > 0)
            {
                effects.push(TellActorEffect{
                    .target = _peer,
                    .message = CrossRegistry::create(snf::load_test::CrossMessage{.remaining_hops = message.remaining_hops - 1}),
                });
            }
            return CompletedTurn{.effects = std::move(effects)};
        }

    private:
        ActorKey _peer;
    };

    class CrossFactory final : public ActorFactory
    {
    public:
        CrossFactory(const ActorKey first, const ActorKey second)
            : _first(first)
            , _second(second)
        {
        }

        [[nodiscard]] ActorConstructionResult construct(const ActorKey key) override
        {
            return ActorConstructionResult::ready(std::make_unique<CrossActor>(key == _first ? _second : _first));
        }

    private:
        ActorKey _first;
        ActorKey _second;
    };

    void runWorkerGroupPressure()
    {
        WorkerActorConfig actors = loadActorConfig();
        actors.worker_shutdown_timeout = 2s;
        ActorKey first{.kind = ActorKind::Player, .entity = 1000};
        while (ownerOf(first, 2, actors.placement_seed) != WorkerId{0})
        {
            ++first.entity;
        }
        ActorKey second{.kind = ActorKind::Player, .entity = 2000};
        while (ownerOf(second, 2, actors.placement_seed) != WorkerId{1})
        {
            ++second.entity;
        }

        WorkerGroupConfig config{};
        config.worker_count = 2;
        config.port = 0;
        config.actor = actors;
        config.watchdog = WorkerWatchdogConfig{};
        config.group_shutdown_grace = 500ms;
        auto factory_factory = [first, second](WorkerId) -> std::unique_ptr<ActorFactory>
        {
            return std::make_unique<CrossFactory>(first, second);
        };
        WorkerGroup group(config, {}, factory_factory);
        Worker& source = snf::worker::WorkerGroupTestAccess::worker(group, 0);
        for (std::size_t index = 0; index < 128; ++index)
        {
            assert(
                source.tryDeliverLocal(first, CrossRegistry::create(snf::load_test::CrossMessage{.remaining_hops = 8})) == DeliveryResult::Accepted
            );
        }

        group.start();
        std::this_thread::sleep_for(1s);
        group.requestStop();
        group.join();

        assert(group.joinOverruns().empty());
        assert(group.watchdogMetrics() != nullptr);
        assert(group.watchdogMetrics()->active_stall_episodes.load(std::memory_order_relaxed) == 0);
        std::uint64_t sent = 0;
        std::uint64_t received = 0;
        for (std::size_t index = 0; index < group.workerCount(); ++index)
        {
            const auto& worker = group.worker(index);
            sent += worker.metrics().actor.remote_tells_sent;
            received += worker.metrics().actor.remote_tells_received;
            assert(worker.metrics().network.invariant_violations == 0);
            assertCleanShutdown(worker.metrics());
            snf::test::printWorkerReport(std::cout, worker.metrics(), worker.dbMetrics());
        }
        assert(sent > 0);
        assert(received == sent);
    }
}

int main(const int argc, char** argv)
{
    if (argc != 2)
    {
        std::cerr << "usage: snf_worker_load_scenario_tests --stub|--mysql\n";
        return 2;
    }
    const std::string_view mode{argv[1]};
    if (mode == "--mysql" && !mysqlConfig())
    {
        std::cout << "SKIP: SNF_MYSQL_TEST_HOST is not set\n";
        return 77;
    }
    if (mode != "--stub" && mode != "--mysql")
    {
        return 2;
    }

    runWorkerGroupPressure();
    runSingleWorkerLoad(mode == "--mysql");
}
