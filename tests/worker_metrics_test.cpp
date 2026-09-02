#include "snf/net/tcp_listener.hpp"
#include "snf/worker/db_client.hpp"
#include "snf/worker/inbox.hpp"
#include "snf/worker/poller.hpp"
#include "snf/worker/worker.hpp"

#include "socket_test_support.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <utility>
#include <vector>

namespace
{
    using namespace std::chrono_literals;
    using Clock = std::chrono::steady_clock;
    using namespace snf::worker;

    class RecordingDbSink final : public DbCompletionSink
    {
    public:
        void completeDb(const AwaitKey, DbResult result) override
        {
            completions.push_back(std::move(result));
        }

        std::vector<DbResult> completions;
    };

    template <class Predicate> [[nodiscard]] bool waitUntil(Predicate predicate, const std::chrono::milliseconds timeout)
    {
        const auto deadline = Clock::now() + timeout;
        while (Clock::now() < deadline)
        {
            if (predicate())
            {
                return true;
            }
            std::this_thread::sleep_for(1ms);
        }
        return predicate();
    }

    [[nodiscard]] AwaitKey awaitKey(const std::uint64_t entity, const std::uint64_t operation)
    {
        return AwaitKey{
            .actor = ActorKey{.kind = ActorKind::Player, .entity = entity},
            .incarnation = ActorIncarnation{1},
            .operation = OperationId{operation},
        };
    }

    [[nodiscard]] WorkerEnvelope envelope(const std::uint32_t id, const std::uint32_t charge)
    {
        return WorkerEnvelope{
            .event =
                RemoteConnectionClose{
                    .connection = ConnectionRef{ConnectionId{id}, ConnectionGeneration{1}, WorkerId{0}},
                    .reason = CloseReason::Shutdown,
                },
            .charged_bytes = charge,
        };
    }

    [[nodiscard]] DbClientConfig dbConfig(const std::uint16_t port)
    {
        return DbClientConfig{
            .host_ip = "127.0.0.1",
            .port = port,
            .user = "snf",
            .password = "snf-test",
            .database = "snf_test",
            .ssl_mode = DbSslMode::Disabled,
            .connection_count = 1,
            .max_queued_operations = 8,
            .max_queued_bytes = 2 * sizeof(DbRequest),
            .operation_timeout = 10s,
        };
    }

    void test_inbox_aggregate_gauge_and_capacity()
    {
        WakeupHandle wakeup;
        WorkerInbox inbox(2, wakeup, WorkerInboxConfig{.max_bytes_per_worker = 1000, .max_workers = 2});
        auto first = inbox.bindSource(WorkerId{0});
        auto second = inbox.bindSource(WorkerId{1});

        assert(first.tryPush(envelope(1, 100)) == InboxPushResult::Accepted);
        assert(second.tryPush(envelope(2, 200)) == InboxPushResult::Accepted);
        assert(inbox.approximateQueuedBytes() == 300);
        assert(inbox.maxQueuedBytesTotal() == 1000);

        WorkerEnvelope popped;
        assert(inbox.lane(0).tryPop(popped));
        assert(inbox.approximateQueuedBytes() == 200);
    }

    void test_worker_samples_current_gauges_and_inbox_high_water()
    {
        WorkerBudgets budgets = WorkerBudgets::defaults();
        budgets.inbox.max_events = 1;
        budgets.inbox.max_per_lane = 1;

        Worker worker(WorkerId{0}, 1, budgets, WorkerInboxConfig{.max_bytes_per_worker = 128, .max_workers = 1});
        std::atomic<std::size_t> handled{0};
        worker.setEventHandler(
            [&handled](WorkerEvent&&)
            {
                handled.fetch_add(1, std::memory_order_release);
            }
        );
        auto port = worker.bindInboxSource(WorkerId{0});
        for (std::uint32_t index = 0; index < 65; ++index)
        {
            assert(port.tryPush(envelope(index, 1)) == InboxPushResult::Accepted);
        }

        std::thread runner(
            [&worker]
            {
                worker.run();
            }
        );
        assert(waitUntil(
            [&handled]
            {
                return handled.load(std::memory_order_acquire) == 65;
            },
            2s
        ));
        worker.requestStop();
        runner.join();

        const WorkerMetrics& metrics = worker.metrics();
        assert(metrics.loop_iterations >= 65);
        assert(metrics.high_water_marks.sampled_inbox_queued_bytes >= 1);
        assert(metrics.high_water_marks.sampled_inbox_queued_bytes <= 128);
        assert(metrics.gauges.inbox_queued_bytes == 0);
        assert(metrics.gauges.connections == 0);
        assert(metrics.gauges.actors == 0);
        assert(metrics.gauges.timer_entries == 0);
        assert(metrics.gauges.db_queued_operations == 0);
        assert(metrics.gauges.db_queued_bytes == 0);
        assert(metrics.gauges.db_in_flight == 0);
        const auto loop_latency = metrics.loop_iteration_ns.snapshot();
        assert(loop_latency.count == metrics.loop_iterations);
        assert(loop_latency.sum > 0);
        assert(loop_latency.max > 0);
    }

    void test_db_queue_byte_admission_and_all_decrement_paths()
    {
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());

        RecordingDbSink sink;
        Poller poller{16};
        DbClient client{dbConfig(port), sink};
        client.start(poller);
        static_cast<void>(client.advance(WorkerBudgets::defaults().db));
        assert(client.inFlightCount() == 0);

        const std::uint64_t request_bytes = sizeof(DbRequest);
        const auto expired = Clock::now() - 1ms;
        const auto future = Clock::now() + 10s;
        assert(client.tryStart(awaitKey(1, 1), LoadPlayerRequest{.player_id = 1}, expired).status == DbSubmitStatus::Pending);
        assert(client.tryStart(awaitKey(2, 2), LoadPlayerRequest{.player_id = 2}, future).status == DbSubmitStatus::Pending);
        assert(client.queuedCount() == 2);
        assert(client.queuedBytes() == 2 * request_bytes);
        assert(client.metrics().queued_operations_high_water == 2);
        assert(client.metrics().queued_bytes_high_water == 2 * request_bytes);

        const auto operation_rejected = client.tryStart(awaitKey(3, 3), LoadPlayerRequest{.player_id = 3}, future);
        assert(operation_rejected.status == DbSubmitStatus::Rejected);
        assert(client.queuedCount() == 2);
        assert(client.queuedBytes() == 2 * request_bytes);

        client.expireDeadlines(Clock::now());
        assert(client.queuedCount() == 1);
        assert(client.queuedBytes() == request_bytes);
        assert(sink.completions.size() == 1);

        SavePlayerRequest allocated_request{
            .player_id = 4,
            .owned_skill_ids = {1},
        };
        allocated_request.owned_skill_ids.reserve(64);
        const auto byte_rejected = client.tryStart(awaitKey(4, 4), DbRequest{std::move(allocated_request)}, future);
        assert(byte_rejected.status == DbSubmitStatus::Rejected);
        assert(client.queuedCount() == 1);
        assert(client.queuedBytes() == request_bytes);

        client.beginShutdown();
        assert(client.queuedCount() == 0);
        assert(client.queuedBytes() == 0);
        assert(sink.completions.size() == 2);
        const auto operation_latency = client.metrics().operation_latency_ns.snapshot();
        assert(operation_latency.count == 2);
        assert(operation_latency.count == client.metrics().operations_completed + client.metrics().operations_failed);
        assert(operation_latency.max > 0);

        static_cast<void>(client.shutdown(poller, Clock::now()));
        assert(client.queuedCount() == 0);
        assert(client.queuedBytes() == 0);
    }
}

void run_worker_metrics_tests()
{
    test_inbox_aggregate_gauge_and_capacity();
    test_worker_samples_current_gauges_and_inbox_high_water();
    test_db_queue_byte_admission_and_all_decrement_paths();
}
