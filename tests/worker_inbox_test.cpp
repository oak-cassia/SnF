#include "snf/worker/inbox.hpp"
#include "snf/worker/poller.hpp"
#include "snf/worker/spsc_ring.hpp"
#include "snf/worker/wakeup.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>
#include <vector>

namespace
{
    using namespace snf::worker;

    struct MoveProbe
    {
        int value{0};
        int* moves{nullptr};

        MoveProbe() = default;
        MoveProbe(const int v, int* const m)
            : value(v)
            , moves(m)
        {
        }

        MoveProbe(const MoveProbe&) = delete;
        MoveProbe& operator=(const MoveProbe&) = delete;

        MoveProbe(MoveProbe&& other) noexcept
            : value(other.value)
            , moves(other.moves)
        {
            if (moves != nullptr)
            {
                ++(*moves);
            }
            other.moves = nullptr;
        }

        MoveProbe& operator=(MoveProbe&& other) noexcept
        {
            value = other.value;
            moves = other.moves;
            if (moves != nullptr)
            {
                ++(*moves);
            }
            other.moves = nullptr;
            return *this;
        }
    };

    void test_spsc_ring_push_pop_order()
    {
        SpscRing<int, 4> ring;
        assert(ring.tryPush(1));
        assert(ring.tryPush(2));
        assert(ring.tryPush(3));

        int val = 0;
        assert(ring.tryPop(val) && val == 1);
        assert(ring.tryPop(val) && val == 2);
        assert(ring.tryPop(val) && val == 3);
        assert(!ring.tryPop(val));
    }

    void test_spsc_ring_rejects_when_full()
    {
        SpscRing<int, 4> ring;
        assert(ring.tryPush(1));
        assert(ring.tryPush(2));
        assert(ring.tryPush(3));
        assert(ring.tryPush(4));
        assert(!ring.tryPush(5));

        int val = 0;
        assert(ring.tryPop(val) && val == 1);
        assert(ring.tryPush(5));
        assert(!ring.tryPush(6));
    }

    void test_spsc_ring_empty_pop_returns_false()
    {
        SpscRing<int, 4> ring;
        int val = 0;
        assert(!ring.tryPop(val));
    }

    void test_spsc_ring_wraps_around()
    {
        SpscRing<int, 4> ring;
        int val = 0;
        for (int i = 0; i < 20; ++i)
        {
            assert(ring.tryPush(int{i}));
            assert(ring.tryPop(val) && val == i);
        }
    }

    void test_push_failure_does_not_move_value()
    {
        SpscRing<MoveProbe, 2> ring;
        int moves1 = 0;
        int moves2 = 0;
        int moves3 = 0;

        assert(ring.tryPush(MoveProbe(1, &moves1)));
        assert(ring.tryPush(MoveProbe(2, &moves2)));

        MoveProbe probe3(3, &moves3);
        assert(!ring.tryPush(std::move(probe3)));

        assert(moves3 == 0);
        assert(probe3.value == 3);
        assert(probe3.moves == &moves3);
    }

    void test_spsc_ring_single_producer_single_consumer()
    {
        SpscRing<std::uint64_t, 1024> ring;
        constexpr std::uint64_t TOTAL_ITEMS = 100'000;

        std::thread producer(
            [&]()
            {
                for (std::uint64_t i = 0; i < TOTAL_ITEMS; ++i)
                {
                    while (!ring.tryPush(std::uint64_t{i}))
                    {
                        std::this_thread::yield();
                    }
                }
            }
        );

        std::uint64_t expected = 0;
        while (expected < TOTAL_ITEMS)
        {
            std::uint64_t val = 0;
            if (ring.tryPop(val))
            {
                assert(val == expected);
                ++expected;
            }
            else
            {
                std::this_thread::yield();
            }
        }

        producer.join();
    }

    void test_inbox_lane_byte_limit_is_derived_from_worker_budget()
    {
        const WorkerInboxConfig config{
            .max_bytes_per_worker = 64ull * 1024 * 1024,
            .max_workers = 32,
        };

        WakeupHandle wakeup;
        WorkerInbox inbox16(16, wakeup, config);
        WorkerInbox inbox32(32, wakeup, config);

        // lane 은 self lane 을 포함해 worker_count 개를 할당하므로 예산도 worker_count 로 나눈다.
        // 이렇게 해야 inbox 하나의 총 payload 상한이 정확히 max_bytes_per_worker 가 된다.
        assert(inbox16.lane(0).maxQueuedBytes() == (64ull * 1024 * 1024) / 16);
        assert(inbox32.lane(0).maxQueuedBytes() == (64ull * 1024 * 1024) / 32);

        assert(inbox16.lane(0).maxQueuedBytes() * inbox16.workerCount() == config.max_bytes_per_worker);
        assert(inbox32.lane(0).maxQueuedBytes() * inbox32.workerCount() == config.max_bytes_per_worker);
    }

    void test_inbox_lane_rejects_over_byte_limit()
    {
        InboxLane lane(100);
        WorkerEnvelope env1{
            .event = RemoteConnectionClose{ConnectionRef{ConnectionId{1}, ConnectionGeneration{1}, WorkerId{0}}, CloseReason::Shutdown},
            .charged_bytes = 60,
        };
        assert(lane.tryPush(std::move(env1)) == InboxPushResult::Accepted);

        WorkerEnvelope env2{
            .event = RemoteConnectionClose{ConnectionRef{ConnectionId{2}, ConnectionGeneration{1}, WorkerId{0}}, CloseReason::Shutdown},
            .charged_bytes = 50,
        };
        assert(lane.tryPush(std::move(env2)) == InboxPushResult::Full);
        assert(env2.charged_bytes == 50);
    }

    void test_inbox_lane_byte_accounting_returns_to_zero()
    {
        InboxLane lane(100);
        WorkerEnvelope env1{
            .event = RemoteConnectionClose{ConnectionRef{ConnectionId{1}, ConnectionGeneration{1}, WorkerId{0}}, CloseReason::Shutdown},
            .charged_bytes = 50,
        };
        assert(lane.tryPush(std::move(env1)) == InboxPushResult::Accepted);
        assert(lane.approximateQueuedBytes() == 50);

        WorkerEnvelope out;
        assert(lane.tryPop(out));
        assert(lane.approximateQueuedBytes() == 0);
    }

    void test_inbox_closed_lane_rejects()
    {
        InboxLane lane(100);
        lane.close();

        WorkerEnvelope env{
            .event = RemoteConnectionClose{ConnectionRef{ConnectionId{1}, ConnectionGeneration{1}, WorkerId{0}}, CloseReason::Shutdown},
            .charged_bytes = 10,
        };
        assert(lane.tryPush(std::move(env)) == InboxPushResult::Closed);
    }

    void test_inbox_bind_source_returns_distinct_lanes()
    {
        WakeupHandle wakeup;
        WorkerInbox inbox(4, wakeup, WorkerInboxConfig{});
        auto p0 = inbox.bindSource(WorkerId{0});
        auto p1 = inbox.bindSource(WorkerId{1});

        assert(p0.isBound());
        assert(p1.isBound());
    }

    void test_inbox_double_bind_is_rejected()
    {
        WakeupHandle wakeup;
        WorkerInbox inbox(4, wakeup, WorkerInboxConfig{});
        auto p0 = inbox.bindSource(WorkerId{0});
        assert(p0.isBound());
    }

    void test_event_is_published_before_wakeup()
    {
        WakeupHandle wakeup;
        WorkerInbox inbox(2, wakeup, WorkerInboxConfig{});
        auto port = inbox.bindSource(WorkerId{0});
        Poller poller(16);
        poller.add(wakeup.descriptor(), PollToken{PollTargetKind::Wakeup, 0, 0}, PollInterest{.read = true});

        for (std::uint64_t i = 0; i < 1'000; ++i)
        {
            WorkerEnvelope env{
                .event =
                    RemoteConnectionClose{
                        ConnectionRef{ConnectionId{static_cast<std::uint32_t>(i)}, ConnectionGeneration{1}, WorkerId{0}}, CloseReason::Shutdown
                    },
                .charged_bytes = 10,
            };
            assert(port.tryPush(std::move(env)) == InboxPushResult::Accepted);

            const auto events = poller.wait(std::chrono::milliseconds(100));
            assert(events.size() == 1);
            wakeup.consume();

            WorkerEnvelope out;
            assert(inbox.lane(0).tryPop(out));
            const auto* close_evt = std::get_if<RemoteConnectionClose>(&out.event);
            assert(close_evt != nullptr);
            assert(close_evt->connection.id.value == static_cast<std::uint32_t>(i));
        }
    }

    void test_inbox_drain_respects_event_budget()
    {
        WakeupHandle wakeup;
        WorkerInbox inbox(2, wakeup, WorkerInboxConfig{});
        auto port = inbox.bindSource(WorkerId{0});

        for (std::uint32_t i = 0; i < 200; ++i)
        {
            WorkerEnvelope env{
                .event = RemoteConnectionClose{ConnectionRef{ConnectionId{i}, ConnectionGeneration{1}, WorkerId{0}}, CloseReason::Shutdown},
                .charged_bytes = 8,
            };
            assert(port.tryPush(std::move(env)) == InboxPushResult::Accepted);
        }

        const InboxBudget budget{
            .max_events = 100,
            .max_per_lane = 200,
            .max_duration = std::chrono::seconds(10),
        };

        std::size_t received = 0;
        const auto result = inbox.drain(
            budget,
            [&](WorkerEvent&&)
            {
                ++received;
            }
        );

        assert(result.processed == 100);
        assert(received == 100);
        assert(result.has_more);
        assert(result.budget_exhausted);
    }

    void test_inbox_drain_reports_empty()
    {
        WakeupHandle wakeup;
        WorkerInbox inbox(2, wakeup, WorkerInboxConfig{});
        auto port = inbox.bindSource(WorkerId{0});

        for (std::uint32_t i = 0; i < 5; ++i)
        {
            WorkerEnvelope env{
                .event = RemoteConnectionClose{ConnectionRef{ConnectionId{i}, ConnectionGeneration{1}, WorkerId{0}}, CloseReason::Shutdown},
                .charged_bytes = 8,
            };
            assert(port.tryPush(std::move(env)) == InboxPushResult::Accepted);
        }

        const InboxBudget budget{
            .max_events = 10,
            .max_per_lane = 10,
            .max_duration = std::chrono::seconds(10),
        };

        std::size_t received = 0;
        const auto result = inbox.drain(
            budget,
            [&](WorkerEvent&&)
            {
                ++received;
            }
        );

        assert(result.processed == 5);
        assert(received == 5);
        assert(!result.has_more);
        assert(!result.budget_exhausted);
    }

    void test_inbox_drain_round_robins_lanes()
    {
        WakeupHandle wakeup;
        WorkerInbox inbox(2, wakeup, WorkerInboxConfig{});
        auto port0 = inbox.bindSource(WorkerId{0});
        auto port1 = inbox.bindSource(WorkerId{1});

        for (std::uint32_t i = 0; i < 100; ++i)
        {
            WorkerEnvelope env{
                .event = RemoteConnectionClose{ConnectionRef{ConnectionId{i}, ConnectionGeneration{1}, WorkerId{0}}, CloseReason::Shutdown},
                .charged_bytes = 8,
            };
            assert(port0.tryPush(std::move(env)) == InboxPushResult::Accepted);
        }

        WorkerEnvelope env_lane1{
            .event = RemoteConnectionClose{ConnectionRef{ConnectionId{999}, ConnectionGeneration{1}, WorkerId{1}}, CloseReason::Shutdown},
            .charged_bytes = 8,
        };
        assert(port1.tryPush(std::move(env_lane1)) == InboxPushResult::Accepted);

        const InboxBudget budget{
            .max_events = 10,
            .max_per_lane = 8,
            .max_duration = std::chrono::seconds(10),
        };

        bool found_lane1 = false;
        const auto result = inbox.drain(
            budget,
            [&](WorkerEvent&& event)
            {
                const auto* close_evt = std::get_if<RemoteConnectionClose>(&event);
                if (close_evt != nullptr && close_evt->connection.owner.value == 1)
                {
                    found_lane1 = true;
                }
            }
        );

        assert(result.processed <= 10);
        assert(found_lane1);
    }

    void test_inbox_drain_resumes_from_next_lane()
    {
        WakeupHandle wakeup;
        WorkerInbox inbox(3, wakeup, WorkerInboxConfig{});
        auto p0 = inbox.bindSource(WorkerId{0});
        auto p1 = inbox.bindSource(WorkerId{1});
        auto p2 = inbox.bindSource(WorkerId{2});

        for (int i = 0; i < 5; ++i)
        {
            assert(
                p0.tryPush(WorkerEnvelope{
                    .event = RemoteConnectionClose{ConnectionRef{ConnectionId{0}, ConnectionGeneration{1}, WorkerId{0}}, CloseReason::Shutdown},
                    .charged_bytes = 1
                }) == InboxPushResult::Accepted
            );
            assert(
                p1.tryPush(WorkerEnvelope{
                    .event = RemoteConnectionClose{ConnectionRef{ConnectionId{1}, ConnectionGeneration{1}, WorkerId{1}}, CloseReason::Shutdown},
                    .charged_bytes = 1
                }) == InboxPushResult::Accepted
            );
            assert(
                p2.tryPush(WorkerEnvelope{
                    .event = RemoteConnectionClose{ConnectionRef{ConnectionId{2}, ConnectionGeneration{1}, WorkerId{2}}, CloseReason::Shutdown},
                    .charged_bytes = 1
                }) == InboxPushResult::Accepted
            );
        }

        // Drain 1 event from lane 0
        const InboxBudget budget1{.max_events = 1, .max_per_lane = 1, .max_duration = std::chrono::seconds(10)};
        std::vector<std::uint16_t> owners;
        const auto res1 = inbox.drain(
            budget1,
            [&](WorkerEvent&& ev)
            {
                owners.push_back(std::get<RemoteConnectionClose>(ev).connection.owner.value);
            }
        );
        assert(res1.processed == 1);
        assert(owners.size() == 1 && owners[0] == 0);

        // Next drain should start from lane 1
        const auto res2 = inbox.drain(
            budget1,
            [&](WorkerEvent&& ev)
            {
                owners.push_back(std::get<RemoteConnectionClose>(ev).connection.owner.value);
            }
        );
        assert(res2.processed == 1);
        assert(owners.size() == 2 && owners[1] == 1);
    }

    void test_inbox_multi_worker_stress()
    {
        WakeupHandle wakeup;
        WorkerInbox inbox(3, wakeup, WorkerInboxConfig{});
        auto p0 = inbox.bindSource(WorkerId{0});
        auto p1 = inbox.bindSource(WorkerId{1});
        auto p2 = inbox.bindSource(WorkerId{2});

        constexpr std::uint32_t ITEMS_PER_PRODUCER = 20'000;
        std::atomic<bool> start_flag{false};

        auto run_producer = [&](WorkerInboxPort port, std::uint16_t owner_id)
        {
            while (!start_flag.load(std::memory_order_acquire))
            {
                std::this_thread::yield();
            }

            for (std::uint32_t i = 0; i < ITEMS_PER_PRODUCER; ++i)
            {
                WorkerEnvelope env{
                    .event =
                        RemoteConnectionClose{
                            ConnectionRef{ConnectionId{i}, ConnectionGeneration{1}, WorkerId{owner_id}},
                            CloseReason::Shutdown,
                        },
                    .charged_bytes = 8,
                };
                while (port.tryPush(std::move(env)) != InboxPushResult::Accepted)
                {
                    std::this_thread::yield();
                }
            }
        };

        std::thread t0(run_producer, p0, 0);
        std::thread t1(run_producer, p1, 1);
        std::thread t2(run_producer, p2, 2);

        start_flag.store(true, std::memory_order_release);

        std::array<std::uint32_t, 3> next_expected{0, 0, 0};
        std::size_t total_received = 0;
        const InboxBudget budget{
            .max_events = 512,
            .max_per_lane = 32,
            .max_duration = std::chrono::milliseconds(50),
        };

        while (total_received < 3 * ITEMS_PER_PRODUCER)
        {
            const auto res = inbox.drain(
                budget,
                [&](WorkerEvent&& event)
                {
                    const auto& close_evt = std::get<RemoteConnectionClose>(event);
                    const auto owner = close_evt.connection.owner.value;
                    const auto seq = close_evt.connection.id.value;
                    assert(owner < 3);
                    assert(seq == next_expected[owner]);
                    ++next_expected[owner];
                    ++total_received;
                }
            );
            (void)res;
        }

        t0.join();
        t1.join();
        t2.join();

        assert(total_received == 3 * ITEMS_PER_PRODUCER);
    }
}

void run_worker_inbox_tests()
{
    test_spsc_ring_push_pop_order();
    test_spsc_ring_rejects_when_full();
    test_spsc_ring_empty_pop_returns_false();
    test_spsc_ring_wraps_around();
    test_push_failure_does_not_move_value();
    test_spsc_ring_single_producer_single_consumer();
    test_inbox_lane_byte_limit_is_derived_from_worker_budget();
    test_inbox_lane_rejects_over_byte_limit();
    test_inbox_lane_byte_accounting_returns_to_zero();
    test_inbox_closed_lane_rejects();
    test_inbox_bind_source_returns_distinct_lanes();
    test_inbox_double_bind_is_rejected();
    test_event_is_published_before_wakeup();
    test_inbox_drain_respects_event_budget();
    test_inbox_drain_reports_empty();
    test_inbox_drain_round_robins_lanes();
    test_inbox_drain_resumes_from_next_lane();
    test_inbox_multi_worker_stress();
}
