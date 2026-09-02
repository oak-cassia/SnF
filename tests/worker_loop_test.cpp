#include "snf/worker/budget.hpp"
#include "snf/worker/worker.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <thread>

namespace
{
    using namespace snf::worker;

    using Clock = std::chrono::steady_clock;

    // WorkerMetrics 는 plain counter 라 owner thread 밖에서 읽으면 data race 이므로,
    // 이 테스트의 작업 완료 여부는 핸들러가 세우는 atomic 을 본다. Worker lifecycle과
    // 현재 phase만 필요할 때는 cross-thread safe한 progress()를 사용한다.
    //
    // stop 후 join 한 뒤 metrics 만 확인하면 셧다운 drain 루프가 같은 일을 하기 때문에
    // 메인 루프가 실제로 동작했는지 구분할 수 없다. 그래서 stop 전에 관측한다.
    template <class Predicate> [[nodiscard]] bool waitUntil(Predicate predicate, const std::chrono::milliseconds timeout)
    {
        const auto deadline = Clock::now() + timeout;
        while (Clock::now() < deadline)
        {
            if (predicate())
            {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return predicate();
    }

    [[nodiscard]] WorkerEnvelope makeEnvelope(const std::uint32_t connection_id)
    {
        return WorkerEnvelope{
            .event =
                RemoteConnectionClose{
                    ConnectionRef{ConnectionId{connection_id}, ConnectionGeneration{1}, WorkerId{1}},
                    CloseReason::Shutdown,
                },
            .charged_bytes = 1,
        };
    }

    void test_worker_starts_and_stops_cleanly()
    {
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{});
        std::thread th(
            [&]()
            {
                worker.run();
            }
        );

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        worker.requestStop();
        th.join();

        assert(worker.metrics().loop_iterations > 0);
        assert(worker.metrics().wakeups_consumed >= 1);
    }

    void test_worker_stop_before_run_returns_immediately()
    {
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{});
        worker.requestStop();

        const auto start = Clock::now();
        worker.run();
        const auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);

        assert(diff < std::chrono::milliseconds(200));
    }

    void test_worker_processes_remote_event()
    {
        std::atomic<std::size_t> handled{0};

        Worker worker(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{});
        worker.setEventHandler(
            [&handled](WorkerEvent&&)
            {
                handled.fetch_add(1, std::memory_order_release);
            }
        );
        auto port = worker.bindInboxSource(WorkerId{1});

        std::thread worker_thread(
            [&]()
            {
                worker.run();
            }
        );

        std::thread producer_thread(
            [&]()
            {
                assert(port.tryPush(makeEnvelope(1)) == InboxPushResult::Accepted);
            }
        );
        producer_thread.join();

        // stop 전에 관측한다. 여기서 통과하면 메인 루프가 처리한 것이다.
        assert(waitUntil(
            [&]
            {
                return handled.load(std::memory_order_acquire) == 1;
            },
            std::chrono::milliseconds(2000)
        ));

        worker.requestStop();
        worker_thread.join();

        assert(worker.metrics().inbox_events == 1);
        assert(worker.metrics().shutdown_inbox_events == 0);
    }

    // 메인 루프에서 drainInbox/expireTimers 를 제거해도 셧다운 drain 루프가 같은 일을 하므로
    // join 후 metrics 만 보는 테스트는 전부 통과해 버린다. 이 테스트가 그 회귀를 막는다.
    void test_main_loop_does_the_work_not_shutdown()
    {
        std::atomic<std::size_t> events_handled{0};
        std::atomic<std::size_t> timers_handled{0};

        Worker worker(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{});
        worker.setEventHandler(
            [&events_handled](WorkerEvent&&)
            {
                events_handled.fetch_add(1, std::memory_order_release);
            }
        );
        worker.setTimerHandler(
            [&timers_handled](TimerPayload&&)
            {
                timers_handled.fetch_add(1, std::memory_order_release);
            }
        );

        auto port = worker.bindInboxSource(WorkerId{1});
        assert(worker.trySchedule(Clock::now() + std::chrono::milliseconds(20), AwaitTimeout{}));

        std::thread worker_thread(
            [&]()
            {
                worker.run();
            }
        );

        assert(port.tryPush(makeEnvelope(7)) == InboxPushResult::Accepted);

        assert(waitUntil(
            [&]
            {
                return events_handled.load(std::memory_order_acquire) == 1 && timers_handled.load(std::memory_order_acquire) == 1;
            },
            std::chrono::milliseconds(2000)
        ));

        worker.requestStop();
        worker_thread.join();

        // 두 phase 모두 메인 루프가 처리했고 셧다운 루프에는 아무것도 남지 않았다.
        assert(worker.metrics().inbox_events == 1);
        assert(worker.metrics().timers_fired == 1);
        assert(worker.metrics().shutdown_inbox_events == 0);
        assert(worker.metrics().shutdown_timers_fired == 0);
    }

    void test_worker_wakes_from_idle_poll()
    {
        // 기본 max_poll_timeout(50ms)이면 wakeup 이 완전히 고장 나도 Worker 가 스스로 깨어나
        // 테스트가 통과한다. poll timeout 을 5초로 올려 그 우회로를 막는다.
        WorkerBudgets budgets = WorkerBudgets::defaults();
        budgets.max_poll_timeout = std::chrono::milliseconds(5000);

        std::atomic<std::size_t> handled{0};

        Worker worker(WorkerId{0}, 2, budgets, WorkerInboxConfig{});
        worker.setEventHandler(
            [&handled](WorkerEvent&&)
            {
                handled.fetch_add(1, std::memory_order_release);
            }
        );
        auto port = worker.bindInboxSource(WorkerId{1});

        std::thread worker_thread(
            [&]()
            {
                worker.run();
            }
        );

        // 1. warm-up event 로 Worker 가 실제로 돌고 있음을 확인한다.
        assert(port.tryPush(makeEnvelope(100)) == InboxPushResult::Accepted);
        assert(waitUntil(
            [&]
            {
                return handled.load(std::memory_order_acquire) == 1;
            },
            std::chrono::milliseconds(2000)
        ));

        // 2. 5초짜리 epoll_wait 에 확실히 진입하도록 잠시 둔다.
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // 3. push 시점부터 핸들러 관측까지의 지연을 잰다. 테스트 자신의 sleep 이 아니라
        //    Worker 의 반응 지연을 재야 하므로 stop 하지 않은 상태에서 측정한다.
        const auto start = Clock::now();
        assert(port.tryPush(makeEnvelope(200)) == InboxPushResult::Accepted);
        const bool observed = waitUntil(
            [&]
            {
                return handled.load(std::memory_order_acquire) == 2;
            },
            std::chrono::milliseconds(1000)
        );
        const auto latency = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);

        worker.requestStop();
        worker_thread.join();

        assert(observed);
        assert(latency < std::chrono::milliseconds(200));
        assert(worker.metrics().inbox_events == 2);
        assert(worker.metrics().shutdown_inbox_events == 0);
    }

    void test_worker_fires_timer()
    {
        std::atomic<std::size_t> fired{0};

        Worker worker(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{});
        worker.setTimerHandler(
            [&fired](TimerPayload&&)
            {
                fired.fetch_add(1, std::memory_order_release);
            }
        );

        const auto deadline = Clock::now() + std::chrono::milliseconds(50);
        assert(worker.trySchedule(deadline, AwaitTimeout{}));

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );

        // deadline 이전에는 fire 되지 않는다.
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(fired.load(std::memory_order_acquire) == 0);

        assert(waitUntil(
            [&]
            {
                return fired.load(std::memory_order_acquire) == 1;
            },
            std::chrono::milliseconds(2000)
        ));
        assert(Clock::now() >= deadline);

        worker.requestStop();
        th.join();

        assert(worker.metrics().timers_fired == 1);
        assert(worker.metrics().shutdown_timers_fired == 0);
    }

    void test_worker_drains_event_accepted_before_stop()
    {
        std::atomic<std::size_t> handled{0};

        Worker worker(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{});
        worker.setEventHandler(
            [&handled](WorkerEvent&&)
            {
                handled.fetch_add(1, std::memory_order_release);
            }
        );
        auto port = worker.bindInboxSource(WorkerId{1});

        assert(port.tryPush(makeEnvelope(1)) == InboxPushResult::Accepted);

        // stop 을 먼저 요청하므로 메인 루프 본문은 한 번도 돌지 않는다.
        // Accepted 된 event 는 resource teardown 전 quiescence phase가 처리한다.
        worker.requestStop();
        worker.run();

        assert(handled.load(std::memory_order_acquire) == 1);
        assert(worker.metrics().inbox_events == 1);
        assert(worker.metrics().shutdown_inbox_events == 0);
    }

    void test_worker_no_phase_starvation()
    {
        std::atomic<std::size_t> events_handled{0};
        std::atomic<std::size_t> timers_handled{0};

        Worker worker(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{});
        worker.setEventHandler(
            [&events_handled](WorkerEvent&&)
            {
                events_handled.fetch_add(1, std::memory_order_release);
            }
        );
        worker.setTimerHandler(
            [&timers_handled](TimerPayload&&)
            {
                timers_handled.fetch_add(1, std::memory_order_release);
            }
        );

        auto port = worker.bindInboxSource(WorkerId{1});

        const auto now = Clock::now();
        for (int i = 0; i < 5; ++i)
        {
            assert(worker.trySchedule(now + std::chrono::milliseconds(10 * (i + 1)), AwaitTimeout{}));
        }

        std::atomic<bool> stop_producer{false};
        std::thread worker_thread(
            [&]()
            {
                worker.run();
            }
        );

        // inbox 를 쉬지 않고 채우는 동안에도 timer phase 가 계속 실행되어야 한다.
        std::thread producer_thread(
            [&]()
            {
                std::uint32_t seq = 0;
                while (!stop_producer.load(std::memory_order_relaxed))
                {
                    const auto push_res = port.tryPush(makeEnvelope(seq++));
                    (void)push_res;
                    std::this_thread::yield();
                }
            }
        );

        // 부하가 걸린 상태에서, stop 하기 전에 두 phase 모두 진행했음을 관측한다.
        const bool both_progressed = waitUntil(
            [&]
            {
                return events_handled.load(std::memory_order_acquire) > 0 && timers_handled.load(std::memory_order_acquire) == 5;
            },
            std::chrono::milliseconds(3000)
        );

        stop_producer.store(true, std::memory_order_relaxed);
        producer_thread.join();
        worker.requestStop();
        worker_thread.join();

        assert(both_progressed);
        assert(worker.metrics().inbox_events > 0);
        assert(worker.metrics().timers_fired == 5);
        assert(worker.metrics().shutdown_timers_fired == 0);
    }

    void test_worker_inbox_full_rejects_without_blocking()
    {
        Worker worker(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{});
        auto port = worker.bindInboxSource(WorkerId{1});

        for (std::size_t i = 0; i < InboxLane::CAPACITY; ++i)
        {
            assert(port.tryPush(makeEnvelope(static_cast<std::uint32_t>(i))) == InboxPushResult::Accepted);
        }

        // 재려는 것은 정확한 지연이 아니라 mutex wait 나 condition wait 가 없다는 사실이다.
        const auto start = Clock::now();
        std::thread producer_thread(
            [&]()
            {
                for (int i = 0; i < 10'000; ++i)
                {
                    assert(port.tryPush(makeEnvelope(99999)) == InboxPushResult::Full);
                }
            }
        );

        producer_thread.join();
        const auto diff = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        assert(diff < std::chrono::milliseconds(100));
    }
}

void run_worker_loop_tests()
{
    test_worker_starts_and_stops_cleanly();
    test_worker_stop_before_run_returns_immediately();
    test_worker_processes_remote_event();
    test_main_loop_does_the_work_not_shutdown();
    test_worker_wakes_from_idle_poll();
    test_worker_fires_timer();
    test_worker_drains_event_accepted_before_stop();
    test_worker_no_phase_starvation();
    test_worker_inbox_full_rejects_without_blocking();
}
