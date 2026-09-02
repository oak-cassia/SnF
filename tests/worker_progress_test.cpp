#include "snf/worker/progress.hpp"
#include "snf/worker/worker.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <sched.h>
#include <sys/resource.h>
#include <thread>
#include <time.h>

namespace snf::worker
{
    struct WorkerProgressTestAccess
    {
        [[nodiscard]] static constexpr std::uint64_t pack(const WorkerPhase phase, const std::uint64_t epoch_micros) noexcept
        {
            return WorkerProgress::pack(phase, epoch_micros);
        }

        [[nodiscard]] static constexpr WorkerPhase unpackPhase(const std::uint64_t word) noexcept
        {
            return WorkerProgress::unpackPhase(word);
        }

        [[nodiscard]] static constexpr std::uint64_t unpackMicros(const std::uint64_t word) noexcept
        {
            return WorkerProgress::unpackMicros(word);
        }
    };
}

namespace
{
    using namespace std::chrono_literals;
    using Clock = std::chrono::steady_clock;
    using namespace snf::worker;

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

    [[nodiscard]] WorkerEnvelope envelope()
    {
        return WorkerEnvelope{
            .event =
                RemoteConnectionClose{
                    .connection = ConnectionRef{ConnectionId{1}, ConnectionGeneration{1}, WorkerId{0}},
                    .reason = CloseReason::Shutdown,
                },
            .charged_bytes = 1,
        };
    }

    [[nodiscard]] std::chrono::nanoseconds threadCpuNow()
    {
        timespec value{};
        assert(::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value) == 0);
        return std::chrono::seconds{value.tv_sec} + std::chrono::nanoseconds{value.tv_nsec};
    }

    void burnThreadCpu(const std::chrono::nanoseconds duration)
    {
        const auto started_at = threadCpuNow();
        while (threadCpuNow() - started_at < duration)
        {
            std::atomic_signal_fence(std::memory_order_seq_cst);
        }
    }

    [[nodiscard]] int firstAllowedCpu()
    {
        cpu_set_t allowed;
        CPU_ZERO(&allowed);
        assert(::sched_getaffinity(0, sizeof(allowed), &allowed) == 0);
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
        {
            if (CPU_ISSET(cpu, &allowed))
            {
                return cpu;
            }
        }
        assert(false && "process has no allowed CPU");
        return 0;
    }

    void pinCurrentThread(const int cpu)
    {
        cpu_set_t target;
        CPU_ZERO(&target);
        CPU_SET(cpu, &target);
        assert(::sched_setaffinity(0, sizeof(target), &target) == 0);
    }

    void test_progress_pack_round_trip_including_timestamp_boundary()
    {
        constexpr std::uint64_t max_timestamp = (std::uint64_t{1} << 60U) - 1;
        constexpr std::uint64_t word = WorkerProgressTestAccess::pack(WorkerPhase::Stopped, max_timestamp);
        static_assert(WorkerProgressTestAccess::unpackPhase(word) == WorkerPhase::Stopped);
        static_assert(WorkerProgressTestAccess::unpackMicros(word) == max_timestamp);

        WorkerProgress progress;
        const auto representable_micros = std::chrono::duration_cast<std::chrono::microseconds>(Clock::duration::max()).count();
        const WorkerProgress::TimePoint entered_at{std::chrono::duration_cast<Clock::duration>(std::chrono::microseconds{representable_micros})};
        progress.publish(WorkerPhase::Stopped, entered_at);
        const auto sample = progress.sample();

        assert(sample.phase == WorkerPhase::Stopped);
        assert(std::chrono::duration_cast<std::chrono::microseconds>(sample.entered_at.time_since_epoch()).count() == representable_micros);
    }

    void test_running_worker_progress_is_cross_thread_observable()
    {
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{});
        std::thread runner(
            [&worker]
            {
                worker.run();
            }
        );

        assert(waitUntil(
            [&worker]
            {
                const auto sample = worker.progress().sample();
                return sample.phase == WorkerPhase::PollWait && sample.entered_at != WorkerProgress::TimePoint{};
            },
            2s
        ));

        worker.requestStop();
        runner.join();

        assert(worker.progress().sample().phase == WorkerPhase::Stopped);
        assert(worker.metrics().phases[static_cast<std::size_t>(WorkerPhase::Starting)].entries == 1);
        assert(worker.metrics().phases[static_cast<std::size_t>(WorkerPhase::Stopped)].entries == 1);
        assert(worker.metrics().thread_execution_sample_failures == 0);
        for (const WorkerPhase phase :
             {WorkerPhase::Poll, WorkerPhase::Inbox, WorkerPhase::Timers, WorkerPhase::Db, WorkerPhase::Actors, WorkerPhase::Writes})
        {
            assert(worker.metrics().phases[static_cast<std::size_t>(phase)].voluntary_context_switches == 0);
        }
    }

    void test_phase_residence_and_other_phase_entry_gap_track_a_blocking_handler()
    {
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{});
        std::atomic<bool> handled{false};
        worker.setEventHandler(
            [&handled](WorkerEvent&&)
            {
                std::this_thread::sleep_for(100ms);
                handled.store(true, std::memory_order_release);
            }
        );
        auto port = worker.bindInboxSource(WorkerId{0});

        std::thread runner(
            [&worker]
            {
                worker.run();
            }
        );

        WorkerProgress::TimePoint first_poll_wait{};
        assert(waitUntil(
            [&worker, &first_poll_wait]
            {
                const auto sample = worker.progress().sample();
                if (sample.phase != WorkerPhase::PollWait)
                {
                    return false;
                }
                if (first_poll_wait == WorkerProgress::TimePoint{})
                {
                    first_poll_wait = sample.entered_at;
                    return false;
                }
                return sample.entered_at > first_poll_wait;
            },
            2s
        ));

        assert(port.tryPush(envelope()) == InboxPushResult::Accepted);
        assert(waitUntil(
            [&worker]
            {
                return worker.progress().sample().phase == WorkerPhase::Inbox;
            },
            2s
        ));
        assert(waitUntil(
            [&handled]
            {
                return handled.load(std::memory_order_acquire);
            },
            2s
        ));

        worker.requestStop();
        runner.join();

        const WorkerMetrics& metrics = worker.metrics();
        const auto& inbox = metrics.phases[static_cast<std::size_t>(WorkerPhase::Inbox)];
        const auto& timers = metrics.phases[static_cast<std::size_t>(WorkerPhase::Timers)];
        assert(inbox.max_residence >= 80ms);
        assert(inbox.voluntary_context_switches > 0);
        assert(inbox.max_wall_residence_voluntary_context_switches > 0);
        assert(inbox.max_cpu_residence < inbox.max_residence);
        assert(timers.max_entry_gap >= 80ms);
        assert(inbox.entries >= 2);
        assert(timers.entries >= 2);
    }

    void test_cpu_spin_increases_phase_cpu_residence_without_voluntary_switch()
    {
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{});
        std::atomic<bool> handled{false};
        worker.setEventHandler(
            [&handled](WorkerEvent&&)
            {
                burnThreadCpu(20ms);
                handled.store(true, std::memory_order_release);
            }
        );
        auto port = worker.bindInboxSource(WorkerId{0});
        std::thread runner(
            [&worker]
            {
                worker.run();
            }
        );

        assert(waitUntil(
            [&worker]
            {
                return worker.progress().sample().phase == WorkerPhase::PollWait;
            },
            2s
        ));
        assert(port.tryPush(envelope()) == InboxPushResult::Accepted);
        assert(waitUntil(
            [&handled]
            {
                return handled.load(std::memory_order_acquire);
            },
            2s
        ));

        worker.requestStop();
        runner.join();

        const auto& inbox = worker.metrics().phases[static_cast<std::size_t>(WorkerPhase::Inbox)];
        assert(inbox.max_cpu_residence >= 15ms);
        assert(inbox.max_residence >= 15ms);
        assert(inbox.voluntary_context_switches == 0);
    }

    void test_involuntary_scheduler_preemption_separates_wall_and_cpu_residence()
    {
        const int cpu = firstAllowedCpu();
        std::mutex competitor_mutex;
        std::condition_variable competitor_cv;
        bool release_competitor = false;
        std::atomic<bool> competitor_ready{false};
        std::thread competitor(
            [&]
            {
                pinCurrentThread(cpu);
                std::unique_lock lock{competitor_mutex};
                competitor_ready.store(true, std::memory_order_release);
                competitor_cv.wait(
                    lock,
                    [&release_competitor]
                    {
                        return release_competitor;
                    }
                );
                lock.unlock();
                const auto deadline = Clock::now() + 120ms;
                while (Clock::now() < deadline)
                {
                    std::atomic_signal_fence(std::memory_order_seq_cst);
                }
            }
        );
        assert(waitUntil(
            [&competitor_ready]
            {
                return competitor_ready.load(std::memory_order_acquire);
            },
            2s
        ));

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{});
        std::atomic<bool> handled{false};
        worker.setEventHandler(
            [&](WorkerEvent&&)
            {
                {
                    std::lock_guard lock{competitor_mutex};
                    release_competitor = true;
                }
                competitor_cv.notify_one();
                burnThreadCpu(10ms);
                handled.store(true, std::memory_order_release);
            }
        );
        auto port = worker.bindInboxSource(WorkerId{0});
        std::thread runner(
            [&worker, cpu]
            {
                pinCurrentThread(cpu);
                assert(::setpriority(PRIO_PROCESS, 0, 19) == 0);
                worker.run();
            }
        );

        assert(waitUntil(
            [&worker]
            {
                return worker.progress().sample().phase == WorkerPhase::PollWait;
            },
            2s
        ));
        assert(port.tryPush(envelope()) == InboxPushResult::Accepted);
        assert(waitUntil(
            [&handled]
            {
                return handled.load(std::memory_order_acquire);
            },
            2s
        ));

        worker.requestStop();
        runner.join();
        competitor.join();

        const auto& inbox = worker.metrics().phases[static_cast<std::size_t>(WorkerPhase::Inbox)];
        assert(inbox.max_residence >= 50ms);
        assert(inbox.max_cpu_residence >= 5ms);
        assert(inbox.max_cpu_residence < 30ms);
        assert(inbox.voluntary_context_switches == 0);
        assert(inbox.involuntary_context_switches > 0);
        assert(inbox.max_wall_residence_involuntary_context_switches > 0);
        assert(inbox.max_wall_residence_cpu < 30ms);
        std::cout << "worker_calibration.scheduler_preemption=wall_ns:" << inbox.max_residence.count()
                  << ",cpu_ns:" << inbox.max_wall_residence_cpu.count() << ",voluntary:" << inbox.max_wall_residence_voluntary_context_switches
                  << ",involuntary:" << inbox.max_wall_residence_involuntary_context_switches << '\n';
    }
}

void run_worker_progress_tests()
{
    test_progress_pack_round_trip_including_timestamp_boundary();
    test_running_worker_progress_is_cross_thread_observable();
    test_phase_residence_and_other_phase_entry_gap_track_a_blocking_handler();
    test_cpu_spin_increases_phase_cpu_residence_without_voluntary_switch();
    test_involuntary_scheduler_preemption_separates_wall_and_cpu_residence();
}
