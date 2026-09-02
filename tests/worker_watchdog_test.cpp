#include "snf/worker/actor_envelope.hpp"
#include "snf/worker/watchdog.hpp"
#include "snf/worker/worker.hpp"
#include "snf/worker/worker_group.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>

namespace
{
    using namespace std::chrono_literals;
    using Clock = std::chrono::steady_clock;
    using namespace snf::worker;

    struct BlockingPayload
    {
    };
}

namespace snf::worker
{
    template <> struct ActorPayloadTraits<BlockingPayload>
    {
        static constexpr std::uint32_t TAG = 1;
        [[nodiscard]] static constexpr std::uint64_t calculateCharge(const BlockingPayload&) noexcept
        {
            return 1;
        }
    };
}

namespace
{
    using WatchdogRegistry = ActorPayloadRegistry<BlockingPayload>;

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

    [[nodiscard]] WorkerWatchdogConfig testConfig()
    {
        return WorkerWatchdogConfig{
            .sample_interval = 5ms,
            .phase_stall_threshold = 50ms,
            .poll_wait_stall_threshold = 0ms,
        };
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

    class BlockingActor final : public ActorInstance
    {
    public:
        BlockingActor(std::atomic<bool>& entered, std::atomic<bool>& completed)
            : _entered(entered)
            , _completed(completed)
        {
        }

        [[nodiscard]] TurnResult dispatch(ActorEnvelope&&, const ActorTurnContext&) override
        {
            _entered.store(true, std::memory_order_release);
            std::this_thread::sleep_for(400ms);
            _completed.store(true, std::memory_order_release);
            return CompletedTurn{.effects = EffectBatch{}};
        }

    private:
        std::atomic<bool>& _entered;
        std::atomic<bool>& _completed;
    };

    class BlockingActorFactory final : public ActorFactory
    {
    public:
        BlockingActorFactory(std::atomic<bool>& entered, std::atomic<bool>& completed)
            : _entered(entered)
            , _completed(completed)
        {
        }

        [[nodiscard]] ActorConstructionResult construct(ActorKey) override
        {
            return ActorConstructionResult::ready(std::make_unique<BlockingActor>(_entered, _completed));
        }

    private:
        std::atomic<bool>& _entered;
        std::atomic<bool>& _completed;
    };

    void test_watchdog_reports_zero_stalls_for_an_idle_worker()
    {
        WorkerBudgets budgets = WorkerBudgets::defaults();
        budgets.sample_phase_execution = true;
        Worker worker(WorkerId{0}, 1, budgets, WorkerInboxConfig{});
        WorkerWatchdog watchdog(testConfig(), budgets, {{.worker = WorkerId{0}, .progress = &worker.progress()}});
        watchdog.start();
        std::thread runner(
            [&worker]
            {
                worker.run();
            }
        );

        std::this_thread::sleep_for(120ms);
        worker.requestStop();
        runner.join();
        watchdog.stop();

        assert(watchdog.metrics().samples_taken.load(std::memory_order_relaxed) > 0);
        assert(watchdog.metrics().active_stall_episodes.load(std::memory_order_relaxed) == 0);
    }

    void test_watchdog_fires_once_on_a_blocking_event_handler()
    {
        WorkerBudgets budgets = WorkerBudgets::defaults();
        budgets.sample_phase_execution = true;
        Worker worker(WorkerId{0}, 1, budgets, WorkerInboxConfig{});
        std::atomic<bool> entered{false};
        std::atomic<bool> completed{false};
        worker.setEventHandler(
            [&entered, &completed](WorkerEvent&&)
            {
                entered.store(true, std::memory_order_release);
                std::this_thread::sleep_for(400ms);
                completed.store(true, std::memory_order_release);
            }
        );
        auto port = worker.bindInboxSource(WorkerId{0});

        std::atomic<std::uint64_t> callbacks{0};
        WorkerWatchdog watchdog(
            testConfig(),
            budgets,
            {{.worker = WorkerId{0}, .progress = &worker.progress()}},
            [&callbacks](const WorkerStallReport&)
            {
                callbacks.fetch_add(1, std::memory_order_relaxed);
            }
        );
        watchdog.start();
        std::thread runner(
            [&worker]
            {
                worker.run();
            }
        );

        assert(port.tryPush(envelope()) == InboxPushResult::Accepted);
        assert(waitUntil(
            [&entered]
            {
                return entered.load(std::memory_order_acquire);
            },
            2s
        ));
        assert(waitUntil(
            [&completed]
            {
                return completed.load(std::memory_order_acquire);
            },
            2s
        ));

        worker.requestStop();
        runner.join();
        watchdog.stop();

        assert(watchdog.metrics().active_stall_episodes.load(std::memory_order_relaxed) == 1);
        assert(watchdog.metrics().last_stall_phase.load(std::memory_order_relaxed) == static_cast<std::uint32_t>(WorkerPhase::Inbox));
        assert(watchdog.metrics().longest_stall_ns.load(std::memory_order_relaxed) >= static_cast<std::uint64_t>(300ms / 1ns));
        assert(callbacks.load(std::memory_order_relaxed) == 1);
        const auto& inbox = worker.metrics().phases[static_cast<std::size_t>(WorkerPhase::Inbox)];
        assert(inbox.voluntary_context_switches > 0);
        assert(inbox.max_cpu_residence < inbox.max_residence);
    }

    void test_watchdog_attributes_a_blocking_turn_to_the_actor_phase()
    {
        WorkerBudgets budgets = WorkerBudgets::defaults();
        budgets.sample_phase_execution = true;
        std::atomic<bool> entered{false};
        std::atomic<bool> completed{false};
        BlockingActorFactory factory(entered, completed);
        WorkerActorConfig actor_config{};
        actor_config.actor_table_capacity = 4;
        actor_config.max_mailbox_messages_per_actor = 4;
        actor_config.max_mailbox_bytes_per_actor = 1024;
        actor_config.max_mailbox_messages_total = 8;
        actor_config.max_mailbox_bytes_total = 2048;
        Worker worker(WorkerId{0}, 1, budgets, WorkerInboxConfig{}, actor_config, factory);
        assert(
            worker.tryDeliverLocal(ActorKey{.kind = ActorKind::Player, .entity = 1}, WatchdogRegistry::create(BlockingPayload{})) ==
            DeliveryResult::Accepted
        );

        WorkerWatchdog watchdog(testConfig(), budgets, {{.worker = WorkerId{0}, .progress = &worker.progress()}});
        watchdog.start();
        std::thread runner(
            [&worker]
            {
                worker.run();
            }
        );
        assert(waitUntil(
            [&entered]
            {
                return entered.load(std::memory_order_acquire);
            },
            2s
        ));
        assert(waitUntil(
            [&completed]
            {
                return completed.load(std::memory_order_acquire);
            },
            2s
        ));

        worker.requestStop();
        runner.join();
        watchdog.stop();

        assert(watchdog.metrics().active_stall_episodes.load(std::memory_order_relaxed) == 1);
        assert(watchdog.metrics().last_stall_phase.load(std::memory_order_relaxed) == static_cast<std::uint32_t>(WorkerPhase::Actors));
        assert(watchdog.metrics().longest_stall_ns.load(std::memory_order_relaxed) >= static_cast<std::uint64_t>(300ms / 1ns));
        const auto& actors = worker.metrics().phases[static_cast<std::size_t>(WorkerPhase::Actors)];
        assert(actors.voluntary_context_switches > 0);
        assert(actors.max_cpu_residence < actors.max_residence);
    }

    void test_worker_group_owns_an_opt_in_watchdog()
    {
        WorkerGroupConfig config;
        config.port = 0;
        config.watchdog = WorkerWatchdogConfig{};
        WorkerGroup group(config);
        assert(group.watchdogMetrics() != nullptr);

        group.start();
        std::this_thread::sleep_for(30ms);
        group.requestStop();
        group.join();

        assert(group.watchdogMetrics()->samples_taken.load(std::memory_order_relaxed) > 0);
        assert(group.watchdogMetrics()->active_stall_episodes.load(std::memory_order_relaxed) == 0);
    }
}

void run_worker_watchdog_tests()
{
    test_watchdog_reports_zero_stalls_for_an_idle_worker();
    test_watchdog_fires_once_on_a_blocking_event_handler();
    test_watchdog_attributes_a_blocking_turn_to_the_actor_phase();
    test_worker_group_owns_an_opt_in_watchdog();
}
