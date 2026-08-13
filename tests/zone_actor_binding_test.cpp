#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/zone_actor_binding.hpp"
#include "snf/server/zone_actor_ingress.hpp"

#include <atomic>
#include <cassert>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

namespace
{
    class RecordingCompletion final : public snf::runtime::RuntimeCompletionSink
    {
    public:
        void notifyDrained(snf::runtime::RuntimeId runtime) noexcept override
        {
            assert(runtime == snf::runtime::RuntimeId::Logic);
            ++drained;
        }

        void notifyFailed(snf::runtime::RuntimeId runtime) noexcept override
        {
            assert(runtime == snf::runtime::RuntimeId::Logic);
            ++failed;
        }

        std::atomic<int> drained{0};
        std::atomic<int> failed{0};
    };

    void test_zone_binding_runs_typed_commands_on_its_owning_worker()
    {
        struct Recorded
        {
            std::mutex mutex;
            std::vector<snf::server::ZoneResult> results;
            std::vector<std::thread::id> threads;
        } recorded;

        const std::thread::id caller = std::this_thread::get_id();
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::ZoneActorBinding binding{
            snf::server::ZoneActorBindingConfig{
                .actor = snf::server::ZoneActorConfig{.aoi_radius = 100},
                .tick_budget = std::chrono::milliseconds{5},
                .on_result =
                    [&recorded](const snf::server::ZoneInboundCommand&,
                                const snf::server::ZoneResult& result)
                {
                    std::lock_guard lock{recorded.mutex};
                    recorded.results.push_back(result);
                    recorded.threads.push_back(std::this_thread::get_id());
                },
            },
            lifecycle};
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{snf::runtime::ActorRuntimeConfig{
                                               .worker_count = 1,
                                               .queue_capacity_per_worker = 8,
                                               .max_in_flight_operations_per_worker = 2,
                                               .on_worker_start = {},
                                               .on_before_dispatch = {},
                                               .on_worker_failure = {},
                                           },
                                           completion};
        runtime.registerBinding(binding);
        snf::server::ZoneActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const snf::server::ZoneId zone{.value = 10};
        const snf::server::PlayerId player{.value = 20};
        assert(ingress.tryPost(snf::server::ZoneInboundCommand{
                   .zone = zone,
                   .command =
                       snf::server::EnterZoneCommand{
                           .player = player,
                           .route_epoch = 1,
                           .position = {.x = 1, .y = 2},
                       },
                   .reply =
                       snf::server::ZoneReplyContext{
                           .connection = {.descriptor = 4, .generation = 1},
                           .request_id = 1,
                           .kind = snf::server::ZoneReplyKind::Entered,
                       },
                   .handoff = std::nullopt,
               }) == snf::runtime::PostResult::Accepted);
        assert(ingress.tryPost(snf::server::ZoneInboundCommand{
                   .zone = zone,
                   .command =
                       snf::server::MoveInZoneCommand{
                           .player = player,
                           .route_epoch = 1,
                           .position = {.x = 3, .y = 4},
                       },
                   .reply =
                       snf::server::ZoneReplyContext{
                           .connection = {.descriptor = 4, .generation = 1},
                           .request_id = 2,
                           .kind = snf::server::ZoneReplyKind::Moved,
                       },
                   .handoff = std::nullopt,
               }) == snf::runtime::PostResult::Accepted);
        assert(ingress.tryPassivate(zone) == snf::runtime::PostResult::Accepted);
        runtime.close();
        runtime.join();

        {
            std::lock_guard lock{recorded.mutex};
            assert(recorded.results.size() == 2);
            assert((recorded.results[0].position == snf::server::ZonePosition{.x = 1, .y = 2}));
            assert((recorded.results[1].position == snf::server::ZonePosition{.x = 3, .y = 4}));
            assert(recorded.threads[0] != caller);
            assert(recorded.threads[0] == recorded.threads[1]);
        }

        const auto stats = runtime.getStats().workers.front();
        assert(stats.accepted == 2);
        assert(stats.processed == 2);
        assert(stats.evicted_actors == 1);
        assert(completion.drained.load() == 1);
        assert(completion.failed.load() == 0);
        assert(lifecycle.releaseCount() == 2);
        assert(lifecycle.terminalCount() == 2);
        assert(lifecycle.admissionRejectionCount() == 0);
    }

    void test_empty_zone_passivates_after_its_timer_is_cancelled()
    {
        snf::server::ZoneActorBinding binding;
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{snf::runtime::ActorRuntimeConfig{
                                               .worker_count = 1,
                                               .queue_capacity_per_worker = 8,
                                               .max_in_flight_operations_per_worker = 2,
                                               .on_worker_start = {},
                                               .on_before_dispatch = {},
                                               .on_worker_failure = {},
                                           },
                                           completion};
        runtime.registerBinding(binding);
        snf::server::ZoneActorIngress ingress{runtime, binding};
        runtime.start();

        const snf::server::ZoneId zone{.value = 12};
        const snf::server::PlayerId player{.value = 22};
        const snf::server::TimerId timer{.value = 32};
        assert(ingress.tryPostTimerCommand(zone,
                                           snf::server::ArmZoneSimulationTimer{.timer = timer}) ==
               snf::runtime::PostResult::Accepted);
        assert(ingress.tryPost(snf::server::ZoneInboundCommand{
                   .zone = zone,
                   .command =
                       snf::server::EnterZoneCommand{
                           .player = player,
                           .route_epoch = 1,
                           .position = {.x = 1, .y = 2},
                       },
                   .reply = std::nullopt,
                   .handoff = std::nullopt,
               }) == snf::runtime::PostResult::Accepted);
        assert(ingress.tryPost(snf::server::ZoneInboundCommand{
                   .zone = zone,
                   .command =
                       snf::server::LeaveZoneCommand{
                           .player = player,
                           .route_epoch = 1,
                       },
                   .reply = std::nullopt,
                   .handoff = std::nullopt,
               }) == snf::runtime::PostResult::Accepted);
        assert(ingress.tryPostTimerCommand(
                   zone, snf::server::CancelZoneSimulationTimer{.timer = timer}) ==
               snf::runtime::PostResult::Accepted);
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats().workers.front();
        assert(stats.processed == 4);
        assert(stats.evicted_actors == 1);
        assert(stats.actor_count == 0);
        assert(completion.drained.load() == 1);
        assert(completion.failed.load() == 0);
    }

    void test_passivation_never_discards_an_already_accepted_reentry()
    {
        snf::server::ZoneActorBinding binding;
        RecordingCompletion completion;
        std::promise<void> first_dispatch_started;
        auto first_dispatch = first_dispatch_started.get_future();
        std::promise<void> release_first_dispatch;
        const auto release = release_first_dispatch.get_future().share();
        std::atomic<bool> gated{false};
        snf::runtime::ActorRuntime runtime{
            snf::runtime::ActorRuntimeConfig{
                .worker_count = 1,
                .queue_capacity_per_worker = 16,
                .max_in_flight_operations_per_worker = 2,
                .on_worker_start = {},
                .on_before_dispatch =
                    [&first_dispatch_started, release, &gated](std::size_t,
                                                               const snf::runtime::ActorKey&,
                                                               const snf::runtime::ActorSubmission&)
                {
                    if (!gated.exchange(true))
                    {
                        first_dispatch_started.set_value();
                        release.wait();
                    }
                },
                .on_worker_failure = {},
            },
            completion};
        runtime.registerBinding(binding);
        snf::server::ZoneActorIngress ingress{runtime, binding};
        runtime.start();

        const snf::server::ZoneId zone{.value = 13};
        const snf::server::PlayerId player{.value = 23};
        const snf::server::TimerId first_timer{.value = 33};
        const snf::server::TimerId second_timer{.value = 34};
        assert(ingress.tryPostTimerCommand(
                   zone, snf::server::ArmZoneSimulationTimer{.timer = first_timer}) ==
               snf::runtime::PostResult::Accepted);
        assert(first_dispatch.wait_for(std::chrono::seconds{1}) == std::future_status::ready);

        const auto post_player = [&ingress, zone](snf::server::ZoneCommand command)
        {
            return ingress.tryPost(snf::server::ZoneInboundCommand{
                .zone = zone,
                .command = std::move(command),
                .reply = std::nullopt,
                .handoff = std::nullopt,
            });
        };
        assert(post_player(snf::server::EnterZoneCommand{
                   .player = player,
                   .route_epoch = 1,
                   .position = {.x = 1, .y = 2},
               }) == snf::runtime::PostResult::Accepted);
        assert(post_player(snf::server::LeaveZoneCommand{
                   .player = player,
                   .route_epoch = 1,
               }) == snf::runtime::PostResult::Accepted);
        assert(ingress.tryPostTimerCommand(
                   zone, snf::server::CancelZoneSimulationTimer{.timer = first_timer}) ==
               snf::runtime::PostResult::Accepted);
        assert(ingress.tryPostTimerCommand(
                   zone, snf::server::ArmZoneSimulationTimer{.timer = second_timer}) ==
               snf::runtime::PostResult::Accepted);
        assert(post_player(snf::server::EnterZoneCommand{
                   .player = player,
                   .route_epoch = 2,
                   .position = {.x = 3, .y = 4},
               }) == snf::runtime::PostResult::Accepted);
        assert(post_player(snf::server::LeaveZoneCommand{
                   .player = player,
                   .route_epoch = 2,
               }) == snf::runtime::PostResult::Accepted);
        assert(ingress.tryPostTimerCommand(
                   zone, snf::server::CancelZoneSimulationTimer{.timer = second_timer}) ==
               snf::runtime::PostResult::Accepted);

        release_first_dispatch.set_value();
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats().workers.front();
        assert(stats.accepted == 8);
        assert(stats.processed == 8);
        assert(stats.evicted_actors == 1);
        assert(stats.actor_count == 0);
        assert(completion.drained.load() == 1);
        assert(completion.failed.load() == 0);
    }

    void test_zone_binding_records_tick_execution_and_budget_overrun()
    {
        snf::server::ZoneActorBinding binding{snf::server::ZoneActorBindingConfig{
            .actor = {},
            .tick_budget = std::chrono::nanoseconds::zero(),
            .on_result = {},
        }};
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{snf::runtime::ActorRuntimeConfig{
                                               .worker_count = 1,
                                               .queue_capacity_per_worker = 4,
                                               .max_in_flight_operations_per_worker = 1,
                                               .on_worker_start = {},
                                               .on_before_dispatch = {},
                                               .on_worker_failure = {},
                                           },
                                           completion};
        runtime.registerBinding(binding);
        snf::server::ZoneActorIngress ingress{runtime, binding};
        runtime.start();

        const snf::server::ZoneId zone{.value = 14};
        const snf::server::TimerId timer{.value = 35};
        assert(ingress.tryPostTimerCommand(zone,
                                           snf::server::ArmZoneSimulationTimer{.timer = timer}) ==
               snf::runtime::PostResult::Accepted);
        assert(ingress.tryPostTimerCommand(zone,
                                           snf::server::ZoneSimulationTick{
                                               .timer = timer,
                                               .tick = 1,
                                           }) == snf::runtime::PostResult::Accepted);
        assert(ingress.tryPostTimerCommand(
                   zone, snf::server::CancelZoneSimulationTimer{.timer = timer}) ==
               snf::runtime::PostResult::Accepted);
        runtime.close();
        runtime.join();

        const auto stats = binding.stats();
        assert(stats.command_execution_nanoseconds.sample_count == 3);
        assert(stats.tick_execution_nanoseconds.sample_count == 1);
        assert(stats.tick_overruns == 1);
    }
}

void run_zone_actor_binding_tests()
{
    test_zone_binding_runs_typed_commands_on_its_owning_worker();
    test_empty_zone_passivates_after_its_timer_is_cancelled();
    test_passivation_never_discards_an_already_accepted_reentry();
    test_zone_binding_records_tick_execution_and_budget_overrun();
}
