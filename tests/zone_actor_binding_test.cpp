#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/zone_actor_binding.hpp"
#include "snf/server/zone_actor_ingress.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <mutex>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

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
            // ZoneResult carries a move-only TellActor payload, so the recorder
            // keeps the field this test asserts on rather than the whole result.
            std::vector<std::optional<snf::server::ZonePosition>> positions;
            std::vector<std::thread::id> threads;
        } recorded;

        const std::thread::id caller = std::this_thread::get_id();
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::ZoneActorBinding binding{snf::server::ZoneActorBindingConfig{
                                                  .actor = snf::server::ZoneActorConfig{.aoi_radius = 100},
                                                  .tick_budget = std::chrono::milliseconds{5},
                                                  .on_result =
                                                      [&recorded](const snf::server::ZoneInboundCommand&, const snf::server::ZoneResult& result)
                                                  {
                                                      std::lock_guard lock{recorded.mutex};
                                                      recorded.positions.push_back(result.position);
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
            assert(recorded.positions.size() >= 2);
            assert((recorded.positions[0] == snf::server::ZonePosition{.x = 1, .y = 2}));
            assert((recorded.positions[1] == snf::server::ZonePosition{.x = 3, .y = 4}));
            assert(recorded.threads[0] != caller);
            assert(recorded.threads[0] == recorded.threads[1]);
        }

        const auto stats = runtime.getStats().workers.front();
        assert(stats.accepted >= 2);
        assert(stats.processed >= 2);
        assert(stats.evicted_actors >= 1);
        assert(completion.drained.load() == 1);
        assert(completion.failed.load() == 0);
        assert(lifecycle.releaseCount() == 2);
        assert(lifecycle.terminalCount() == 2);
        assert(lifecycle.admissionRejectionCount() == 0);
    }

    void test_empty_zone_passivates_automatically_when_last_player_leaves()
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
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats().workers.front();
        assert(stats.processed >= 2);
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
        snf::runtime::ActorRuntime runtime{snf::runtime::ActorRuntimeConfig{
                                               .worker_count = 1,
                                               .queue_capacity_per_worker = 16,
                                               .max_in_flight_operations_per_worker = 2,
                                               .on_worker_start = {},
                                               .on_before_dispatch =
                                                   [&first_dispatch_started, release, &gated](std::size_t, const snf::runtime::ActorKey&, const snf::runtime::ActorSubmission&)
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
        assert(first_dispatch.wait_for(std::chrono::seconds{1}) == std::future_status::ready);

        assert(post_player(snf::server::LeaveZoneCommand{
                   .player = player,
                   .route_epoch = 1,
               }) == snf::runtime::PostResult::Accepted);
        assert(post_player(snf::server::EnterZoneCommand{
                   .player = player,
                   .route_epoch = 2,
                   .position = {.x = 3, .y = 4},
               }) == snf::runtime::PostResult::Accepted);
        assert(post_player(snf::server::LeaveZoneCommand{
                   .player = player,
                   .route_epoch = 2,
               }) == snf::runtime::PostResult::Accepted);

        release_first_dispatch.set_value();
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats().workers.front();
        assert(stats.processed >= 4);
        assert(stats.evicted_actors == 1);
        assert(stats.actor_count == 0);
        assert(completion.drained.load() == 1);
        assert(completion.failed.load() == 0);
    }

    void test_zone_binding_schedules_ticks_and_records_execution()
    {
        snf::server::ZoneActorBinding binding{snf::server::ZoneActorBindingConfig{
            .actor =
                snf::server::ZoneActorConfig{
                    .aoi_radius = 100,
                    .tick_interval = 10ms,
                },
            .tick_budget = std::chrono::nanoseconds::zero(),
            .on_result = {},
        }};
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{snf::runtime::ActorRuntimeConfig{
                                               .worker_count = 1,
                                               .queue_capacity_per_worker = 16,
                                               .max_in_flight_operations_per_worker = 2,
                                               .on_worker_start = {},
                                               .on_before_dispatch = {},
                                               .on_worker_failure = {},
                                           },
                                           completion};
        runtime.registerBinding(binding);
        snf::server::ZoneActorIngress ingress{runtime, binding};
        runtime.start();

        const snf::server::ZoneId zone{.value = 14};
        const snf::server::PlayerId player{.value = 24};

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

        // Let simulation tick several times
        std::this_thread::sleep_for(50ms);

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

        runtime.close();
        runtime.join();

        const auto stats = binding.stats();
        assert(stats.command_execution_nanoseconds.sample_count >= 2);
        assert(stats.tick_execution_nanoseconds.sample_count >= 2);
        assert(stats.tick_overruns >= 2);
    }
}

void run_zone_actor_binding_tests()
{
    test_zone_binding_runs_typed_commands_on_its_owning_worker();
    test_empty_zone_passivates_automatically_when_last_player_leaves();
    test_passivation_never_discards_an_already_accepted_reentry();
    test_zone_binding_schedules_ticks_and_records_execution();
}
