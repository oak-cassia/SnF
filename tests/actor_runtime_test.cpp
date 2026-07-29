#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/actor_runtime.hpp"
#include "snf/server/outbound_action.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/protocol_player_effect_sink.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <latch>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    using namespace std::chrono_literals;

    class QueueOutboundSink final : public snf::server::OutboundSink
    {
    public:
        explicit QueueOutboundSink(
            snf::runtime::BoundedQueue<snf::server::OutboundAction>& actions) noexcept
            : _actions(actions)
        {
        }

        [[nodiscard]] bool publish(snf::server::OutboundAction action,
                                   const std::stop_token stop_token) override
        {
            return _actions.push(std::move(action), stop_token);
        }

    private:
        snf::runtime::BoundedQueue<snf::server::OutboundAction>& _actions;
    };

    class RecordingRuntimeCompletion final : public snf::server::RuntimeCompletionSink
    {
    public:
        void notifyDrained(const snf::server::RuntimeId runtime) noexcept override
        {
            assert(runtime == snf::server::RuntimeId::Player);
            drained_count.fetch_add(1);
        }

        void notifyFailed(const snf::server::RuntimeId runtime) noexcept override
        {
            assert(runtime == snf::server::RuntimeId::Player);
            failed_count.fetch_add(1);
        }

        std::atomic<int> drained_count{0};
        std::atomic<int> failed_count{0};
    };

    struct RuntimeDependencies
    {
        explicit RuntimeDependencies(const std::size_t outbound_capacity)
            : outbound(outbound_capacity)
            , raw_outbound_sink(outbound)
            , outbound_sink(raw_outbound_sink)
        {
        }

        snf::runtime::BoundedQueue<snf::server::OutboundAction> outbound;
        QueueOutboundSink raw_outbound_sink;
        snf::server::ProtocolPlayerEffectSink outbound_sink;
        RecordingRuntimeCompletion completion;
    };

    snf::server::InboundCommand make_command(const std::uint64_t actor_id,
                                             const std::uint32_t request_id)
    {
        return snf::server::InboundCommand{
            .actor = snf::server::ProvisionalActorId{.value = actor_id},
            .connection =
                snf::net::ConnectionId{
                    .descriptor = static_cast<int>(42 + actor_id),
                    .generation = actor_id,
                },
            .command =
                snf::server::PingCommand{
                    .request_id = request_id,
                    .payload = {},
                },
        };
    }

    snf::server::ConnectionClosed make_closed(const std::uint64_t actor_id)
    {
        return snf::server::ConnectionClosed{
            .connection =
                snf::net::ConnectionId{
                    .descriptor = static_cast<int>(42 + actor_id),
                    .generation = actor_id,
                },
            .cause = snf::server::ConnectionCloseCause::PeerClosed,
        };
    }

    struct CommandRunResult
    {
        std::vector<snf::server::OutboundAction> actions;
        int drained_count{0};
        int failed_count{0};
    };

    CommandRunResult run_ping_commands(std::vector<snf::server::InboundCommand> commands)
    {
        RuntimeDependencies dependencies{16};
        auto config = snf::server::ActorRuntimeConfig{snf::server::RuntimeId::Player};
        config.worker_count = 2;
        config.queue_capacity_per_worker = 8;
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        for (auto& command : commands)
        {
            assert(runtime.tryPost(std::move(command)) == snf::server::PostResult::Accepted);
        }
        runtime.close();
        runtime.join();

        CommandRunResult result;
        while (auto action = dependencies.outbound.tryPop())
        {
            result.actions.push_back(std::move(*action));
        }
        result.drained_count = dependencies.completion.drained_count.load();
        result.failed_count = dependencies.completion.failed_count.load();
        return result;
    }

    snf::server::ActorRuntimeConfig
    with_before_command(std::function<void(const snf::server::PlayerCommand&)> before_command)
    {
        auto config = snf::server::ActorRuntimeConfig{snf::server::RuntimeId::Player};
        config.worker_count = 2;
        config.queue_capacity_per_worker = 8;
        config.on_before_command =
            [before_command = std::move(before_command)](std::size_t,
                                                         snf::server::ProvisionalActorId,
                                                         const snf::server::PlayerCommand& command)
        { before_command(command); };
        return config;
    }

    void test_rejects_invalid_configuration_and_posts_only_after_start()
    {
        RuntimeDependencies dependencies{8};

        bool rejected_zero_workers = false;
        try
        {
            auto config = snf::server::ActorRuntimeConfig{snf::server::RuntimeId::Player};
            config.worker_count = 0;
            config.queue_capacity_per_worker = 1;
            [[maybe_unused]] snf::server::ActorRuntime invalid{
                config, dependencies.outbound_sink, dependencies.completion};
        }
        catch (const std::invalid_argument&)
        {
            rejected_zero_workers = true;
        }
        assert(rejected_zero_workers);

        bool rejected_zero_capacity = false;
        try
        {
            auto config = snf::server::ActorRuntimeConfig{snf::server::RuntimeId::Player};
            config.worker_count = 1;
            config.queue_capacity_per_worker = 0;
            [[maybe_unused]] snf::server::ActorRuntime invalid{
                config, dependencies.outbound_sink, dependencies.completion};
        }
        catch (const std::invalid_argument&)
        {
            rejected_zero_capacity = true;
        }
        assert(rejected_zero_capacity);

        auto config = snf::server::ActorRuntimeConfig{snf::server::RuntimeId::Player};
        config.worker_count = 2;
        config.queue_capacity_per_worker = 2;
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Closed);
        assert(runtime.workerCount() == 2);
        assert(runtime.workerIndexFor(snf::server::ProvisionalActorId{.value = 0}) == 0);
        assert(runtime.workerIndexFor(snf::server::ProvisionalActorId{.value = 1}) == 1);
        assert(runtime.workerIndexFor(snf::server::ProvisionalActorId{.value = 11}) == 1);
        assert(runtime.workerIndexFor(snf::server::ProvisionalActorId{
                   .value = std::numeric_limits<std::uint64_t>::max()}) == 1);

        runtime.start();
        assert(runtime.tryPost(make_command(0, 2)) == snf::server::PostResult::Accepted);
        runtime.close();
        runtime.close();
        assert(runtime.tryPost(make_command(0, 3)) == snf::server::PostResult::Closed);
        runtime.join();
        assert(dependencies.completion.drained_count.load() == 1);
        assert(dependencies.completion.failed_count.load() == 0);

        bool rejected_restart = false;
        try
        {
            runtime.start();
        }
        catch (const std::logic_error&)
        {
            rejected_restart = true;
        }
        assert(rejected_restart);
    }

    void test_turns_ping_into_identical_pong_and_preserves_actor_order()
    {
        const auto result = run_ping_commands({
            make_command(7, 100),
            make_command(7, 101),
            make_command(7, 102),
        });

        assert(result.actions.size() == 3);
        for (std::uint32_t index = 0; index < 3; ++index)
        {
            const auto* send = std::get_if<snf::server::SendFrame>(&result.actions[index]);
            assert(send != nullptr);
            assert(send->connection.generation == 7);
            assert(send->frame.type == snf::protocol::MessageType::Pong);
            assert(send->frame.request_id == 100 + index);
        }
        assert(result.drained_count == 1);
        assert(result.failed_count == 0);
    }

    void test_serializes_same_actor_execution()
    {
        struct State
        {
            std::atomic<int> active{0};
            std::atomic<int> maximum_active{0};
            std::mutex mutex;
            std::vector<std::uint32_t> requests;
        };
        const auto state = std::make_shared<State>();

        RuntimeDependencies dependencies{8};
        auto config = with_before_command(
            [state](const snf::server::PlayerCommand& request)
            {
                const int active = state->active.fetch_add(1) + 1;
                int observed_maximum = state->maximum_active.load();
                while (observed_maximum < active &&
                       !state->maximum_active.compare_exchange_weak(observed_maximum, active))
                {
                }

                std::this_thread::sleep_for(2ms);
                {
                    std::lock_guard lock{state->mutex};
                    state->requests.push_back(snf::server::requestId(request));
                }
                state->active.fetch_sub(1);
            });
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(4, 1)) == snf::server::PostResult::Accepted);
        assert(runtime.tryPost(make_command(4, 2)) == snf::server::PostResult::Accepted);
        assert(runtime.tryPost(make_command(4, 3)) == snf::server::PostResult::Accepted);
        runtime.close();
        runtime.join();

        assert(state->maximum_active.load() == 1);
        assert(state->requests == std::vector<std::uint32_t>({1, 2, 3}));
    }

    void test_runs_different_shards_in_parallel()
    {
        struct State
        {
            std::atomic<int> entered{0};
            std::atomic<int> active{0};
            std::atomic<int> maximum_active{0};
            std::promise<void> both_entered;
            std::latch release{1};
        };
        const auto state = std::make_shared<State>();
        const auto both_entered = state->both_entered.get_future();

        RuntimeDependencies dependencies{8};
        auto config = with_before_command(
            [state](const snf::server::PlayerCommand&)
            {
                const int active = state->active.fetch_add(1) + 1;
                int observed_maximum = state->maximum_active.load();
                while (observed_maximum < active &&
                       !state->maximum_active.compare_exchange_weak(observed_maximum, active))
                {
                }

                if (state->entered.fetch_add(1) + 1 == 2)
                {
                    state->both_entered.set_value();
                }
                state->release.wait();
                state->active.fetch_sub(1);
            });
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Accepted);
        assert(runtime.tryPost(make_command(1, 2)) == snf::server::PostResult::Accepted);
        assert(both_entered.wait_for(1s) == std::future_status::ready);
        assert(state->maximum_active.load() == 2);

        state->release.count_down();
        runtime.close();
        runtime.join();
    }

    void test_full_shard_does_not_block_another_shard()
    {
        struct State
        {
            std::atomic<bool> started{false};
            std::promise<void> first_handler_started;
            std::shared_future<void> release;
        };
        const auto state = std::make_shared<State>();
        std::promise<void> release_promise;
        state->release = release_promise.get_future().share();
        const auto first_handler_started = state->first_handler_started.get_future();

        RuntimeDependencies dependencies{8};
        auto config = with_before_command(
            [state](const snf::server::PlayerCommand&)
            {
                if (!state->started.exchange(true))
                {
                    state->first_handler_started.set_value();
                }
                state->release.wait();
            });
        config.queue_capacity_per_worker = 2;
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Accepted);
        assert(first_handler_started.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(0, 2)) == snf::server::PostResult::Accepted);
        assert(runtime.getStats().workers[0].queue_depth == 2);
        assert(runtime.tryPost(make_command(0, 3)) == snf::server::PostResult::Full);
        assert(runtime.tryPost(make_command(1, 4)) == snf::server::PostResult::Accepted);

        release_promise.set_value();
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats();
        assert(stats.workers[0].accepted == 2);
        assert(stats.workers[0].rejected_full == 1);
        assert(stats.workers[0].queue_high_water_mark == 2);
        assert(stats.workers[1].accepted == 1);
    }

    void test_drains_all_workers_before_notifying_runtime_completion_once()
    {
        const auto result = run_ping_commands({
            make_command(0, 1),
            make_command(1, 2),
        });

        assert(result.actions.size() == 2);
        assert(std::holds_alternative<snf::server::SendFrame>(result.actions[0]));
        assert(std::holds_alternative<snf::server::SendFrame>(result.actions[1]));
        assert(result.drained_count == 1);
        assert(result.failed_count == 0);
    }

    void test_cancel_discards_queued_commands()
    {
        struct State
        {
            std::atomic<int> handled{0};
            std::promise<void> first_handler_started;
            std::shared_future<void> release;
        };
        const auto state = std::make_shared<State>();
        std::promise<void> release_promise;
        state->release = release_promise.get_future().share();
        const auto first_handler_started = state->first_handler_started.get_future();

        RuntimeDependencies dependencies{8};
        auto config = with_before_command(
            [state](const snf::server::PlayerCommand&)
            {
                if (state->handled.fetch_add(1) == 0)
                {
                    state->first_handler_started.set_value();
                    state->release.wait();
                }
            });
        config.queue_capacity_per_worker = 2;
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Accepted);
        assert(first_handler_started.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(0, 2)) == snf::server::PostResult::Accepted);
        runtime.close();
        runtime.cancel();
        runtime.cancel();
        assert(runtime.tryPost(make_command(0, 3)) == snf::server::PostResult::Closed);
        release_promise.set_value();
        runtime.join();

        assert(state->handled.load() == 1);
        assert(dependencies.completion.drained_count.load() == 0);
        assert(dependencies.completion.failed_count.load() == 0);
    }

    void test_cancel_discards_commands_already_routed_to_a_mailbox()
    {
        struct State
        {
            std::atomic<int> handled{0};
            std::promise<void> worker_start_entered;
            std::shared_future<void> release_worker_start;
            std::promise<void> first_handler_started;
            std::shared_future<void> release_first_handler;
        };
        const auto state = std::make_shared<State>();
        std::promise<void> release_worker_start_promise;
        state->release_worker_start = release_worker_start_promise.get_future().share();
        std::promise<void> release_first_handler_promise;
        state->release_first_handler = release_first_handler_promise.get_future().share();
        const auto worker_start_entered = state->worker_start_entered.get_future();
        const auto first_handler_started = state->first_handler_started.get_future();

        RuntimeDependencies dependencies{8};
        auto config = snf::server::ActorRuntimeConfig{snf::server::RuntimeId::Player};
        config.worker_count = 1;
        config.queue_capacity_per_worker = 2;
        config.on_worker_start = [state](std::size_t)
        {
            state->worker_start_entered.set_value();
            state->release_worker_start.wait();
        };
        config.on_before_command =
            [state](std::size_t, snf::server::ProvisionalActorId, const snf::server::PlayerCommand&)
        {
            if (state->handled.fetch_add(1) == 0)
            {
                state->first_handler_started.set_value();
                state->release_first_handler.wait();
            }
        };
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Accepted);
        assert(worker_start_entered.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(0, 2)) == snf::server::PostResult::Accepted);

        release_worker_start_promise.set_value();
        assert(first_handler_started.wait_for(1s) == std::future_status::ready);
        runtime.cancel();
        release_first_handler_promise.set_value();
        runtime.join();

        assert(state->handled.load() == 1);
        const auto stats = runtime.getStats().workers.front();
        assert(stats.processed == 1);
        assert(stats.mailbox_depth == 0);
        assert(stats.queue_depth == 0);
        assert(dependencies.completion.drained_count.load() == 0);
        assert(dependencies.completion.failed_count.load() == 0);
    }

    void test_cancel_interrupts_a_worker_blocked_on_full_outbound_sink()
    {
        std::promise<void> second_handler_started;
        const auto second_handler = second_handler_started.get_future();

        RuntimeDependencies dependencies{1};
        auto config = with_before_command(
            [&second_handler_started](const snf::server::PlayerCommand& command)
            {
                if (snf::server::requestId(command) == 2)
                {
                    second_handler_started.set_value();
                }
            });
        config.worker_count = 1;
        config.queue_capacity_per_worker = 2;
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Accepted);

        const auto outbound_deadline = std::chrono::steady_clock::now() + 1s;
        while (dependencies.outbound.size() != 1 &&
               std::chrono::steady_clock::now() < outbound_deadline)
        {
            std::this_thread::yield();
        }
        assert(dependencies.outbound.size() == 1);

        assert(runtime.tryPost(make_command(0, 2)) == snf::server::PostResult::Accepted);
        assert(second_handler.wait_for(1s) == std::future_status::ready);

        runtime.cancel();
        auto join_result = std::async(std::launch::async, [&runtime] { runtime.join(); });
        const bool joined_after_cancel = join_result.wait_for(1s) == std::future_status::ready;
        if (!joined_after_cancel)
        {
            // Keep a failing regression test from leaving its helper thread blocked.
            dependencies.outbound.cancel();
        }
        join_result.get();

        assert(joined_after_cancel);
        assert(dependencies.outbound.size() == 1);
        assert(dependencies.completion.drained_count.load() == 0);
        assert(dependencies.completion.failed_count.load() == 0);
    }

    void test_aggregates_queue_wait_and_high_water_mark()
    {
        struct State
        {
            std::promise<void> first_handler_started;
            std::shared_future<void> release;
        };
        const auto state = std::make_shared<State>();
        std::promise<void> release_promise;
        state->release = release_promise.get_future().share();
        const auto first_handler_started = state->first_handler_started.get_future();

        RuntimeDependencies dependencies{8};
        const auto first = std::make_shared<std::atomic<bool>>(true);
        auto config = with_before_command(
            [state, first](const snf::server::PlayerCommand&)
            {
                if (first->exchange(false))
                {
                    state->first_handler_started.set_value();
                    state->release.wait();
                }
            });
        config.queue_capacity_per_worker = 2;
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Accepted);
        assert(first_handler_started.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(0, 2)) == snf::server::PostResult::Accepted);
        std::this_thread::sleep_for(20ms);
        release_promise.set_value();
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats().workers[0];
        assert(stats.accepted == 2);
        assert(stats.processed == 2);
        assert(stats.queue_high_water_mark == 2);
        assert(stats.average_queue_wait > 5ms);
        assert(stats.max_queue_wait >= stats.average_queue_wait);
    }

    void test_yields_once_without_duplicate_ready_tokens_and_runs_another_actor()
    {
        struct State
        {
            std::atomic<bool> first_actor_a_command{true};
            std::promise<void> actor_a_started;
            std::shared_future<void> release_actor_a;
            std::mutex mutex;
            std::vector<std::uint32_t> requests;
        };
        const auto state = std::make_shared<State>();
        std::promise<void> release_actor_a_promise;
        state->release_actor_a = release_actor_a_promise.get_future().share();
        const auto actor_a_started = state->actor_a_started.get_future();

        RuntimeDependencies dependencies{64};
        auto config = with_before_command(
            [state](const snf::server::PlayerCommand& request)
            {
                if (snf::server::requestId(request) == 1 &&
                    state->first_actor_a_command.exchange(false))
                {
                    state->actor_a_started.set_value();
                    state->release_actor_a.wait();
                }

                std::lock_guard lock{state->mutex};
                state->requests.push_back(snf::server::requestId(request));
            });
        config.worker_count = 1;
        config.queue_capacity_per_worker = 64;
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Accepted);
        assert(actor_a_started.wait_for(1s) == std::future_status::ready);
        for (std::uint32_t request_id = 2; request_id <= 32; ++request_id)
        {
            assert(runtime.tryPost(make_command(0, request_id)) ==
                   snf::server::PostResult::Accepted);
        }
        assert(runtime.tryPost(make_command(2, 1000)) == snf::server::PostResult::Accepted);

        release_actor_a_promise.set_value();
        runtime.close();
        runtime.join();

        const auto actor_b = std::find(state->requests.begin(), state->requests.end(), 1000);
        const auto final_actor_a =
            std::find(state->requests.begin(), state->requests.end(), std::uint32_t{32});
        assert(state->requests.size() == 33);
        assert(actor_b != state->requests.end());
        assert(final_actor_a != state->requests.end());
        assert(actor_b < final_actor_a);

        const auto stats = runtime.getStats().workers.front();
        assert(stats.actor_count == 2);
        assert(stats.ready_actor_count == 0);
        assert(stats.mailbox_depth == 0);
        assert(stats.queue_depth == 0);
        assert(stats.budget_yield_turns == 1);
    }

    void test_worker_exception_cancels_runtime_and_is_rethrown_after_join()
    {
        std::promise<void> failure_notified;
        const auto failure_notification = failure_notified.get_future();
        RuntimeDependencies dependencies{8};
        auto config = with_before_command([](const snf::server::PlayerCommand&)
                                          { throw std::runtime_error{"test worker failure"}; });
        config.on_worker_failure = [&failure_notified] { failure_notified.set_value(); };
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Accepted);
        assert(failure_notification.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(1, 2)) == snf::server::PostResult::Closed);

        bool rethrown = false;
        try
        {
            runtime.join();
        }
        catch (const std::runtime_error& error)
        {
            rethrown = std::string_view{error.what()} == "test worker failure";
        }
        assert(rethrown);

        assert(dependencies.completion.drained_count.load() == 0);
        assert(dependencies.completion.failed_count.load() == 1);
        assert(!dependencies.outbound.tryPop().has_value());
    }

    void test_connection_closed_evicts_after_prior_commands_without_polluting_command_stats()
    {
        RuntimeDependencies dependencies{8};
        auto config = snf::server::ActorRuntimeConfig{snf::server::RuntimeId::Player};
        config.worker_count = 1;
        config.queue_capacity_per_worker = 8;
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPost(make_command(3, 1)) == snf::server::PostResult::Accepted);
        assert(runtime.tryPost(make_command(3, 2)) == snf::server::PostResult::Accepted);
        assert(runtime.tryPost(make_command(3, 3)) == snf::server::PostResult::Accepted);
        assert(runtime.tryPostConnectionClosed(snf::server::ProvisionalActorId{.value = 3},
                                               make_closed(3)) ==
               snf::server::PostResult::Accepted);
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats().workers.front();
        assert(stats.accepted == 3);
        assert(stats.processed == 3);
        assert(stats.evicted_actors == 1);
        assert(stats.actor_count == 0);
        assert(stats.mailbox_depth == 0);
        assert(stats.queue_depth == 0);
        assert(stats.average_queue_wait.count() >= 0);
    }

    void test_unknown_connection_closed_does_not_create_an_actor()
    {
        RuntimeDependencies dependencies{8};
        auto config = snf::server::ActorRuntimeConfig{snf::server::RuntimeId::Player};
        config.worker_count = 1;
        config.queue_capacity_per_worker = 2;
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(runtime.tryPostConnectionClosed(snf::server::ProvisionalActorId{.value = 3},
                                               make_closed(3)) ==
               snf::server::PostResult::Accepted);
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats().workers.front();
        assert(stats.accepted == 0);
        assert(stats.processed == 0);
        assert(stats.evicted_actors == 0);
        assert(stats.actor_count == 0);
        assert(stats.queue_depth == 0);
    }

    void test_connection_closed_full_and_closed_results_do_not_change_rejected_commands()
    {
        struct State
        {
            std::promise<void> worker_started;
            std::shared_future<void> release;
        };
        const auto state = std::make_shared<State>();
        std::promise<void> release;
        state->release = release.get_future().share();
        const auto worker_started = state->worker_started.get_future();

        RuntimeDependencies dependencies{8};
        auto config = snf::server::ActorRuntimeConfig{snf::server::RuntimeId::Player};
        config.worker_count = 1;
        config.queue_capacity_per_worker = 1;
        config.on_worker_start = [state](std::size_t)
        {
            state->worker_started.set_value();
            state->release.wait();
        };
        snf::server::ActorRuntime runtime{
            config, dependencies.outbound_sink, dependencies.completion};

        runtime.start();
        assert(worker_started.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(3, 1)) == snf::server::PostResult::Accepted);
        assert(runtime.tryPostConnectionClosed(snf::server::ProvisionalActorId{.value = 3},
                                               make_closed(3)) == snf::server::PostResult::Full);
        assert(runtime.getStats().workers.front().rejected_full == 0);

        release.set_value();
        runtime.close();
        runtime.join();
        assert(runtime.tryPostConnectionClosed(snf::server::ProvisionalActorId{.value = 3},
                                               make_closed(3)) == snf::server::PostResult::Closed);
    }
}

void run_actor_runtime_tests()
{
    test_rejects_invalid_configuration_and_posts_only_after_start();
    test_turns_ping_into_identical_pong_and_preserves_actor_order();
    test_serializes_same_actor_execution();
    test_runs_different_shards_in_parallel();
    test_full_shard_does_not_block_another_shard();
    test_drains_all_workers_before_notifying_runtime_completion_once();
    test_cancel_discards_queued_commands();
    test_cancel_discards_commands_already_routed_to_a_mailbox();
    test_cancel_interrupts_a_worker_blocked_on_full_outbound_sink();
    test_aggregates_queue_wait_and_high_water_mark();
    test_yields_once_without_duplicate_ready_tokens_and_runs_another_actor();
    test_worker_exception_cancels_runtime_and_is_rethrown_after_join();
    test_connection_closed_evicts_after_prior_commands_without_polluting_command_stats();
    test_unknown_connection_closed_does_not_create_an_actor();
    test_connection_closed_full_and_closed_results_do_not_change_rejected_commands();
}
