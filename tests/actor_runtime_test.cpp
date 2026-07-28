#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/actor_runtime.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <latch>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <sys/eventfd.h>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    using namespace std::chrono_literals;

    snf::net::UniqueFileDescriptor make_eventfd()
    {
        const int descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        assert(descriptor != -1);
        return snf::net::UniqueFileDescriptor{descriptor};
    }

    snf::server::InboundCommand make_command(const std::uint64_t actor_id,
                                              const std::uint32_t request_id,
                                              const snf::protocol::MessageType type =
                                                  snf::protocol::MessageType::Ping)
    {
        return snf::server::InboundCommand{
            .actor = snf::server::ActorId{.value = actor_id},
            .connection = snf::server::ConnectionId{
                .descriptor = static_cast<int>(42 + actor_id),
                .generation = actor_id,
            },
            .frame = snf::protocol::Frame{
                .type = type,
                .request_id = request_id,
                .payload = {},
            },
        };
    }

    std::vector<snf::server::NetworkAction>
    run_ping_commands(std::vector<snf::server::InboundCommand> commands)
    {
        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{16};
        const auto event = make_eventfd();
        snf::server::ActorRuntime runtime{
            snf::server::ActorRuntimeConfig{
                .worker_count = 2,
                .queue_capacity_per_worker = 8,
                .message_dispatcher_factory = {},
                .on_worker_failure = {},
            },
            outbound,
            event.getDescriptor()};

        runtime.start();
        for (auto& command : commands)
        {
            assert(runtime.tryPost(std::move(command)) == snf::server::PostResult::Accepted);
        }
        runtime.close();
        runtime.join();

        std::vector<snf::server::NetworkAction> actions;
        while (auto action = outbound.tryPop())
        {
            actions.push_back(std::move(*action));
        }
        return actions;
    }

    snf::server::ActorRuntimeConfig with_pong_handler(snf::server::MessageDispatcher::Handler handler)
    {
        return snf::server::ActorRuntimeConfig{
            .worker_count = 2,
            .queue_capacity_per_worker = 8,
            .message_dispatcher_factory = [handler = std::move(handler)](std::size_t)
            {
                snf::server::MessageDispatcher dispatcher;
                assert(dispatcher.registerHandler(snf::protocol::MessageType::Pong, handler));
                return dispatcher;
            },
            .on_worker_failure = {},
        };
    }

    void test_rejects_invalid_configuration_and_posts_only_after_start()
    {
        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{8};
        const auto event = make_eventfd();

        bool rejected_zero_workers = false;
        try
        {
            [[maybe_unused]] snf::server::ActorRuntime invalid{
                snf::server::ActorRuntimeConfig{
                    .worker_count = 0,
                    .queue_capacity_per_worker = 1,
                    .message_dispatcher_factory = {},
                    .on_worker_failure = {},
                },
                outbound,
                event.getDescriptor()};
        }
        catch (const std::invalid_argument&)
        {
            rejected_zero_workers = true;
        }
        assert(rejected_zero_workers);

        bool rejected_zero_capacity = false;
        try
        {
            [[maybe_unused]] snf::server::ActorRuntime invalid{
                snf::server::ActorRuntimeConfig{
                    .worker_count = 1,
                    .queue_capacity_per_worker = 0,
                    .message_dispatcher_factory = {},
                    .on_worker_failure = {},
                },
                outbound,
                event.getDescriptor()};
        }
        catch (const std::invalid_argument&)
        {
            rejected_zero_capacity = true;
        }
        assert(rejected_zero_capacity);

        snf::server::ActorRuntime runtime{
            snf::server::ActorRuntimeConfig{
                .worker_count = 2,
                .queue_capacity_per_worker = 2,
                .message_dispatcher_factory = {},
                .on_worker_failure = {},
            },
            outbound,
            event.getDescriptor()};
        assert(runtime.tryPost(make_command(0, 1)) == snf::server::PostResult::Closed);
        assert(runtime.workerCount() == 2);
        assert(runtime.workerIndexFor(snf::server::ActorId{.value = 0}) == 0);
        assert(runtime.workerIndexFor(snf::server::ActorId{.value = 1}) == 1);
        assert(runtime.workerIndexFor(snf::server::ActorId{.value = 11}) == 1);
        assert(runtime.workerIndexFor(
                   snf::server::ActorId{.value = std::numeric_limits<std::uint64_t>::max()}) ==
               1);

        runtime.start();
        assert(runtime.tryPost(make_command(0, 2)) == snf::server::PostResult::Accepted);
        runtime.close();
        runtime.close();
        assert(runtime.tryPost(make_command(0, 3)) == snf::server::PostResult::Closed);
        runtime.join();

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
        const auto actions = run_ping_commands({
            make_command(7, 100),
            make_command(7, 101),
            make_command(7, 102),
        });

        assert(actions.size() == 4);
        for (std::uint32_t index = 0; index < 3; ++index)
        {
            const auto* send = std::get_if<snf::server::SendFrame>(&actions[index]);
            assert(send != nullptr);
            assert(send->connection.generation == 7);
            assert(send->frame.type == snf::protocol::MessageType::Pong);
            assert(send->frame.request_id == 100 + index);
        }
        assert(std::holds_alternative<snf::server::GameRuntimeDrained>(actions.back()));
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

        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{8};
        const auto event = make_eventfd();
        auto config = with_pong_handler(
            [state](const snf::protocol::Frame& request)
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
                    state->requests.push_back(request.request_id);
                }
                state->active.fetch_sub(1);
                return std::vector<snf::protocol::Frame>{};
            });
        snf::server::ActorRuntime runtime{config, outbound, event.getDescriptor()};

        runtime.start();
        assert(runtime.tryPost(make_command(4, 1, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        assert(runtime.tryPost(make_command(4, 2, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        assert(runtime.tryPost(make_command(4, 3, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
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

        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{8};
        const auto event = make_eventfd();
        auto config = with_pong_handler(
            [state](const snf::protocol::Frame&)
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
                return std::vector<snf::protocol::Frame>{};
            });
        snf::server::ActorRuntime runtime{config, outbound, event.getDescriptor()};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        assert(runtime.tryPost(make_command(1, 2, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
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

        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{8};
        const auto event = make_eventfd();
        auto config = with_pong_handler(
            [state](const snf::protocol::Frame&)
            {
                if (!state->started.exchange(true))
                {
                    state->first_handler_started.set_value();
                }
                state->release.wait();
                return std::vector<snf::protocol::Frame>{};
            });
        config.queue_capacity_per_worker = 1;
        snf::server::ActorRuntime runtime{config, outbound, event.getDescriptor()};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        assert(first_handler_started.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(0, 2, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        assert(runtime.tryPost(make_command(0, 3, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Full);
        assert(runtime.tryPost(make_command(1, 4, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);

        release_promise.set_value();
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats();
        assert(stats.workers[0].accepted == 2);
        assert(stats.workers[0].rejected_full == 1);
        assert(stats.workers[1].accepted == 1);
    }

    void test_drains_all_workers_before_publishing_one_runtime_drained()
    {
        const auto actions = run_ping_commands({
            make_command(0, 1),
            make_command(1, 2),
        });

        assert(actions.size() == 3);
        const auto drained_count = static_cast<std::size_t>(std::count_if(
            actions.begin(),
            actions.end(),
            [](const snf::server::NetworkAction& action)
            { return std::holds_alternative<snf::server::GameRuntimeDrained>(action); }));
        assert(drained_count == 1);
        assert(std::holds_alternative<snf::server::GameRuntimeDrained>(actions.back()));
        assert(std::holds_alternative<snf::server::SendFrame>(actions[0]));
        assert(std::holds_alternative<snf::server::SendFrame>(actions[1]));
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

        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{8};
        const auto event = make_eventfd();
        auto config = with_pong_handler(
            [state](const snf::protocol::Frame&)
            {
                if (state->handled.fetch_add(1) == 0)
                {
                    state->first_handler_started.set_value();
                    state->release.wait();
                }
                return std::vector<snf::protocol::Frame>{};
            });
        config.queue_capacity_per_worker = 2;
        snf::server::ActorRuntime runtime{config, outbound, event.getDescriptor()};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        assert(first_handler_started.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(0, 2, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        runtime.cancel();
        runtime.cancel();
        assert(runtime.tryPost(make_command(0, 3, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Closed);
        release_promise.set_value();
        runtime.join();

        assert(state->handled.load() == 1);
        while (outbound.tryPop())
        {
            assert(false && "cancel must not publish GameRuntimeDrained");
        }
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

        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{8};
        const auto event = make_eventfd();
        const auto first = std::make_shared<std::atomic<bool>>(true);
        auto config = with_pong_handler(
            [state, first](const snf::protocol::Frame&)
            {
                if (first->exchange(false))
                {
                    state->first_handler_started.set_value();
                    state->release.wait();
                }
                return std::vector<snf::protocol::Frame>{};
            });
        config.queue_capacity_per_worker = 2;
        snf::server::ActorRuntime runtime{config, outbound, event.getDescriptor()};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        assert(first_handler_started.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(0, 2, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        std::this_thread::sleep_for(20ms);
        release_promise.set_value();
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats().workers[0];
        assert(stats.accepted == 2);
        assert(stats.processed == 2);
        assert(stats.queue_high_water_mark == 1);
        assert(stats.average_queue_wait > 5ms);
        assert(stats.max_queue_wait >= stats.average_queue_wait);
    }

    void test_worker_exception_cancels_runtime_and_is_rethrown_after_join()
    {
        std::promise<void> failure_notified;
        const auto failure_notification = failure_notified.get_future();
        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{8};
        const auto event = make_eventfd();
        auto config = with_pong_handler(
            [](const snf::protocol::Frame&) -> std::vector<snf::protocol::Frame>
            { throw std::runtime_error{"test worker failure"}; });
        config.on_worker_failure = [&failure_notified] { failure_notified.set_value(); };
        snf::server::ActorRuntime runtime{config, outbound, event.getDescriptor()};

        runtime.start();
        assert(runtime.tryPost(make_command(0, 1, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Accepted);
        assert(failure_notification.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(make_command(1, 2, snf::protocol::MessageType::Pong)) ==
               snf::server::PostResult::Closed);

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
    }
}

void run_actor_runtime_tests()
{
    test_rejects_invalid_configuration_and_posts_only_after_start();
    test_turns_ping_into_identical_pong_and_preserves_actor_order();
    test_serializes_same_actor_execution();
    test_runs_different_shards_in_parallel();
    test_full_shard_does_not_block_another_shard();
    test_drains_all_workers_before_publishing_one_runtime_drained();
    test_cancel_discards_queued_commands();
    test_aggregates_queue_wait_and_high_water_mark();
    test_worker_exception_cancels_runtime_and_is_rethrown_after_join();
}
