#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/bounded_queue.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/outbound_action.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_actor_ingress.hpp"
#include "snf/server/protocol_player_effect_sink.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <latch>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <stop_token>
#include <string_view>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    using namespace std::chrono_literals;
    using snf::runtime::ActorAccounting;
    using snf::runtime::ActorActivation;
    using snf::runtime::ActorBinding;
    using snf::runtime::ActorDispatchResult;
    using snf::runtime::ActorKey;
    using snf::runtime::ActorKind;
    using snf::runtime::ActorRuntime;
    using snf::runtime::ActorRuntimeConfig;
    using snf::runtime::ActorSlot;
    using snf::runtime::ActorSubmission;
    using snf::runtime::EntityId;
    using snf::runtime::PostResult;

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

    class RecordingRuntimeCompletion final : public snf::runtime::RuntimeCompletionSink
    {
    public:
        void notifyDrained(const snf::runtime::RuntimeId runtime) noexcept override
        {
            assert(runtime == snf::runtime::RuntimeId::Logic);
            drained_count.fetch_add(1);
        }

        void notifyFailed(const snf::runtime::RuntimeId runtime) noexcept override
        {
            assert(runtime == snf::runtime::RuntimeId::Logic);
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

    class PlayerRuntime final
    {
    public:
        PlayerRuntime(RuntimeDependencies& dependencies,
                      ActorRuntimeConfig runtime_config,
                      snf::server::PlayerActorBindingConfig binding_config = {})
            : binding(dependencies.outbound_sink, std::move(binding_config))
            , runtime(runtime_config, dependencies.completion)
            , ingress(runtime, binding)
        {
            runtime.registerBinding(binding);
        }

        snf::server::PlayerActorBinding binding;
        ActorRuntime runtime;
        snf::server::PlayerActorIngress ingress;
    };

    snf::server::PlayerInboundCommand make_command(const std::uint64_t actor_id,
                                                   const std::uint32_t request_id)
    {
        return snf::server::PlayerInboundCommand{
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

    ActorRuntimeConfig player_runtime_config(const std::size_t workers = 2,
                                             const std::size_t capacity = 8)
    {
        ActorRuntimeConfig config;
        config.worker_count = workers;
        config.queue_capacity_per_worker = capacity;
        return config;
    }

    class SyntheticBinding final : public ActorBinding
    {
    public:
        struct State
        {
            std::mutex mutex;
            std::vector<std::pair<ActorKey, int>> dispatched;
            std::function<void(const ActorKey&, int)> on_dispatch;
            bool throw_on_dispatch{false};
        };

        SyntheticBinding(const ActorKind kind, std::shared_ptr<State> state)
            : _kind(kind)
            , _state(std::move(state))
        {
        }

        [[nodiscard]] ActorKind kind() const noexcept override
        {
            return _kind;
        }

        [[nodiscard]] ActorSubmission post(const EntityId entity, const int value) const
        {
            return makeSubmission(ActorKey{.kind = _kind, .entity = entity},
                                  ActorActivation::ActivateIfMissing,
                                  ActorAccounting::Command,
                                  Payload{.value = value});
        }

        [[nodiscard]] ActorSubmission evict(const EntityId entity) const
        {
            return makeSubmission(ActorKey{.kind = _kind, .entity = entity},
                                  ActorActivation::ExistingOnly,
                                  ActorAccounting::Control,
                                  Payload{.value = 0});
        }

    protected:
        [[nodiscard]] std::unique_ptr<ActorSlot> activate(const EntityId) override
        {
            return std::make_unique<Slot>();
        }

        [[nodiscard]] ActorDispatchResult
        dispatch(ActorSlot& slot, const ActorSubmission& submission, std::stop_token) override
        {
            static_cast<void>(dynamic_cast<Slot&>(slot));
            const Payload& payload = payloadAs<Payload>(submission);
            if (submission.accounting() == ActorAccounting::Control)
            {
                return ActorDispatchResult::Evict;
            }

            std::function<void(const ActorKey&, int)> on_dispatch;
            {
                std::lock_guard lock{_state->mutex};
                _state->dispatched.emplace_back(submission.target(), payload.value);
                if (_state->throw_on_dispatch)
                {
                    throw std::runtime_error{"synthetic binding failure"};
                }
                on_dispatch = _state->on_dispatch;
            }
            if (on_dispatch)
            {
                on_dispatch(submission.target(), payload.value);
            }
            return ActorDispatchResult::KeepActive;
        }

    private:
        struct Slot final : ActorSlot
        {
        };

        struct Payload
        {
            int value;
        };

        ActorKind _kind;
        std::shared_ptr<State> _state;
    };

    void test_actor_key_affinity_and_registry_rules()
    {
        const ActorKey provisional{.kind = ActorKind::ProvisionalPlayer, .entity = 17};
        const ActorKey same_provisional{.kind = ActorKind::ProvisionalPlayer, .entity = 17};
        const ActorKey zone{.kind = ActorKind::Zone, .entity = 17};
        assert(provisional == same_provisional);
        assert(provisional != zone);
        assert(snf::runtime::ActorKeyHash{}(provisional) ==
               snf::runtime::ActorKeyHash{}(same_provisional));

        RecordingRuntimeCompletion completion;
        auto config = player_runtime_config(3, 4);
        ActorRuntime runtime{config, completion};
        const std::size_t expected = snf::runtime::ActorKeyHash{}(provisional) % 3;
        assert(runtime.workerIndexFor(provisional) == expected);

        const auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding players{ActorKind::ProvisionalPlayer, state};
        SyntheticBinding duplicate_players{ActorKind::ProvisionalPlayer, state};
        runtime.registerBinding(players);

        bool duplicate_rejected = false;
        try
        {
            runtime.registerBinding(duplicate_players);
        }
        catch (const std::invalid_argument&)
        {
            duplicate_rejected = true;
        }
        assert(duplicate_rejected);

        runtime.start();
        bool late_registration_rejected = false;
        try
        {
            runtime.registerBinding(duplicate_players);
        }
        catch (const std::logic_error&)
        {
            late_registration_rejected = true;
        }
        assert(late_registration_rejected);
        bool unregistered_submission_rejected = false;
        try
        {
            static_cast<void>(runtime.tryPost(duplicate_players.post(17, 1)));
        }
        catch (const std::invalid_argument&)
        {
            unregistered_submission_rejected = true;
        }
        assert(unregistered_submission_rejected);
        runtime.close();
        runtime.join();
    }

    void test_player_binding_preserves_ping_pong_fifo_and_effect_order()
    {
        RuntimeDependencies dependencies{8};
        PlayerRuntime player{dependencies, player_runtime_config()};
        player.runtime.start();
        assert(player.ingress.tryPost(make_command(7, 100)) == PostResult::Accepted);
        assert(player.ingress.tryPost(make_command(7, 101)) == PostResult::Accepted);
        assert(player.ingress.tryPost(make_command(7, 102)) == PostResult::Accepted);
        player.runtime.close();
        player.runtime.join();

        for (std::uint32_t request_id = 100; request_id <= 102; ++request_id)
        {
            const auto action = dependencies.outbound.tryPop();
            assert(action.has_value());
            const auto* send = std::get_if<snf::server::SendFrame>(&*action);
            assert(send != nullptr);
            assert(send->connection.generation == 7);
            assert(send->frame.type == snf::protocol::MessageType::Pong);
            assert(send->frame.request_id == request_id);
        }
        assert(dependencies.completion.drained_count.load() == 1);
        assert(dependencies.completion.failed_count.load() == 0);
    }

    void test_same_actor_is_serial_and_different_shards_run_in_parallel()
    {
        struct State
        {
            std::atomic<int> active{0};
            std::atomic<int> maximum_active{0};
            std::atomic<int> entered{0};
            std::promise<void> both_entered;
            std::latch release{1};
        };
        const auto state = std::make_shared<State>();
        const auto both_entered = state->both_entered.get_future();

        RuntimeDependencies dependencies{8};
        auto binding_config = snf::server::PlayerActorBindingConfig{
            .on_before_command =
                [state](snf::server::ProvisionalActorId, const snf::server::PlayerCommand&)
            {
                const int active = state->active.fetch_add(1) + 1;
                int maximum = state->maximum_active.load();
                while (maximum < active &&
                       !state->maximum_active.compare_exchange_weak(maximum, active))
                {
                }
                if (state->entered.fetch_add(1) + 1 == 2)
                {
                    state->both_entered.set_value();
                }
                state->release.wait();
                state->active.fetch_sub(1);
            },
        };
        PlayerRuntime player{dependencies, player_runtime_config(), std::move(binding_config)};

        ActorKey first{.kind = ActorKind::ProvisionalPlayer, .entity = 0};
        ActorKey second{.kind = ActorKind::ProvisionalPlayer, .entity = 1};
        while (player.runtime.workerIndexFor(first) == player.runtime.workerIndexFor(second))
        {
            ++second.entity;
        }

        player.runtime.start();
        assert(player.ingress.tryPost(make_command(first.entity, 1)) == PostResult::Accepted);
        assert(player.ingress.tryPost(make_command(first.entity, 2)) == PostResult::Accepted);
        assert(player.ingress.tryPost(make_command(second.entity, 3)) == PostResult::Accepted);
        assert(both_entered.wait_for(1s) == std::future_status::ready);
        assert(state->maximum_active.load() == 2);

        state->release.count_down();
        player.runtime.close();
        player.runtime.join();
        // The same key's second command cannot overlap its first, so the global
        // maximum is exactly the two independently sharded actors.
        assert(state->maximum_active.load() == 2);
    }

    void test_synthetic_bindings_share_capacity_fairness_and_cross_kind_slots()
    {
        // Player and Zone submissions share the single Worker's outstanding
        // budget; one kind cannot reserve a private queue.
        RecordingRuntimeCompletion capacity_completion;
        auto capacity_config = player_runtime_config(1, 2);
        std::promise<void> capacity_worker_start_signal;
        std::promise<void> release_capacity_worker;
        const auto capacity_worker_started = capacity_worker_start_signal.get_future();
        const auto release_capacity = release_capacity_worker.get_future().share();
        capacity_config.on_worker_start =
            [&capacity_worker_start_signal, release_capacity](std::size_t)
        {
            capacity_worker_start_signal.set_value();
            release_capacity.wait();
        };
        ActorRuntime capacity_runtime{capacity_config, capacity_completion};
        const auto capacity_state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding capacity_players{ActorKind::ProvisionalPlayer, capacity_state};
        SyntheticBinding capacity_zones{ActorKind::Zone, capacity_state};
        capacity_runtime.registerBinding(capacity_players);
        capacity_runtime.registerBinding(capacity_zones);
        capacity_runtime.start();
        assert(capacity_worker_started.wait_for(1s) == std::future_status::ready);
        assert(capacity_runtime.tryPost(capacity_players.post(1, 1)) == PostResult::Accepted);
        assert(capacity_runtime.tryPost(capacity_zones.post(1, 2)) == PostResult::Accepted);
        assert(capacity_runtime.tryPost(capacity_players.post(2, 3)) == PostResult::Full);
        const auto capacity_stats = capacity_runtime.getStats().workers.front();
        assert(capacity_stats.accepted == 2);
        assert(capacity_stats.rejected_full == 1);
        release_capacity_worker.set_value();
        capacity_runtime.close();
        capacity_runtime.join();

        RecordingRuntimeCompletion completion;
        auto config = player_runtime_config(1, 64);
        ActorRuntime runtime{config, completion};
        const auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding players{ActorKind::ProvisionalPlayer, state};
        SyntheticBinding zones{ActorKind::Zone, state};
        runtime.registerBinding(players);
        runtime.registerBinding(zones);

        struct Gate
        {
            std::promise<void> entered;
            std::shared_future<void> release;
        };
        auto gate = std::make_shared<Gate>();
        std::promise<void> release;
        gate->release = release.get_future().share();
        const auto entered = gate->entered.get_future();
        state->on_dispatch = [gate](const ActorKey&, const int value)
        {
            if (value == 1)
            {
                gate->entered.set_value();
                gate->release.wait();
            }
        };

        runtime.start();
        assert(runtime.tryPost(players.post(9, 1)) == PostResult::Accepted);
        assert(entered.wait_for(1s) == std::future_status::ready);
        for (int value = 2; value <= 32; ++value)
        {
            assert(runtime.tryPost(players.post(9, value)) == PostResult::Accepted);
        }
        assert(runtime.tryPost(zones.post(9, 1000)) == PostResult::Accepted);

        release.set_value();
        runtime.close();
        runtime.join();

        const auto zone_dispatch =
            std::find_if(state->dispatched.begin(),
                         state->dispatched.end(),
                         [](const auto& event) { return event.second == 1000; });
        const auto final_player_dispatch =
            std::find_if(state->dispatched.begin(),
                         state->dispatched.end(),
                         [](const auto& event) { return event.second == 32; });
        assert(zone_dispatch != state->dispatched.end());
        assert(final_player_dispatch != state->dispatched.end());
        assert(zone_dispatch < final_player_dispatch);
        assert(runtime.getStats().workers.front().budget_yield_turns == 1);

        // Same numeric id remains independent by kind: evicting the Player slot
        // must never evict a Zone slot.
        RecordingRuntimeCompletion completion_two;
        ActorRuntime cross_kind{player_runtime_config(1, 8), completion_two};
        const auto cross_players = std::make_shared<SyntheticBinding::State>();
        const auto cross_zones = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding player_binding{ActorKind::ProvisionalPlayer, cross_players};
        SyntheticBinding zone_binding{ActorKind::Zone, cross_zones};
        cross_kind.registerBinding(player_binding);
        cross_kind.registerBinding(zone_binding);
        cross_kind.start();
        assert(cross_kind.tryPost(player_binding.post(5, 1)) == PostResult::Accepted);
        assert(cross_kind.tryPost(zone_binding.post(5, 2)) == PostResult::Accepted);
        assert(cross_kind.tryPost(player_binding.evict(5)) == PostResult::Accepted);
        assert(cross_kind.tryPost(zone_binding.post(5, 3)) == PostResult::Accepted);
        cross_kind.close();
        cross_kind.join();
        assert(cross_zones->dispatched.size() == 2);
        assert(completion_two.drained_count.load() == 1);
    }

    void test_capacity_and_lifecycle_control_accounting()
    {
        RuntimeDependencies dependencies{8};
        auto config = player_runtime_config(1, 1);
        std::promise<void> worker_started;
        std::shared_future<void> hold;
        std::promise<void> release;
        hold = release.get_future().share();
        config.on_worker_start = [&worker_started, hold](std::size_t)
        {
            worker_started.set_value();
            hold.wait();
        };
        PlayerRuntime player{dependencies, std::move(config)};
        player.runtime.start();
        assert(worker_started.get_future().wait_for(1s) == std::future_status::ready);
        assert(player.ingress.tryPost(make_command(3, 1)) == PostResult::Accepted);
        assert(player.ingress.tryPostConnectionClosed(snf::server::ProvisionalActorId{.value = 3},
                                                      make_closed(3)) == PostResult::Full);
        assert(player.runtime.getStats().workers.front().rejected_full == 0);
        release.set_value();
        player.runtime.close();
        player.runtime.join();

        RuntimeDependencies empty_dependencies{8};
        PlayerRuntime empty_player{empty_dependencies, player_runtime_config(1, 2)};
        empty_player.runtime.start();
        assert(empty_player.ingress.tryPostConnectionClosed(
                   snf::server::ProvisionalActorId{.value = 17}, make_closed(17)) ==
               PostResult::Accepted);
        empty_player.runtime.close();
        empty_player.runtime.join();
        const auto stats = empty_player.runtime.getStats().workers.front();
        assert(stats.accepted == 0);
        assert(stats.processed == 0);
        assert(stats.evicted_actors == 0);
        assert(stats.queue_depth == 0);
    }

    void test_cancel_and_failure_terminal_paths()
    {
        struct Gate
        {
            std::promise<void> first_handler_started;
            std::shared_future<void> release;
            std::atomic<int> handled{0};
        };
        const auto gate = std::make_shared<Gate>();
        std::promise<void> release;
        gate->release = release.get_future().share();
        const auto started = gate->first_handler_started.get_future();

        RuntimeDependencies dependencies{8};
        auto binding_config = snf::server::PlayerActorBindingConfig{
            .on_before_command =
                [gate](snf::server::ProvisionalActorId, const snf::server::PlayerCommand&)
            {
                if (gate->handled.fetch_add(1) == 0)
                {
                    gate->first_handler_started.set_value();
                    gate->release.wait();
                }
            },
        };
        PlayerRuntime player{dependencies, player_runtime_config(1, 2), std::move(binding_config)};
        player.runtime.start();
        assert(player.ingress.tryPost(make_command(1, 1)) == PostResult::Accepted);
        assert(started.wait_for(1s) == std::future_status::ready);
        assert(player.ingress.tryPost(make_command(1, 2)) == PostResult::Accepted);
        player.runtime.cancel();
        release.set_value();
        player.runtime.join();
        assert(gate->handled.load() == 1);
        assert(dependencies.completion.drained_count.load() == 0);

        RecordingRuntimeCompletion completion;
        auto state = std::make_shared<SyntheticBinding::State>();
        state->throw_on_dispatch = true;
        SyntheticBinding broken{ActorKind::Zone, state};
        ActorRuntime failing{player_runtime_config(1, 2), completion};
        failing.registerBinding(broken);
        failing.start();
        assert(failing.tryPost(broken.post(2, 1)) == PostResult::Accepted);
        bool rethrown = false;
        try
        {
            failing.join();
        }
        catch (const std::runtime_error& error)
        {
            rethrown = std::string_view{error.what()} == "synthetic binding failure";
        }
        assert(rethrown);
        assert(completion.failed_count.load() == 1);
        assert(completion.drained_count.load() == 0);
    }

    void test_cancel_interrupts_player_effect_backpressure_without_canceling_outbound()
    {
        RuntimeDependencies dependencies{1};
        PlayerRuntime player{dependencies, player_runtime_config(1, 2)};
        player.runtime.start();
        assert(player.ingress.tryPost(make_command(1, 1)) == PostResult::Accepted);

        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (dependencies.outbound.size() != 1 && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::yield();
        }
        assert(dependencies.outbound.size() == 1);
        assert(player.ingress.tryPost(make_command(1, 2)) == PostResult::Accepted);
        player.runtime.cancel();
        auto joined = std::async(std::launch::async, [&player] { player.runtime.join(); });
        const bool completed = joined.wait_for(1s) == std::future_status::ready;
        if (!completed)
        {
            dependencies.outbound.cancel();
        }
        joined.get();
        assert(completed);
        assert(dependencies.outbound.size() == 1);
    }
}

void run_actor_runtime_tests()
{
    test_actor_key_affinity_and_registry_rules();
    test_player_binding_preserves_ping_pong_fifo_and_effect_order();
    test_same_actor_is_serial_and_different_shards_run_in_parallel();
    test_synthetic_bindings_share_capacity_fairness_and_cross_kind_slots();
    test_capacity_and_lifecycle_control_accounting();
    test_cancel_and_failure_terminal_paths();
    test_cancel_interrupts_player_effect_backpressure_without_canceling_outbound();
}
