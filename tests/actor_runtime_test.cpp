#include "outbound_reservation_test_support.hpp"
#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/bounded_queue.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/outbound_action.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_actor_ingress.hpp"
#include "snf/server/protocol_player_response_sink.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
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
    using snf::runtime::ActorContext;
    using snf::runtime::ActorDispatchResult;
    using snf::runtime::ActorKey;
    using snf::runtime::ActorKind;
    using snf::runtime::ActorRuntime;
    using snf::runtime::ActorRuntimeConfig;
    using snf::runtime::ActorSlot;
    using snf::runtime::ActorSubmission;
    using snf::runtime::EntityId;
    using snf::runtime::PostResult;
    using snf::runtime::TellPayload;

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

    // The real channel, because the reservation path is part of what these tests
    // exercise. Without a reactor the test itself has to drain and grant, which is
    // exactly what a saturated outbound needs from the reactor in production.
    struct RuntimeDependencies
    {
        explicit RuntimeDependencies(const std::size_t outbound_capacity)
            : outbound_event(snf::test::make_wake_descriptor())
            , outbound(
                  snf::server::OutboundChannelConfig{
                      .capacity = outbound_capacity,
                      .max_slots_per_connection = outbound_capacity,
                  },
                  outbound_event.getDescriptor()
              )
            , outbound_sink(outbound)
        {
        }

        snf::net::UniqueFileDescriptor outbound_event;
        snf::server::OutboundChannel outbound;
        snf::server::ProtocolPlayerResponseSink outbound_sink;
        snf::server::CountingCommandLifecycleSink lifecycle;
        RecordingRuntimeCompletion completion;
    };

    class PlayerRuntime final
    {
    public:
        PlayerRuntime(
            RuntimeDependencies& dependencies, const ActorRuntimeConfig& runtime_config, snf::server::PlayerActorBindingConfig binding_config = {}
        )
            : binding(dependencies.outbound_sink, dependencies.outbound, dependencies.lifecycle, std::move(binding_config))
            , runtime(runtime_config, dependencies.completion)
            , ingress(runtime, binding, dependencies.lifecycle)
        {
            runtime.registerBinding(binding);
        }

        snf::server::PlayerActorBinding binding;
        ActorRuntime runtime;
        snf::server::PlayerActorIngress ingress;
    };

    class DelayedLoadPlayerRepository final : public snf::server::PlayerRepository
    {
    public:
        DelayedLoadPlayerRepository()
            : load_requested_future(load_requested.get_future())
        {
        }

        void asyncLoad(snf::server::PlayerId player, snf::server::PlayerLoadCompletion completion) override
        {
            std::lock_guard lock{mutex};
            requested_player = player;
            pending_load = std::move(completion);
            load_requested.set_value();
        }

        void asyncSave(snf::server::PlayerRecord record, snf::server::PlayerSaveCompletion completion) override
        {
            saved = record;
            completion(snf::server::PlayerSaveResult{
                .status = snf::server::PlayerRepositoryStatus::Success,
            });
        }

        void completeLoad(snf::server::PlayerRecord record)
        {
            snf::server::PlayerLoadCompletion completion;
            {
                std::lock_guard lock{mutex};
                completion = std::move(pending_load);
            }
            assert(completion);
            completion(snf::server::PlayerLoadResult{
                .status = snf::server::PlayerRepositoryStatus::Success,
                .record = std::move(record),
            });
        }

        std::promise<void> load_requested;
        std::future<void> load_requested_future;
        std::mutex mutex;
        snf::server::PlayerLoadCompletion pending_load;
        std::optional<snf::server::PlayerId> requested_player;
        std::optional<snf::server::PlayerRecord> saved;
    };

    snf::server::PlayerInboundCommand make_command(const std::uint64_t actor_id, const std::uint32_t request_id)
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
                    .payload = {},
                },
            .request_id = request_id,
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
            .has_location_snapshot = false,
            .last_location = std::nullopt,
        };
    }

    ActorRuntimeConfig player_runtime_config(const std::size_t workers = 2, const std::size_t capacity = 8)
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
            std::function<void(EntityId)> on_activate;
            std::function<void(const ActorKey&, int)> on_dispatch;
            std::function<void(ActorContext&, const ActorKey&, int)> on_dispatch_with_context;
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
            return makeSubmission(
                ActorKey{.kind = _kind, .entity = entity},
                ActorActivation::ActivateIfMissing,
                ActorAccounting::Command,
                Payload{.value = value, .evict = false}
            );
        }

        [[nodiscard]] ActorSubmission control(const EntityId entity, const int value) const
        {
            return makeSubmission(
                ActorKey{.kind = _kind, .entity = entity},
                ActorActivation::ActivateIfMissing,
                ActorAccounting::Control,
                Payload{.value = value, .evict = false}
            );
        }

        [[nodiscard]] ActorSubmission evict(const EntityId entity) const
        {
            return makeSubmission(
                ActorKey{.kind = _kind, .entity = entity}, ActorActivation::ExistingOnly, ActorAccounting::Control, Payload{.value = 0, .evict = true}
            );
        }

    protected:
        // Restores the int a sender handed to tryTell and wraps it in this
        // binding's own payload, which is what the runtime cannot do itself.
        [[nodiscard]] std::optional<ActorSubmission> makeTell(const ActorKey target, TellPayload payload) override
        {
            auto value = payload.take<int>();
            if (!value)
            {
                return std::nullopt;
            }

            return makeSubmission(target, ActorActivation::ActivateIfMissing, ActorAccounting::Command, Payload{.value = *value, .evict = false});
        }

        [[nodiscard]] std::unique_ptr<ActorSlot> activate(const EntityId entity) override
        {
            std::function<void(EntityId)> on_activate;
            {
                std::lock_guard lock{_state->mutex};
                on_activate = _state->on_activate;
            }
            if (on_activate)
            {
                on_activate(entity);
            }
            return std::make_unique<Slot>();
        }

        [[nodiscard]] ActorDispatchResult
        dispatch(ActorSlot& slot, const ActorSubmission& submission, ActorContext& context, std::stop_token) override
        {
            static_cast<void>(dynamic_cast<Slot&>(slot));
            const Payload& payload = payloadAs<Payload>(submission);
            if (submission.accounting() == ActorAccounting::Control && payload.evict)
            {
                return ActorDispatchResult::Evict;
            }

            std::function<void(const ActorKey&, int)> on_dispatch;
            std::function<void(ActorContext&, const ActorKey&, int)> on_dispatch_with_context;
            {
                std::lock_guard lock{_state->mutex};
                _state->dispatched.emplace_back(submission.target(), payload.value);
                if (_state->throw_on_dispatch)
                {
                    throw std::runtime_error{"synthetic binding failure"};
                }
                on_dispatch = _state->on_dispatch;
                on_dispatch_with_context = _state->on_dispatch_with_context;
            }
            if (on_dispatch)
            {
                on_dispatch(submission.target(), payload.value);
            }
            if (on_dispatch_with_context)
            {
                on_dispatch_with_context(context, submission.target(), payload.value);
            }
            return ActorDispatchResult::KeepActive;
        }

        // This binding never suspends, so the scheduler never resumes it.
        [[nodiscard]] ActorDispatchResult resume(ActorSlot&, ActorContext&, std::stop_token) override
        {
            throw std::logic_error{"SyntheticBinding does not suspend"};
        }

    private:
        struct Slot final : ActorSlot
        {
        };

        struct Payload
        {
            int value;
            bool evict;
        };

        ActorKind _kind;
        std::shared_ptr<State> _state;
    };

    void test_actor_key_affinity_and_registry_rules()
    {
        const ActorKey provisional{.kind = ActorKind::ProvisionalPlayer, .entity = 17};
        const ActorKey same_provisional{.kind = ActorKind::ProvisionalPlayer, .entity = 17};
        const ActorKey persistent_player{.kind = ActorKind::Player, .entity = 17};
        const ActorKey zone{.kind = ActorKind::Zone, .entity = 17};
        assert(provisional == same_provisional);
        assert(provisional != persistent_player);
        assert(persistent_player != zone);
        assert(provisional != zone);
        assert(snf::runtime::ActorKeyHash{}(provisional) == snf::runtime::ActorKeyHash{}(same_provisional));

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

    void test_player_binding_preserves_ping_pong_fifo_and_follow_up_order()
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
            const auto posted = dependencies.outbound.tryPop();
            assert(posted.has_value());
            const auto* send = std::get_if<snf::server::SendFrame>(&posted->action);
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
            .actor_kind = snf::runtime::ActorKind::ProvisionalPlayer,
            .repository = nullptr,
            .on_before_command =
                [state](snf::server::PlayerActorId, const snf::server::PlayerCommand&)
            {
                const int active = state->active.fetch_add(1) + 1;
                int maximum = state->maximum_active.load();
                while (maximum < active && !state->maximum_active.compare_exchange_weak(maximum, active))
                {
                }
                if (state->entered.fetch_add(1) + 1 == 2)
                {
                    state->both_entered.set_value();
                }
                state->release.wait();
                state->active.fetch_sub(1);
            },
            .on_actor_deactivated = {},
            .on_record_loaded = {},
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
        capacity_config.on_worker_start = [&capacity_worker_start_signal, release_capacity](std::size_t)
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

        const auto zone_dispatch = std::find_if(
            state->dispatched.begin(),
            state->dispatched.end(),
            [](const auto& event)
            {
                return event.second == 1000;
            }
        );
        const auto final_player_dispatch = std::find_if(
            state->dispatched.begin(),
            state->dispatched.end(),
            [](const auto& event)
            {
                return event.second == 32;
            }
        );
        assert(zone_dispatch != state->dispatched.end());
        assert(final_player_dispatch != state->dispatched.end());
        assert(zone_dispatch < final_player_dispatch);
        assert(runtime.getStats().workers.front().budget_yield_turns == 1);

        // Same numeric id remains independent by kind: evicting the Player slot
        // must never evict a Zone slot.
        RecordingRuntimeCompletion completion_two;
        ActorRuntime cross_kind{player_runtime_config(1, 8), completion_two};
        const auto cross_players = std::make_shared<SyntheticBinding::State>();
        const auto cross_persistent_players = std::make_shared<SyntheticBinding::State>();
        const auto cross_zones = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding player_binding{ActorKind::ProvisionalPlayer, cross_players};
        SyntheticBinding persistent_player_binding{ActorKind::Player, cross_persistent_players};
        SyntheticBinding zone_binding{ActorKind::Zone, cross_zones};
        cross_kind.registerBinding(player_binding);
        cross_kind.registerBinding(persistent_player_binding);
        cross_kind.registerBinding(zone_binding);
        cross_kind.start();
        assert(cross_kind.tryPost(player_binding.post(5, 1)) == PostResult::Accepted);
        assert(cross_kind.tryPost(zone_binding.post(5, 2)) == PostResult::Accepted);
        assert(cross_kind.tryPost(persistent_player_binding.post(5, 3)) == PostResult::Accepted);
        assert(cross_kind.tryPost(player_binding.evict(5)) == PostResult::Accepted);
        assert(cross_kind.tryPost(zone_binding.post(5, 4)) == PostResult::Accepted);
        assert(cross_kind.tryPost(persistent_player_binding.post(5, 5)) == PostResult::Accepted);
        cross_kind.close();
        cross_kind.join();
        assert(cross_zones->dispatched.size() == 2);
        assert(cross_persistent_players->dispatched.size() == 2);
        assert(completion_two.drained_count.load() == 1);
    }

    void test_control_submissions_consume_the_turn_budget()
    {
        RecordingRuntimeCompletion completion;
        ActorRuntime runtime{player_runtime_config(1, 64), completion};
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
        const auto gate = std::make_shared<Gate>();
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
        assert(runtime.tryPost(players.control(9, 1)) == PostResult::Accepted);
        assert(entered.wait_for(1s) == std::future_status::ready);
        for (int value = 2; value <= 32; ++value)
        {
            assert(runtime.tryPost(players.control(9, value)) == PostResult::Accepted);
        }
        assert(runtime.tryPost(zones.post(9, 1000)) == PostResult::Accepted);

        release.set_value();
        runtime.close();
        runtime.join();

        const auto zone_dispatch = std::find_if(
            state->dispatched.begin(),
            state->dispatched.end(),
            [](const auto& event)
            {
                return event.second == 1000;
            }
        );
        const auto final_control = std::find_if(
            state->dispatched.begin(),
            state->dispatched.end(),
            [](const auto& event)
            {
                return event.second == 32;
            }
        );
        assert(zone_dispatch != state->dispatched.end());
        assert(final_control != state->dispatched.end());
        assert(zone_dispatch < final_control);
        assert(runtime.getStats().workers.front().budget_yield_turns == 1);
    }

    void test_binding_activation_can_reenter_the_runtime()
    {
        RecordingRuntimeCompletion completion;
        ActorRuntime runtime{player_runtime_config(1, 4), completion};
        const auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        runtime.registerBinding(binding);

        std::promise<void> reentrant_posted;
        const auto posted = reentrant_posted.get_future();
        state->on_activate = [&runtime, &binding, &reentrant_posted](const EntityId entity)
        {
            assert(runtime.tryPost(binding.post(entity, 2)) == PostResult::Accepted);
            reentrant_posted.set_value();
        };

        runtime.start();
        assert(runtime.tryPost(binding.post(3, 1)) == PostResult::Accepted);
        assert(posted.wait_for(1s) == std::future_status::ready);
        runtime.close();
        runtime.join();

        assert(state->dispatched.size() == 2);
        assert(state->dispatched[0].second == 1);
        assert(state->dispatched[1].second == 2);
    }

    void test_full_shard_does_not_block_another_shard()
    {
        RecordingRuntimeCompletion completion;
        auto config = player_runtime_config(2, 2);
        ActorRuntime runtime{config, completion};
        const auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        runtime.registerBinding(binding);

        ActorKey blocked_key{.kind = ActorKind::Zone, .entity = 0};
        ActorKey other_key{.kind = ActorKind::Zone, .entity = 1};
        while (runtime.workerIndexFor(blocked_key) == runtime.workerIndexFor(other_key))
        {
            ++other_key.entity;
        }
        const std::size_t blocked_worker = runtime.workerIndexFor(blocked_key);
        const std::size_t other_worker = runtime.workerIndexFor(other_key);

        struct Gate
        {
            std::promise<void> blocked_started;
            std::promise<void> other_processed;
            std::shared_future<void> release;
        };
        const auto gate = std::make_shared<Gate>();
        std::promise<void> release;
        gate->release = release.get_future().share();
        const auto blocked_started = gate->blocked_started.get_future();
        const auto other_processed = gate->other_processed.get_future();
        state->on_dispatch = [gate, blocked_key, other_key](const ActorKey& key, const int value)
        {
            if (key == blocked_key && value == 1)
            {
                gate->blocked_started.set_value();
                gate->release.wait();
            }
            else if (key == other_key)
            {
                gate->other_processed.set_value();
            }
        };

        runtime.start();
        assert(runtime.tryPost(binding.post(blocked_key.entity, 1)) == PostResult::Accepted);
        assert(blocked_started.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(binding.post(blocked_key.entity, 2)) == PostResult::Accepted);
        assert(runtime.getStats().workers[blocked_worker].queue_depth == 2);
        assert(runtime.tryPost(binding.post(blocked_key.entity, 3)) == PostResult::Full);
        assert(runtime.tryPost(binding.post(other_key.entity, 4)) == PostResult::Accepted);
        assert(other_processed.wait_for(1s) == std::future_status::ready);

        release.set_value();
        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats();
        assert(stats.workers[blocked_worker].accepted == 2);
        assert(stats.workers[blocked_worker].rejected_full == 1);
        assert(stats.workers[blocked_worker].queue_high_water_mark == 2);
        assert(stats.workers[other_worker].accepted == 1);
        assert(stats.workers[other_worker].processed == 1);
    }

    void test_notifies_drain_only_after_every_worker_finishes()
    {
        RecordingRuntimeCompletion completion;
        ActorRuntime runtime{player_runtime_config(2, 4), completion};
        const auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        runtime.registerBinding(binding);

        ActorKey first{.kind = ActorKind::Zone, .entity = 0};
        ActorKey second{.kind = ActorKind::Zone, .entity = 1};
        while (runtime.workerIndexFor(first) == runtime.workerIndexFor(second))
        {
            ++second.entity;
        }

        struct Gate
        {
            std::promise<void> first_started;
            std::promise<void> second_started;
            std::promise<void> first_finished;
            std::shared_future<void> release_first;
            std::shared_future<void> release_second;
        };
        const auto gate = std::make_shared<Gate>();
        std::promise<void> release_first;
        std::promise<void> release_second;
        gate->release_first = release_first.get_future().share();
        gate->release_second = release_second.get_future().share();
        const auto first_started = gate->first_started.get_future();
        const auto second_started = gate->second_started.get_future();
        const auto first_finished = gate->first_finished.get_future();
        state->on_dispatch = [gate, first](const ActorKey& key, int)
        {
            if (key == first)
            {
                gate->first_started.set_value();
                gate->release_first.wait();
                gate->first_finished.set_value();
            }
            else
            {
                gate->second_started.set_value();
                gate->release_second.wait();
            }
        };

        runtime.start();
        assert(runtime.tryPost(binding.post(first.entity, 1)) == PostResult::Accepted);
        assert(runtime.tryPost(binding.post(second.entity, 2)) == PostResult::Accepted);
        assert(first_started.wait_for(1s) == std::future_status::ready);
        assert(second_started.wait_for(1s) == std::future_status::ready);
        runtime.close();

        release_first.set_value();
        assert(first_finished.wait_for(1s) == std::future_status::ready);
        assert(completion.drained_count.load() == 0);
        release_second.set_value();
        runtime.join();
        assert(completion.drained_count.load() == 1);
        assert(completion.failed_count.load() == 0);
    }

    void test_cancel_discards_submissions_already_routed_to_a_mailbox()
    {
        RecordingRuntimeCompletion completion;
        auto config = player_runtime_config(1, 3);
        std::promise<void> worker_started;
        std::promise<void> release_worker;
        const auto release_start = release_worker.get_future().share();
        const auto started = worker_started.get_future();
        config.on_worker_start = [&worker_started, release_start](std::size_t)
        {
            worker_started.set_value();
            release_start.wait();
        };

        ActorRuntime runtime{config, completion};
        const auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        runtime.registerBinding(binding);

        std::promise<void> first_dispatch_started;
        std::promise<void> release_dispatch;
        const auto dispatch_release = release_dispatch.get_future().share();
        const auto dispatch_started = first_dispatch_started.get_future();
        state->on_dispatch = [&first_dispatch_started, dispatch_release](const ActorKey&, const int value)
        {
            if (value == 1)
            {
                first_dispatch_started.set_value();
                dispatch_release.wait();
            }
        };

        runtime.start();
        assert(started.wait_for(1s) == std::future_status::ready);
        assert(runtime.tryPost(binding.post(5, 1)) == PostResult::Accepted);
        assert(runtime.tryPost(binding.post(5, 2)) == PostResult::Accepted);
        release_worker.set_value();
        assert(dispatch_started.wait_for(1s) == std::future_status::ready);
        assert(runtime.getStats().workers.front().mailbox_depth == 1);

        runtime.cancel();
        release_dispatch.set_value();
        runtime.join();

        assert(state->dispatched.size() == 1);
        const auto stats = runtime.getStats().workers.front();
        assert(stats.mailbox_depth == 0);
        assert(stats.queue_depth == 0);
        assert(completion.drained_count.load() == 0);
    }

    void test_aggregates_queue_wait_and_high_water_marks()
    {
        RecordingRuntimeCompletion completion;
        auto config = player_runtime_config(1, 4);
        std::promise<void> worker_started;
        std::promise<void> release_worker;
        const auto release = release_worker.get_future().share();
        const auto started = worker_started.get_future();
        config.on_worker_start = [&worker_started, release](std::size_t)
        {
            worker_started.set_value();
            release.wait();
        };

        ActorRuntime runtime{config, completion};
        const auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        runtime.registerBinding(binding);
        runtime.start();
        assert(started.wait_for(1s) == std::future_status::ready);

        assert(runtime.tryPost(binding.post(7, 1)) == PostResult::Accepted);
        assert(runtime.tryPost(binding.post(7, 2)) == PostResult::Accepted);
        assert(runtime.tryPost(binding.post(7, 3)) == PostResult::Accepted);
        std::this_thread::sleep_for(2ms);
        runtime.close();
        release_worker.set_value();
        runtime.join();

        const auto stats = runtime.getStats().workers.front();
        assert(stats.accepted == 3);
        assert(stats.processed == 3);
        assert(stats.queue_depth == 0);
        assert(stats.queue_high_water_mark == 3);
        assert(stats.mailbox_high_water_mark == 3);
        assert(stats.queue_wait_nanoseconds.sample_count == 3);
        assert(stats.queue_wait_nanoseconds.p50 > 0);
        assert(stats.queue_wait_nanoseconds.p99 >= stats.queue_wait_nanoseconds.p50);
        assert(stats.queue_wait_nanoseconds.max >= stats.queue_wait_nanoseconds.p99);
    }

    void test_player_close_follows_commands_and_preserves_command_metrics()
    {
        RuntimeDependencies dependencies{8};
        auto config = player_runtime_config(1, 8);
        std::promise<void> worker_started;
        std::promise<void> release_worker;
        const auto release = release_worker.get_future().share();
        const auto started = worker_started.get_future();
        config.on_worker_start = [&worker_started, release](std::size_t)
        {
            worker_started.set_value();
            release.wait();
        };

        PlayerRuntime player{dependencies, config};
        player.runtime.start();
        assert(started.wait_for(1s) == std::future_status::ready);
        assert(player.ingress.tryPost(make_command(3, 1)) == PostResult::Accepted);
        assert(player.ingress.tryPost(make_command(3, 2)) == PostResult::Accepted);
        assert(player.ingress.tryPost(make_command(3, 3)) == PostResult::Accepted);
        assert(player.ingress.tryPostConnectionClosed(snf::server::ProvisionalActorId{.value = 3}, make_closed(3)) == PostResult::Accepted);
        player.runtime.close();
        release_worker.set_value();
        player.runtime.join();

        for (std::uint32_t request_id = 1; request_id <= 3; ++request_id)
        {
            const auto posted = dependencies.outbound.tryPop();
            assert(posted.has_value());
            const auto* send = std::get_if<snf::server::SendFrame>(&posted->action);
            assert(send != nullptr);
            assert(send->frame.request_id == request_id);
        }
        assert(!dependencies.outbound.tryPop().has_value());

        const auto stats = player.runtime.getStats().workers.front();
        assert(stats.accepted == 3);
        assert(stats.processed == 3);
        assert(stats.rejected_full == 0);
        assert(stats.evicted_actors == 1);
        assert(stats.queue_depth == 0);
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
        PlayerRuntime player{dependencies, config};
        player.runtime.start();
        assert(worker_started.get_future().wait_for(1s) == std::future_status::ready);
        assert(player.ingress.tryPost(make_command(3, 1)) == PostResult::Accepted);
        assert(player.ingress.tryPostConnectionClosed(snf::server::ProvisionalActorId{.value = 3}, make_closed(3)) == PostResult::Full);
        assert(player.runtime.getStats().workers.front().rejected_full == 0);
        release.set_value();
        player.runtime.close();
        player.runtime.join();

        RuntimeDependencies empty_dependencies{8};
        PlayerRuntime empty_player{empty_dependencies, player_runtime_config(1, 2)};
        empty_player.runtime.start();
        assert(empty_player.ingress.tryPostConnectionClosed(snf::server::ProvisionalActorId{.value = 17}, make_closed(17)) == PostResult::Accepted);
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
            .actor_kind = snf::runtime::ActorKind::ProvisionalPlayer,
            .repository = nullptr,
            .on_before_command =
                [gate](snf::server::PlayerActorId, const snf::server::PlayerCommand&)
            {
                if (gate->handled.fetch_add(1) == 0)
                {
                    gate->first_handler_started.set_value();
                    gate->release.wait();
                }
            },
            .on_actor_deactivated = {},
            .on_record_loaded = {},
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

    void test_cancel_releases_an_actor_suspended_on_outbound_capacity()
    {
        RuntimeDependencies dependencies{1};
        PlayerRuntime player{dependencies, player_runtime_config(1, 2)};
        player.runtime.start();
        assert(player.ingress.tryPost(make_command(1, 1)) == PostResult::Accepted);

        auto deadline = std::chrono::steady_clock::now() + 1s;
        while (dependencies.outbound.size() != 1 && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::yield();
        }
        assert(dependencies.outbound.size() == 1);

        // The channel is full, so applying this command's follow-ups suspends its actor rather
        // than parking the Worker. A registered waiter is the observable proof.
        assert(player.ingress.tryPost(make_command(1, 2)) == PostResult::Accepted);
        deadline = std::chrono::steady_clock::now() + 1s;
        while (dependencies.outbound.pendingWaiterCount() != 1 && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::yield();
        }
        assert(dependencies.outbound.pendingWaiterCount() == 1);

        player.runtime.cancel();
        auto joined = std::async(
            std::launch::async,
            [&player]
            {
                player.runtime.join();
            }
        );
        const bool completed = joined.wait_for(1s) == std::future_status::ready;
        if (!completed)
        {
            static_cast<void>(dependencies.outbound.cancel());
        }
        joined.get();
        assert(completed);
        // Cancelling one runtime never cancels the shared channel: the action already
        // committed is still waiting for the reactor.
        assert(dependencies.outbound.size() == 1);
        assert(!dependencies.outbound.isCancelled());
        // Destroying a suspended frame runs the awaiting scope's destructors, so the
        // waiter withdraws itself rather than lingering in the registry.
        assert(dependencies.outbound.pendingWaiterCount() == 0);
    }

    void test_admitted_commands_and_refused_posts_are_counted_apart()
    {
        // Success, cancellation and mailbox discard in one run: three commands and a
        // close for the same actor, with the Worker parked until everything is queued.
        {
            RuntimeDependencies dependencies{8};
            auto config = player_runtime_config(1, 8);
            std::promise<void> worker_started;
            std::promise<void> release_worker;
            const auto release = release_worker.get_future().share();
            const auto started = worker_started.get_future();
            config.on_worker_start = [&worker_started, release](std::size_t)
            {
                worker_started.set_value();
                release.wait();
            };

            PlayerRuntime player{dependencies, config};
            player.runtime.start();
            assert(started.wait_for(1s) == std::future_status::ready);
            for (std::uint32_t request_id = 1; request_id <= 3; ++request_id)
            {
                assert(player.ingress.tryPost(make_command(9, request_id)) == PostResult::Accepted);
            }

            player.runtime.cancel();
            release_worker.set_value();
            player.runtime.join();

            // Cancelled and discarded commands reach a result exactly like the ones that
            // answered, and none of them was refused.
            assert(dependencies.lifecycle.releaseCount() == 3);
            assert(dependencies.lifecycle.admissionRejectionCount() == 0);
            assert(dependencies.lifecycle.terminalCount() == 3);
        }

        // A refused post still releases the credit it took at this boundary, but it is
        // not a command that ran, so it is counted apart.
        {
            RuntimeDependencies dependencies{8};
            auto config = player_runtime_config(1, 1);
            std::promise<void> worker_started;
            std::promise<void> release_worker;
            const auto release = release_worker.get_future().share();
            const auto started = worker_started.get_future();
            config.on_worker_start = [&worker_started, release](std::size_t)
            {
                worker_started.set_value();
                release.wait();
            };

            PlayerRuntime player{dependencies, config};
            player.runtime.start();
            assert(started.wait_for(1s) == std::future_status::ready);
            assert(player.ingress.tryPost(make_command(9, 1)) == PostResult::Accepted);
            assert(player.ingress.tryPost(make_command(9, 2)) == PostResult::Full);

            player.runtime.close();
            release_worker.set_value();
            player.runtime.join();

            assert(player.ingress.tryPost(make_command(9, 3)) == PostResult::Closed);

            // Three submissions released their credit, two of them because the runtime
            // refused the post. Only the admitted one reached a result.
            assert(dependencies.lifecycle.releaseCount() == 3);
            assert(dependencies.lifecycle.admissionRejectionCount() == 2);
            assert(dependencies.lifecycle.terminalCount() == 1);
        }
    }

    // Prices every result above what one connection may ever hold. This is the shape a
    // future multi-follow-up result takes when the per-connection limit is smaller than
    // the follow-up count.
    class OversizedResponseSink final : public snf::server::PlayerResponseSink
    {
    public:
        explicit OversizedResponseSink(const std::size_t slots) noexcept
            : _slots(slots)
        {
        }

        [[nodiscard]] std::size_t requiredSlots(const snf::server::PlayerResult&) const noexcept override
        {
            return _slots;
        }

        [[nodiscard]] bool
        applyResponses(snf::net::ConnectionId, std::uint32_t, snf::server::PlayerResult, snf::server::OutboundReservation&) override
        {
            applied = true;
            return true;
        }

        bool applied{false};

    private:
        std::size_t _slots;
    };

    void test_an_unsatisfiable_result_closes_the_connection_instead_of_failing_the_worker()
    {
        RuntimeDependencies dependencies{2};
        OversizedResponseSink response_sink{3};
        snf::server::PlayerActorBinding binding{response_sink, dependencies.outbound, dependencies.lifecycle};
        ActorRuntime runtime{player_runtime_config(1, 8), dependencies.completion};
        runtime.registerBinding(binding);
        snf::server::PlayerActorIngress ingress{runtime, binding, dependencies.lifecycle};

        runtime.start();
        assert(ingress.tryPost(make_command(1, 1)) == PostResult::Accepted);

        std::vector<snf::net::ConnectionId> failures;
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (failures.empty() && std::chrono::steady_clock::now() < deadline)
        {
            const bool used_fail_safe = dependencies.outbound.takePendingAdmissionFailures(failures);
            assert(!used_fail_safe);
            std::this_thread::yield();
        }

        // Reported for closing, not thrown: one oversized result must not take down every
        // actor the Worker owns.
        assert(failures.size() == 1);
        assert(!response_sink.applied);

        runtime.close();
        runtime.join();
        assert(dependencies.completion.drained_count.load() == 1);
        assert(dependencies.completion.failed_count.load() == 0);
        assert(dependencies.outbound.pendingWaiterCount() == 0);
    }

    void test_exhausted_in_flight_budget_closes_the_connection_instead_of_dropping_a_response()
    {
        RuntimeDependencies dependencies{1};
        auto config = player_runtime_config(1, 8);
        config.max_in_flight_operations_per_worker = 1;
        PlayerRuntime player{dependencies, config};
        player.runtime.start();

        // The first command fills the only outbound slot, the second suspends on the
        // only in-flight slot, and the third has nowhere left to wait.
        for (std::uint64_t actor_id = 1; actor_id <= 3; ++actor_id)
        {
            assert(player.ingress.tryPost(make_command(actor_id, 1)) == PostResult::Accepted);
        }

        std::vector<snf::net::ConnectionId> failures;
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (failures.empty() && std::chrono::steady_clock::now() < deadline)
        {
            const bool used_fail_safe = dependencies.outbound.takePendingAdmissionFailures(failures);
            assert(!used_fail_safe);
            std::this_thread::yield();
        }

        // The response is not dropped in silence: the connection is reported so the
        // reactor closes it under the overflow policy.
        assert(failures.size() == 1);
        assert(dependencies.outbound.pendingWaiterCount() == 1);

        player.runtime.cancel();
        player.runtime.join();
        const auto stats = player.runtime.getStats().workers.front();
        assert(stats.reservation_rejections == 1);
        assert(stats.in_flight_operations == 0);
    }

    void test_saturated_outbound_preserves_follow_up_order_and_handler_atomicity()
    {
        RuntimeDependencies dependencies{1};
        PlayerRuntime player{dependencies, player_runtime_config(1, 8)};
        player.runtime.start();
        for (std::uint32_t request_id = 1; request_id <= 3; ++request_id)
        {
            assert(player.ingress.tryPost(make_command(5, request_id)) == PostResult::Accepted);
        }

        std::vector<std::uint32_t> emitted;
        // Stands in for the reactor: drain, then grant whatever the drain freed. With a
        // capacity of one, every command after the first has to wait for a grant.
        const auto pump = [&dependencies, &emitted]
        {
            while (auto posted = dependencies.outbound.tryPop())
            {
                emitted.push_back(std::get<snf::server::SendFrame>(posted->action).frame.request_id);
            }

            static_cast<void>(dependencies.outbound.grantPending());
        };

        player.runtime.close();
        auto joined = std::async(
            std::launch::async,
            [&player]
            {
                player.runtime.join();
            }
        );
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (joined.wait_for(1ms) != std::future_status::ready && std::chrono::steady_clock::now() < deadline)
        {
            pump();
        }
        pump();

        const bool completed = joined.wait_for(1s) == std::future_status::ready;
        if (!completed)
        {
            static_cast<void>(dependencies.outbound.cancel());
        }
        joined.get();
        assert(completed);

        assert((emitted == std::vector<std::uint32_t>{1, 2, 3}));
        const auto stats = player.runtime.getStats().workers.front();
        assert(stats.processed == 3);
        // At least one command could not emit immediately, which is the only way this
        // order could have been produced without a Worker ever blocking.
        assert(stats.suspended_commands >= 1);
        assert(stats.in_flight_operations == 0);
        assert(dependencies.completion.drained_count.load() == 1);
        assert(dependencies.completion.failed_count.load() == 0);
    }

    void test_player_repository_wait_suspends_only_the_loading_actor()
    {
        RuntimeDependencies dependencies{8};
        DelayedLoadPlayerRepository repository;
        snf::server::PlayerActorBinding provisional{dependencies.outbound_sink, dependencies.outbound, dependencies.lifecycle};
        snf::server::PlayerActorBinding persistent{
            dependencies.outbound_sink,
            dependencies.outbound,
            dependencies.lifecycle,
            snf::server::PlayerActorBindingConfig{
                .actor_kind = ActorKind::Player,
                .repository = &repository,
                .on_before_command = {},
                .on_actor_deactivated = {},
                .on_record_loaded = {},
            }
        };
        ActorRuntime runtime{player_runtime_config(1, 8), dependencies.completion};
        runtime.registerBinding(provisional);
        runtime.registerBinding(persistent);
        snf::server::PlayerActorIngress ingress{runtime, provisional, persistent, dependencies.lifecycle};
        runtime.start();

        const snf::server::PlayerId player{.value = 77};
        const snf::net::ConnectionId player_connection{.descriptor = 70, .generation = 700};
        assert(
            ingress.tryPost(snf::server::PlayerInboundCommand{
                .actor = player,
                .connection = player_connection,
                .command =
                    snf::server::AuthenticateCommand{
                        .player = player,
                    },
                .request_id = 1,
            }) == PostResult::Accepted
        );
        assert(repository.load_requested_future.wait_for(1s) == std::future_status::ready);
        assert(repository.requested_player == player);

        const snf::net::ConnectionId provisional_connection{
            .descriptor = 71,
            .generation = 701,
        };
        assert(
            ingress.tryPost(snf::server::PlayerInboundCommand{
                .actor = snf::server::ProvisionalActorId{.value = 701},
                .connection = provisional_connection,
                .command = snf::server::PingCommand{.payload = {}},
                .request_id = 2,
            }) == PostResult::Accepted
        );

        std::optional<snf::server::PostedOutboundAction> pong;
        const auto pong_deadline = std::chrono::steady_clock::now() + 1s;
        while (!pong && std::chrono::steady_clock::now() < pong_deadline)
        {
            pong = dependencies.outbound.tryPop();
            if (!pong)
            {
                std::this_thread::sleep_for(1ms);
            }
        }
        assert(pong.has_value());
        assert(std::get<snf::server::SendFrame>(pong->action).frame.request_id == 2);

        // The close snapshot is deliberately unknown because the disconnect raced
        // the repository load. The binding must retain the location the load restores.
        assert(
            ingress.tryPostConnectionClosed(
                player,
                snf::server::ConnectionClosed{
                    .connection = player_connection,
                    .cause = snf::server::ConnectionCloseCause::PeerClosed,
                    .has_location_snapshot = false,
                    .last_location = std::nullopt,
                }
            ) == PostResult::Accepted
        );
        repository.completeLoad(snf::server::PlayerRecord{
            .player = player,
            .handled_command_count = 0,
            .last_location =
                snf::server::PlayerLocation{
                    .zone = snf::server::ZoneId{.value = 9},
                    .position = {.x = 17, .y = -19},
                },
            .currency_balance = snf::server::INITIAL_CURRENCY_BALANCE,
            .purchased_item_count = 0,
        });
        std::optional<snf::server::PostedOutboundAction> authenticated;
        const auto auth_deadline = std::chrono::steady_clock::now() + 1s;
        while (!authenticated && std::chrono::steady_clock::now() < auth_deadline)
        {
            authenticated = dependencies.outbound.tryPop();
            if (!authenticated)
            {
                std::this_thread::sleep_for(1ms);
            }
        }
        assert(authenticated.has_value());
        const auto& auth_frame = std::get<snf::server::SendFrame>(authenticated->action).frame;
        assert(auth_frame.type == snf::protocol::MessageType::Authenticated);
        assert(auth_frame.request_id == 1);

        assert(
            ingress.tryPostConnectionClosed(
                snf::server::ProvisionalActorId{.value = 701},
                snf::server::ConnectionClosed{
                    .connection = provisional_connection,
                    .cause = snf::server::ConnectionCloseCause::PeerClosed,
                    .has_location_snapshot = false,
                    .last_location = std::nullopt,
                }
            ) == PostResult::Accepted
        );
        runtime.close();
        runtime.join();

        assert(repository.saved.has_value());
        assert(repository.saved->player == player);
        assert(repository.saved->handled_command_count == 1);
        assert(
            (repository.saved->last_location ==
             snf::server::PlayerLocation{
                 .zone = snf::server::ZoneId{.value = 9},
                 .position = {.x = 17, .y = -19},
             })
        );
        const auto stats = runtime.getStats().workers.front();
        assert(stats.suspended_commands >= 2);
        assert(stats.in_flight_operations == 0);
    }

    void test_live_purchase_does_not_suspend_on_repository()
    {
        RuntimeDependencies dependencies{8};
        DelayedLoadPlayerRepository repository;
        snf::server::PlayerActorBinding provisional{dependencies.outbound_sink, dependencies.outbound, dependencies.lifecycle};
        snf::server::PlayerActorBinding persistent{
            dependencies.outbound_sink,
            dependencies.outbound,
            dependencies.lifecycle,
            snf::server::PlayerActorBindingConfig{
                .actor_kind = ActorKind::Player,
                .repository = &repository,
                .on_before_command = {},
                .on_actor_deactivated = {},
                .on_record_loaded = {},
            }
        };
        ActorRuntime runtime{player_runtime_config(1, 8), dependencies.completion};
        runtime.registerBinding(provisional);
        runtime.registerBinding(persistent);
        snf::server::PlayerActorIngress ingress{runtime, provisional, persistent, dependencies.lifecycle};
        runtime.start();

        const snf::server::PlayerId player{.value = 781};
        const snf::net::ConnectionId connection{.descriptor = 75, .generation = 705};
        assert(
            ingress.tryPost(snf::server::PlayerInboundCommand{
                .actor = player,
                .connection = connection,
                .command =
                    snf::server::AuthenticateCommand{
                        .player = player,
                    },
                .request_id = 1,
            }) == PostResult::Accepted
        );
        assert(repository.load_requested_future.wait_for(1s) == std::future_status::ready);
        repository.completeLoad(snf::server::PlayerRecord{
            .player = player,
            .handled_command_count = 0,
            .last_location = std::nullopt,
            .currency_balance = snf::server::INITIAL_CURRENCY_BALANCE,
            .purchased_item_count = 0,
        });

        std::optional<snf::server::PostedOutboundAction> authenticated;
        const auto auth_deadline = std::chrono::steady_clock::now() + 1s;
        while (!authenticated && std::chrono::steady_clock::now() < auth_deadline)
        {
            authenticated = dependencies.outbound.tryPop();
            if (!authenticated)
            {
                std::this_thread::sleep_for(1ms);
            }
        }
        assert(authenticated.has_value());

        const snf::server::PurchaseCommand command{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 7810},
            .product = snf::server::BASIC_PRODUCT,
        };
        constexpr std::uint32_t purchase_request_id = 55;
        assert(
            ingress.tryPost(snf::server::PlayerInboundCommand{
                .actor = player,
                .connection = connection,
                .command = command,
                .request_id = purchase_request_id,
            }) == PostResult::Accepted
        );

        std::optional<snf::server::PostedOutboundAction> purchase;
        const auto purchase_deadline = std::chrono::steady_clock::now() + 1s;
        while (!purchase && std::chrono::steady_clock::now() < purchase_deadline)
        {
            purchase = dependencies.outbound.tryPop();
            if (!purchase)
            {
                std::this_thread::sleep_for(1ms);
            }
        }
        assert(purchase.has_value());
        const auto& frame = std::get<snf::server::SendFrame>(purchase->action).frame;
        assert(frame.type == snf::protocol::MessageType::PurchaseResult);
        assert(frame.request_id == purchase_request_id);
        assert(
            ingress.tryPostConnectionClosed(
                player,
                snf::server::ConnectionClosed{
                    .connection = connection,
                    .cause = snf::server::ConnectionCloseCause::PeerClosed,
                    .has_location_snapshot = false,
                    .last_location = std::nullopt,
                }
            ) == PostResult::Accepted
        );
        runtime.close();
        runtime.join();

        assert(repository.saved.has_value());
        assert(repository.saved->currency_balance == 900);
        assert(repository.saved->purchased_item_count == 1);
    }

    void test_scheduled_alarm_fires_and_executes_handler()
    {
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        RecordingRuntimeCompletion completion;
        ActorRuntime runtime{player_runtime_config(1, 16), completion};
        runtime.registerBinding(binding);
        runtime.start();

        std::optional<snf::runtime::TimerHandle> handle;
        state->on_dispatch_with_context = [&](ActorContext& context, const ActorKey&, int value)
        {
            if (value == 1)
            {
                handle = context.trySchedule(10ms, binding.post(1, 2));
                assert(handle.has_value());
            }
        };

        assert(runtime.tryPost(binding.post(1, 1)) == PostResult::Accepted);

        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (std::chrono::steady_clock::now() < deadline)
        {
            {
                std::lock_guard lock{state->mutex};
                if (state->dispatched.size() >= 2)
                {
                    break;
                }
            }
            std::this_thread::sleep_for(1ms);
        }

        runtime.close();
        runtime.join();

        {
            std::lock_guard lock{state->mutex};
            assert(state->dispatched.size() == 2);
            assert(state->dispatched[0].second == 1);
            assert(state->dispatched[1].second == 2);
        }

        const auto stats = runtime.getStats().workers.front();
        assert(stats.timers_scheduled == 1);
        assert(stats.timers_fired == 1);
        assert(stats.timers_cancelled == 0);
        assert(stats.timer_lateness_nanoseconds.sample_count == 1);
    }

    void test_cancelled_timer_does_not_fire()
    {
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        RecordingRuntimeCompletion completion;
        ActorRuntime runtime{player_runtime_config(1, 16), completion};
        runtime.registerBinding(binding);
        runtime.start();

        std::optional<snf::runtime::TimerHandle> handle;
        state->on_dispatch_with_context = [&](ActorContext& context, const ActorKey&, int value)
        {
            if (value == 1)
            {
                handle = context.trySchedule(100ms, binding.post(1, 2));
                assert(handle.has_value());
            }
            else if (value == 3)
            {
                assert(handle.has_value());
                context.cancelTimer(*handle);
            }
        };

        assert(runtime.tryPost(binding.post(1, 1)) == PostResult::Accepted);
        assert(runtime.tryPost(binding.post(1, 3)) == PostResult::Accepted);

        std::this_thread::sleep_for(150ms);

        runtime.close();
        runtime.join();

        {
            std::lock_guard lock{state->mutex};
            assert(state->dispatched.size() == 2);
            assert(state->dispatched[0].second == 1);
            assert(state->dispatched[1].second == 3);
        }

        const auto stats = runtime.getStats().workers.front();
        assert(stats.timers_scheduled == 1);
        assert(stats.timers_cancelled == 1);
        assert(stats.timers_fired == 0);
    }

    void test_evicted_and_reactivated_actor_ignores_or_purges_old_alarm()
    {
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        RecordingRuntimeCompletion completion;
        ActorRuntime runtime{player_runtime_config(1, 16), completion};
        runtime.registerBinding(binding);
        runtime.start();

        state->on_dispatch_with_context = [&](ActorContext& context, const ActorKey&, int value)
        {
            if (value == 1)
            {
                auto handle = context.trySchedule(100ms, binding.post(1, 2));
                assert(handle.has_value());
            }
        };

        assert(runtime.tryPost(binding.post(1, 1)) == PostResult::Accepted);

        const auto first_deadline = std::chrono::steady_clock::now() + 1s;
        while (std::chrono::steady_clock::now() < first_deadline)
        {
            {
                std::lock_guard lock{state->mutex};
                if (!state->dispatched.empty())
                {
                    break;
                }
            }
            std::this_thread::sleep_for(1ms);
        }

        assert(runtime.tryPost(binding.evict(1)) == PostResult::Accepted);

        const auto evict_deadline = std::chrono::steady_clock::now() + 1s;
        while (std::chrono::steady_clock::now() < evict_deadline)
        {
            if (runtime.getStats().workers.front().evicted_actors >= 1)
            {
                break;
            }
            std::this_thread::sleep_for(1ms);
        }

        assert(runtime.tryPost(binding.post(1, 3)) == PostResult::Accepted);

        std::this_thread::sleep_for(150ms);

        runtime.close();
        runtime.join();

        {
            std::lock_guard lock{state->mutex};
            assert(state->dispatched.size() == 2);
            assert(state->dispatched[0].second == 1);
            assert(state->dispatched[1].second == 3);
        }

        const auto stats = runtime.getStats().workers.front();
        assert(stats.timers_scheduled == 1);
        assert(stats.timers_cancelled == 1);
        assert(stats.timers_fired == 0);
    }

    void test_timer_capacity_rejection_returns_nullopt()
    {
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        RecordingRuntimeCompletion completion;
        ActorRuntime runtime{player_runtime_config(1, 2), completion};
        runtime.registerBinding(binding);
        runtime.start();

        std::optional<bool> schedule_succeeded;
        std::promise<void> first_dispatch_started;
        auto first_dispatch = first_dispatch_started.get_future();
        std::promise<void> release_first_dispatch;
        auto release = release_first_dispatch.get_future().share();
        std::atomic<bool> first{true};

        state->on_dispatch_with_context = [&](ActorContext& context, const ActorKey&, int value)
        {
            if (value == 1 && first.exchange(false))
            {
                first_dispatch_started.set_value();
                release.wait();
                auto handle = context.trySchedule(10ms, binding.post(1, 99));
                schedule_succeeded = handle.has_value();
            }
        };

        assert(runtime.tryPost(binding.post(1, 1)) == PostResult::Accepted);
        first_dispatch.wait();

        assert(runtime.tryPost(binding.post(1, 2)) == PostResult::Accepted);

        release_first_dispatch.set_value();

        runtime.close();
        runtime.join();

        assert(schedule_succeeded.has_value());
        assert(!*schedule_succeeded);

        const auto stats = runtime.getStats().workers.front();
        assert(stats.timers_rejected_full == 1);
    }

    void test_repeating_timer_drains_cleanly_on_close()
    {
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        RecordingRuntimeCompletion completion;
        ActorRuntime runtime{player_runtime_config(1, 16), completion};
        runtime.registerBinding(binding);
        runtime.start();

        state->on_dispatch_with_context = [&](ActorContext& context, const ActorKey&, int value)
        {
            static_cast<void>(context.trySchedule(5ms, binding.post(1, value + 1)));
        };

        assert(runtime.tryPost(binding.post(1, 1)) == PostResult::Accepted);

        const auto deadline = std::chrono::steady_clock::now() + 100ms;
        while (std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(5ms);
        }

        runtime.close();
        runtime.join();

        assert(completion.drained_count.load() == 1);
        const auto stats = runtime.getStats().workers.front();
        assert(stats.timers_fired >= 3);
        assert(stats.queue_depth == 0);
    }

    void test_timer_accounting_invariants_and_cross_actor_rejection()
    {
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        RecordingRuntimeCompletion completion;
        ActorRuntime runtime{player_runtime_config(1, 16), completion};
        runtime.registerBinding(binding);
        runtime.start();

        bool cross_actor_threw = false;
        state->on_dispatch_with_context = [&](ActorContext& context, const ActorKey&, int value)
        {
            if (value == 1)
            {
                try
                {
                    static_cast<void>(context.trySchedule(10ms, binding.post(2, 99)));
                }
                catch (const std::invalid_argument&)
                {
                    cross_actor_threw = true;
                }
            }
        };

        assert(runtime.tryPost(binding.post(1, 1)) == PostResult::Accepted);

        runtime.close();
        runtime.join();

        assert(cross_actor_threw);
        const auto stats = runtime.getStats().workers.front();
        assert(stats.queue_depth == 0);
    }

    ActorKey key_on_worker(const ActorRuntime& runtime, const ActorKind kind, const std::size_t worker_index, const EntityId first_entity)
    {
        for (EntityId entity = first_entity; entity < first_entity + 4096; ++entity)
        {
            const ActorKey key{.kind = kind, .entity = entity};
            if (runtime.workerIndexFor(key) == worker_index)
            {
                return key;
            }
        }

        throw std::logic_error{"No entity hashes to the requested worker"};
    }

    bool wait_for_dispatch(SyntheticBinding::State& state, const ActorKey& key, const int value)
    {
        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (std::chrono::steady_clock::now() < deadline)
        {
            {
                std::lock_guard lock{state.mutex};
                for (const auto& [dispatched_key, dispatched_value] : state.dispatched)
                {
                    if (dispatched_key == key && dispatched_value == value)
                    {
                        return true;
                    }
                }
            }
            std::this_thread::yield();
        }

        return false;
    }

    // Sends one tell from inside a dispatch and reports whether it arrived. The
    // guard on the sentinel value is what keeps the target's own dispatch from
    // telling again.
    bool tell_arrives(const std::size_t worker_count, const bool same_worker, RecordingRuntimeCompletion& completion)
    {
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        ActorRuntime runtime{player_runtime_config(worker_count, 8), completion};
        runtime.registerBinding(binding);

        const ActorKey sender = key_on_worker(runtime, ActorKind::Zone, 0, 1);
        const ActorKey target = key_on_worker(runtime, ActorKind::Zone, same_worker ? 0 : worker_count - 1, sender.entity + 1);
        assert((runtime.workerIndexFor(sender) == runtime.workerIndexFor(target)) == same_worker);

        state->on_dispatch_with_context = [target](ActorContext& context, const ActorKey&, const int value)
        {
            if (value != 1)
            {
                return;
            }
            assert(context.tryTell(target, TellPayload::of(2)) == PostResult::Accepted);
        };

        runtime.start();
        assert(runtime.tryPost(binding.post(sender.entity, 1)) == PostResult::Accepted);

        const bool arrived = wait_for_dispatch(*state, target, 2);
        runtime.close();
        runtime.join();
        return arrived;
    }

    void test_a_tell_reaches_a_target_on_the_same_worker()
    {
        RecordingRuntimeCompletion completion;
        assert(tell_arrives(1, true, completion));
    }

    void test_a_tell_reaches_a_target_on_another_worker()
    {
        RecordingRuntimeCompletion completion;
        assert(tell_arrives(4, false, completion));
    }

    void test_a_tell_is_refused_when_the_target_worker_is_full()
    {
        RecordingRuntimeCompletion completion;
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        // One worker, one outstanding slot: the command being dispatched still holds
        // it, so a tell to that same worker cannot reserve one.
        ActorRuntime runtime{player_runtime_config(1, 1), completion};
        runtime.registerBinding(binding);

        std::promise<PostResult> tell_result;
        state->on_dispatch_with_context = [&tell_result](ActorContext& context, const ActorKey& key, const int value)
        {
            if (value != 1)
            {
                return;
            }
            tell_result.set_value(context.tryTell(ActorKey{.kind = ActorKind::Zone, .entity = key.entity + 1}, TellPayload::of(2)));
        };

        runtime.start();
        assert(runtime.tryPost(binding.post(1, 1)) == PostResult::Accepted);

        auto result = tell_result.get_future();
        assert(result.wait_for(2s) == std::future_status::ready);
        assert(result.get() == PostResult::Full);

        runtime.close();
        runtime.join();
    }

    void test_tells_sent_in_one_turn_arrive_in_order()
    {
        RecordingRuntimeCompletion completion;
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        ActorRuntime runtime{player_runtime_config(2, 16), completion};
        runtime.registerBinding(binding);

        const ActorKey sender = key_on_worker(runtime, ActorKind::Zone, 0, 1);
        const ActorKey target = key_on_worker(runtime, ActorKind::Zone, 1, sender.entity + 1);

        state->on_dispatch_with_context = [target](ActorContext& context, const ActorKey&, const int value)
        {
            if (value != 1)
            {
                return;
            }
            for (const int payload : {10, 20, 30})
            {
                assert(context.tryTell(target, TellPayload::of(payload)) == PostResult::Accepted);
            }
        };

        runtime.start();
        assert(runtime.tryPost(binding.post(sender.entity, 1)) == PostResult::Accepted);
        assert(wait_for_dispatch(*state, target, 30));

        // FIFO holds for one sender's turn, which is the only ordering a mailbox
        // can promise. Two senders on different Workers interleave by design.
        std::vector<int> seen;
        {
            std::lock_guard lock{state->mutex};
            for (const auto& [key, value] : state->dispatched)
            {
                if (key == target)
                {
                    seen.push_back(value);
                }
            }
        }
        assert((seen == std::vector<int>{10, 20, 30}));

        runtime.close();
        runtime.join();
    }

    void test_a_tell_activates_a_target_that_is_not_resident()
    {
        RecordingRuntimeCompletion completion;
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        ActorRuntime runtime{player_runtime_config(2, 8), completion};
        runtime.registerBinding(binding);

        const ActorKey sender = key_on_worker(runtime, ActorKind::Zone, 0, 1);
        const ActorKey target = key_on_worker(runtime, ActorKind::Zone, 1, sender.entity + 1);

        std::mutex activated_mutex;
        std::vector<EntityId> activated;
        state->on_activate = [&](const EntityId entity)
        {
            std::lock_guard lock{activated_mutex};
            activated.push_back(entity);
        };
        state->on_dispatch_with_context = [target](ActorContext& context, const ActorKey&, const int value)
        {
            if (value != 1)
            {
                return;
            }
            assert(context.tryTell(target, TellPayload::of(2)) == PostResult::Accepted);
        };

        runtime.start();
        assert(runtime.tryPost(binding.post(sender.entity, 1)) == PostResult::Accepted);
        assert(wait_for_dispatch(*state, target, 2));

        // ActivateIfMissing: a reward for a passivated actor must still arrive.
        {
            std::lock_guard lock{activated_mutex};
            assert(std::find(activated.begin(), activated.end(), target.entity) != activated.end());
        }

        runtime.close();
        runtime.join();
    }

    void test_a_tell_is_closed_after_the_runtime_stops_accepting_input()
    {
        RecordingRuntimeCompletion completion;
        auto state = std::make_shared<SyntheticBinding::State>();
        SyntheticBinding binding{ActorKind::Zone, state};
        ActorRuntime runtime{player_runtime_config(2, 8), completion};
        runtime.registerBinding(binding);

        runtime.start();
        runtime.close();

        // The target binding still assembles the submission; tryPost is what refuses
        // it, so a tell and a reactor command report the same shutdown state.
        assert(runtime.tryTell(ActorKey{.kind = ActorKind::Zone, .entity = 1}, TellPayload::of(7)) == PostResult::Closed);

        runtime.join();
    }
}

void test_observed_at_is_when_the_turn_ran_not_when_the_command_arrived()
{
    auto state = std::make_shared<SyntheticBinding::State>();
    std::mutex observed_mutex;
    std::vector<std::chrono::steady_clock::time_point> observed;

    state->on_dispatch_with_context = [&](ActorContext& context, const ActorKey&, const int value)
    {
        {
            const std::lock_guard lock{observed_mutex};
            observed.push_back(context.observedAt());
        }
        if (value == 1)
        {
            // Holds the Worker, so the second command waits in the mailbox. That
            // wait is the whole point: it must show up in the time the second turn
            // reports, not be hidden by the moment the command was accepted.
            std::this_thread::sleep_for(60ms);
        }
    };

    SyntheticBinding binding{ActorKind::Zone, state};
    RecordingRuntimeCompletion completion;
    ActorRuntime runtime{player_runtime_config(1, 16), completion};
    runtime.registerBinding(binding);
    runtime.start();

    const auto posted_at = std::chrono::steady_clock::now();
    assert(runtime.tryPost(binding.post(1, 1)) == PostResult::Accepted);
    assert(runtime.tryPost(binding.post(1, 2)) == PostResult::Accepted);

    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < deadline)
    {
        {
            const std::lock_guard lock{observed_mutex};
            if (observed.size() >= 2)
            {
                break;
            }
        }
        std::this_thread::sleep_for(1ms);
    }
    runtime.close();
    runtime.join();

    const std::lock_guard lock{observed_mutex};
    assert(observed.size() >= 2);
    // Both were accepted at once, so an arrival-time reading would put the second
    // at posted_at. It ran after the first held the Worker for 60ms instead.
    assert(observed[1] >= posted_at + 60ms);
    assert(observed[0] < observed[1]);
    assert(completion.failed_count.load() == 0);
}
void run_actor_runtime_tests()
{
    test_observed_at_is_when_the_turn_ran_not_when_the_command_arrived();
    test_actor_key_affinity_and_registry_rules();
    test_player_binding_preserves_ping_pong_fifo_and_follow_up_order();
    test_same_actor_is_serial_and_different_shards_run_in_parallel();
    test_synthetic_bindings_share_capacity_fairness_and_cross_kind_slots();
    test_control_submissions_consume_the_turn_budget();
    test_binding_activation_can_reenter_the_runtime();
    test_full_shard_does_not_block_another_shard();
    test_notifies_drain_only_after_every_worker_finishes();
    test_cancel_discards_submissions_already_routed_to_a_mailbox();
    test_aggregates_queue_wait_and_high_water_marks();
    test_player_close_follows_commands_and_preserves_command_metrics();
    test_capacity_and_lifecycle_control_accounting();
    test_cancel_and_failure_terminal_paths();
    test_cancel_releases_an_actor_suspended_on_outbound_capacity();
    test_saturated_outbound_preserves_follow_up_order_and_handler_atomicity();
    test_exhausted_in_flight_budget_closes_the_connection_instead_of_dropping_a_response();
    test_an_unsatisfiable_result_closes_the_connection_instead_of_failing_the_worker();
    test_admitted_commands_and_refused_posts_are_counted_apart();
    test_player_repository_wait_suspends_only_the_loading_actor();
    test_live_purchase_does_not_suspend_on_repository();
    test_scheduled_alarm_fires_and_executes_handler();
    test_cancelled_timer_does_not_fire();
    test_evicted_and_reactivated_actor_ignores_or_purges_old_alarm();
    test_timer_capacity_rejection_returns_nullopt();
    test_repeating_timer_drains_cleanly_on_close();
    test_timer_accounting_invariants_and_cross_actor_rejection();
    test_a_tell_reaches_a_target_on_the_same_worker();
    test_a_tell_reaches_a_target_on_another_worker();
    test_a_tell_is_refused_when_the_target_worker_is_full();
    test_tells_sent_in_one_turn_arrive_in_order();
    test_a_tell_activates_a_target_that_is_not_resident();
    test_a_tell_is_closed_after_the_runtime_stops_accepting_input();
}
