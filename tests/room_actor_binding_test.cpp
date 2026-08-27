#include "snf/game/room_join_request.hpp"
#include "snf/game/street_experience_grant.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/room_actor_binding.hpp"
#include "snf/server/room_actor_ingress.hpp"
#include "snf/server/room_join_tell.hpp"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <variant>
#include <vector>

import snf.game.skill_catalog;

using namespace std::chrono_literals;

namespace
{
    class RecordingCompletion final : public snf::runtime::RuntimeCompletionSink
    {
    public:
        void notifyDrained(snf::runtime::RuntimeId) noexcept override
        {
            ++drained;
        }

        void notifyFailed(snf::runtime::RuntimeId) noexcept override
        {
            if (failed.fetch_add(1) == 0)
            {
                try
                {
                    failure_reported.set_value();
                }
                catch (...)
                {
                }
            }
        }

        [[nodiscard]] std::future<void> failureReported()
        {
            return failure_reported.get_future();
        }

        std::atomic<int> drained{0};
        std::atomic<int> failed{0};

    private:
        std::promise<void> failure_reported;
    };

    class RecordingPlayerBinding final : public snf::runtime::ActorBinding
    {
    public:
        struct Grant
        {
            std::uint64_t entity{0};
            std::uint64_t experience{0};

            [[nodiscard]] bool operator==(const Grant&) const noexcept = default;
        };

        [[nodiscard]] snf::runtime::ActorKind kind() const noexcept override
        {
            return snf::runtime::ActorKind::Player;
        }

        std::mutex mutex;
        std::vector<Grant> grants;
        std::size_t expected{0};
        std::promise<void> all_arrived;

    protected:
        [[nodiscard]] std::optional<snf::runtime::ActorSubmission>
        makeTell(const snf::runtime::ActorKey target, snf::runtime::TellPayload payload) override
        {
            auto grant = payload.take<snf::server::StreetExperienceGrant>();
            if (!grant)
            {
                return std::nullopt;
            }
            return makeSubmission(
                target,
                snf::runtime::ActorActivation::ActivateIfMissing,
                snf::runtime::ActorAccounting::Command,
                GrantPayload{.experience = grant->experience}
            );
        }

        [[nodiscard]] std::unique_ptr<snf::runtime::ActorState> activate(snf::runtime::EntityId) override
        {
            return std::make_unique<Slot>();
        }

        [[nodiscard]] snf::runtime::ActorDispatchResult
        dispatch(snf::runtime::ActorState&, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext&, std::stop_token) override
        {
            const GrantPayload& payload = payloadAs<GrantPayload>(submission);
            {
                std::lock_guard lock{mutex};
                grants.push_back(Grant{.entity = submission.target().entity, .experience = payload.experience});
                if (grants.size() == expected)
                {
                    all_arrived.set_value();
                }
            }
            return snf::runtime::ActorDispatchResult::PassivateIfIdle;
        }

        [[nodiscard]] snf::runtime::ActorDispatchResult resume(snf::runtime::ActorState&, snf::runtime::ActorContext&, std::stop_token) override
        {
            throw std::logic_error{"RecordingPlayerBinding has no suspension point"};
        }

    private:
        struct GrantPayload
        {
            std::uint64_t experience{0};
        };

        struct Slot final : snf::runtime::ActorState
        {
        };
    };

    class TerminalPhaseWatch
    {
    public:
        void observe(const snf::server::RoomResult& result)
        {
            std::lock_guard lock{_mutex};
            if (_signalled || result.status != snf::server::RoomCommandStatus::Applied)
            {
                return;
            }
            if (result.phase != snf::server::RoomPhase::Failed && result.phase != snf::server::RoomPhase::Cleared)
            {
                return;
            }
            _signalled = true;
            _reached.set_value(result);
        }

        [[nodiscard]] std::future<snf::server::RoomResult> reached()
        {
            return _reached.get_future();
        }

    private:
        std::mutex _mutex;
        bool _signalled{false};
        std::promise<snf::server::RoomResult> _reached;
    };

    class BossSpawnWatch
    {
    public:
        void observe(const snf::server::RoomResult& result)
        {
            std::lock_guard lock{_mutex};
            if (_signalled || !result.digest)
            {
                return;
            }

            for (const snf::server::BattleEvent& event : result.digest->events)
            {
                const auto* spawned = std::get_if<snf::server::EnemySpawned>(&event);
                if (spawned && spawned->kind == snf::server::EnemyKind::Boss)
                {
                    _signalled = true;
                    _reached.set_value();
                    return;
                }
            }
        }

        [[nodiscard]] std::future<void> reached()
        {
            return _reached.get_future();
        }

    private:
        std::mutex _mutex;
        bool _signalled{false};
        std::promise<void> _reached;
    };

    [[nodiscard]] snf::runtime::ActorRuntimeConfig runtime_config()
    {
        return snf::runtime::ActorRuntimeConfig{
            .worker_count = 2,
            .queue_capacity_per_worker = 16,
            .max_in_flight_operations_per_worker = 2,
            .on_worker_start = {},
            .on_before_dispatch = {},
            .on_worker_failure = {},
        };
    }

    void test_a_negative_tick_budget_is_rejected()
    {
        bool rejected = false;
        try
        {
            static_cast<void>(snf::server::RoomActorBinding{snf::server::RoomActorBindingConfig{
                .actor = {},
                .tick_budget = -1ns,
                .on_result = {},
            }});
        }
        catch (const std::invalid_argument&)
        {
            rejected = true;
        }
        assert(rejected);
    }

    void test_room_join_tell_preserves_the_equipped_skill_id()
    {
        std::promise<snf::server::RoomResult> skill_result;
        auto skill_result_future = skill_result.get_future();
        bool skill_result_set = false;
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::RoomActorBinding binding{
            snf::server::RoomActorBindingConfig{
                .actor =
                    snf::server::RoomConfig{
                        .battle_duration = 2s,
                        .tick_interval = 100ms,
                        .wave_interval = 1s,
                        .wave_count = 1,
                        .minions_per_wave = 1,
                        .boss_spawn_after = 1500ms,
                        .max_spawned_enemies = 2,
                    },
                .on_result =
                    [&skill_result, &skill_result_set](
                        const snf::server::RoomInboundCommand& command, const snf::server::RoomResult& result
                    )
                {
                    if (!skill_result_set && std::holds_alternative<snf::server::UseSkill>(command.command))
                    {
                        skill_result_set = true;
                        skill_result.set_value(result);
                    }
                },
            },
            lifecycle
        };
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{runtime_config(), completion};
        runtime.registerBinding(binding);
        snf::server::RoomActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const snf::server::RoomId room{.value = 9};
        const snf::server::PlayerId player{.value = 90};
        assert(runtime.tryTell(
                   snf::runtime::ActorKey{.kind = snf::runtime::ActorKind::Room, .entity = room.value},
                   snf::runtime::TellPayload::of(snf::server::RoomJoinTell{
                       .player = player,
                       .request =
                           snf::server::RoomJoinRequest{
                               .room = room,
                               .stats = {.attack = 10, .health = 100},
                               .equipped_skill_id = snf::server::ARCANE_BOLT_SKILL_ID,
                           },
                       .reply =
                           snf::server::RoomReplyContext{
                               .connection = {.descriptor = 9, .generation = 1},
                               .request_id = 1,
                               .kind = snf::server::RoomReplyKind::Joined,
                           },
                   })
               ) == snf::runtime::PostResult::Accepted);
        assert(ingress.tryPost(snf::server::RoomInboundCommand{
                   .room = room,
                   .command = snf::server::StartBattle{},
                   .reply = std::nullopt,
               }) == snf::runtime::PostResult::Accepted);
        assert(ingress.tryPost(snf::server::RoomInboundCommand{
                   .room = room,
                   .command =
                       snf::server::UseSkill{
                           .player = player,
                           .skill_id = snf::server::ARCANE_BOLT_SKILL_ID,
                           .request_sequence = 1,
                       },
                   .reply = std::nullopt,
               }) == snf::runtime::PostResult::Accepted);

        assert(skill_result_future.wait_for(5s) == std::future_status::ready);
        const auto result = skill_result_future.get();
        assert(result.status == snf::server::RoomCommandStatus::Applied);

        runtime.close();
        runtime.join();
        assert(completion.failed.load() == 0);
    }

    void test_killing_the_boss_rewards_every_participant()
    {
        RecordingPlayerBinding player_binding;
        player_binding.expected = 2;
        auto arrived = player_binding.all_arrived.get_future();
        BossSpawnWatch boss_spawn;
        auto boss_reached = boss_spawn.reached();

        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::RoomActorBinding binding{
            snf::server::RoomActorBindingConfig{
                .actor =
                    snf::server::RoomConfig{
                        .battle_duration = 5s,
                        .max_participants = 4,
                        .clear_experience = 300,
                        .boss_health = 50,
                        .tick_interval = 5ms,
                        .wave_interval = 1s,
                        .wave_count = 0,
                        .minions_per_wave = 0,
                        .boss_spawn_after = 10ms,
                        .max_spawned_enemies = 1,
                        .arena_width = 20,
                        .arena_height = 20,
                        .participant_spawn_spacing = 2,
                        .minion_spawn_radius = 5,
                    },
                .on_result =
                    [&boss_spawn](const snf::server::RoomInboundCommand&, const snf::server::RoomResult& result)
                {
                    boss_spawn.observe(result);
                },
            },
            lifecycle
        };
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{runtime_config(), completion};
        runtime.registerBinding(binding);
        runtime.registerBinding(player_binding);
        snf::server::RoomActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const snf::server::RoomId room{.value = 10};
        for (const std::uint64_t player : {std::uint64_t{20}, std::uint64_t{10}})
        {
            assert(
                ingress.tryPost(snf::server::RoomInboundCommand{
                    .room = room,
                    .command =
                        snf::server::JoinRoom{
                            .player = snf::server::PlayerId{.value = player},
                            .stats = {.attack = 50, .health = 100},
                        },
                    .reply = std::nullopt,
                }) == snf::runtime::PostResult::Accepted
            );
        }
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command = snf::server::StartBattle{},
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );

        assert(boss_reached.wait_for(5s) == std::future_status::ready);
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command =
                    snf::server::UseSkill{
                        .player = snf::server::PlayerId{.value = 10},
                        .skill_id = snf::server::SLASH,
                        .request_sequence = 1,
                    },
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );

        assert(arrived.wait_for(5s) == std::future_status::ready);

        runtime.close();
        runtime.join();

        std::lock_guard lock{player_binding.mutex};
        std::ranges::sort(player_binding.grants, {}, &RecordingPlayerBinding::Grant::entity);
        assert(
            (player_binding.grants ==
             std::vector<RecordingPlayerBinding::Grant>{
                 {.entity = 10, .experience = 300},
                 {.entity = 20, .experience = 300},
             })
        );
        assert(completion.drained.load() == 1);
        assert(completion.failed.load() == 0);
    }

    void test_a_room_fails_from_its_own_timer_when_the_boss_survives()
    {
        RecordingPlayerBinding player_binding;
        TerminalPhaseWatch watch;
        auto reached = watch.reached();

        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::RoomActorBinding binding{
            snf::server::RoomActorBindingConfig{
                .actor =
                    snf::server::RoomConfig{
                        .battle_duration = 30ms,
                        .max_participants = 4,
                        .clear_experience = 300,
                        .boss_health = 50,
                        .tick_interval = 5ms,
                        .wave_interval = 1s,
                        .wave_count = 0,
                        .minions_per_wave = 0,
                        .boss_spawn_after = 10ms,
                        .max_spawned_enemies = 1,
                    },
                .tick_budget = 0ns,
                .on_result =
                    [&watch](const snf::server::RoomInboundCommand&, const snf::server::RoomResult& result)
                {
                    watch.observe(result);
                },
            },
            lifecycle
        };
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{runtime_config(), completion};
        runtime.registerBinding(binding);
        runtime.registerBinding(player_binding);
        snf::server::RoomActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const snf::server::RoomId room{.value = 10};
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command =
                    snf::server::JoinRoom{
                        .player = snf::server::PlayerId{.value = 10},
                        .stats = {.attack = 50, .health = 100},
                    },
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command = snf::server::StartBattle{},
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );

        assert(reached.wait_for(5s) == std::future_status::ready);
        const snf::server::RoomResult result = reached.get();
        assert(result.phase == snf::server::RoomPhase::Failed);
        assert(result.grants.empty());
        assert((result.audience == std::vector<snf::server::PlayerId>{snf::server::PlayerId{.value = 10}}));

        runtime.close();
        runtime.join();

        std::lock_guard lock{player_binding.mutex};
        assert(player_binding.grants.empty());
        assert(completion.failed.load() == 0);

        const auto stats = binding.stats();
        assert(stats.tick_execution_nanoseconds.sample_count > 0);
        assert(stats.tick_publish_nanoseconds.sample_count == stats.tick_execution_nanoseconds.sample_count);
        assert(stats.tick_turn_nanoseconds.sample_count == stats.tick_execution_nanoseconds.sample_count);
        assert(stats.tick_overruns == stats.tick_turn_nanoseconds.sample_count);
        assert(stats.tick_schedule_rejections == 0);
    }

    void test_deadline_schedule_rejection_refuses_start_and_keeps_the_runtime_alive()
    {
        RecordingPlayerBinding player_binding;
        std::promise<void> blocker_joined;
        auto blocker_joined_future = blocker_joined.get_future();
        std::promise<void> blocker_started;
        auto blocker_started_future = blocker_started.get_future();
        std::promise<void> blocker_failed;
        auto blocker_failed_future = blocker_failed.get_future();
        std::promise<void> target_joined;
        auto target_joined_future = target_joined.get_future();
        std::promise<snf::server::RoomResult> rejected;
        auto rejected_future = rejected.get_future();
        std::promise<snf::server::RoomResult> retried;
        auto retried_future = retried.get_future();

        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::RoomActorBinding binding{
            snf::server::RoomActorBindingConfig{
                .actor =
                    snf::server::RoomConfig{
                        .battle_duration = 200ms,
                        .tick_interval = 5ms,
                        .wave_interval = 1s,
                        .wave_count = 0,
                        .minions_per_wave = 0,
                        .boss_spawn_after = 10ms,
                        .max_spawned_enemies = 1,
                    },
                .on_result =
                    [&blocker_joined, &blocker_started, &blocker_failed, &target_joined, &rejected, &retried](
                        const snf::server::RoomInboundCommand& command, const snf::server::RoomResult& result
                    )
                {
                    if (command.room.value == 11 && std::holds_alternative<snf::server::JoinRoom>(command.command))
                    {
                        blocker_joined.set_value();
                    }
                    else if (command.room.value == 11 && std::holds_alternative<snf::server::StartBattle>(command.command))
                    {
                        blocker_started.set_value();
                    }
                    else if (command.room.value == 11 && std::holds_alternative<snf::server::BattleDeadline>(command.command))
                    {
                        blocker_failed.set_value();
                    }
                    else if (command.room.value == 12 && std::holds_alternative<snf::server::JoinRoom>(command.command))
                    {
                        target_joined.set_value();
                    }
                    else if (command.room.value == 12 && std::holds_alternative<snf::server::StartBattle>(command.command))
                    {
                        if (result.status == snf::server::RoomCommandStatus::RuntimeOverloaded)
                        {
                            rejected.set_value(result);
                        }
                        else
                        {
                            retried.set_value(result);
                        }
                    }
                },
            },
            lifecycle
        };
        RecordingCompletion completion;
        auto config = runtime_config();
        config.worker_count = 1;
        config.queue_capacity_per_worker = 2;
        snf::runtime::ActorRuntime runtime{config, completion};
        runtime.registerBinding(binding);
        runtime.registerBinding(player_binding);
        snf::server::RoomActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const auto post_until_accepted = [&ingress](auto make_command)
        {
            for (int attempt = 0; attempt < 500; ++attempt)
            {
                if (ingress.tryPost(make_command()) == snf::runtime::PostResult::Accepted)
                {
                    return true;
                }
                std::this_thread::sleep_for(1ms);
            }
            return false;
        };

        const snf::server::RoomId blocker{.value = 11};
        const snf::server::RoomId target{.value = 12};
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = blocker,
                .command =
                    snf::server::JoinRoom{
                        .player = snf::server::PlayerId{.value = 10},
                        .stats = {.attack = 50, .health = 100},
                    },
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );
        assert(blocker_joined_future.wait_for(5s) == std::future_status::ready);
        assert(post_until_accepted([blocker]
                                   {
                                       return snf::server::RoomInboundCommand{
                                           .room = blocker,
                                           .command = snf::server::StartBattle{},
                                           .reply = std::nullopt,
                                       };
                                   }));
        assert(blocker_started_future.wait_for(5s) == std::future_status::ready);

        assert(post_until_accepted([target]
                                   {
                                       return snf::server::RoomInboundCommand{
                                           .room = target,
                                           .command =
                                               snf::server::JoinRoom{
                                                   .player = snf::server::PlayerId{.value = 20},
                                                   .stats = {.attack = 50, .health = 100},
                                               },
                                           .reply = std::nullopt,
                                       };
                                   }));
        assert(target_joined_future.wait_for(5s) == std::future_status::ready);
        assert(post_until_accepted([target]
                                   {
                                       return snf::server::RoomInboundCommand{
                                           .room = target,
                                           .command = snf::server::StartBattle{},
                                           .reply = std::nullopt,
                                       };
                                   }));

        assert(rejected_future.wait_for(5s) == std::future_status::ready);
        const auto result = rejected_future.get();
        assert(result.status == snf::server::RoomCommandStatus::RuntimeOverloaded);
        assert(result.phase == snf::server::RoomPhase::Waiting);
        assert(!result.deadline_after && !result.tick_after && !result.outcome && !result.digest);

        assert(blocker_failed_future.wait_for(5s) == std::future_status::ready);
        assert(post_until_accepted([target]
                                   {
                                       return snf::server::RoomInboundCommand{
                                           .room = target,
                                           .command = snf::server::StartBattle{},
                                           .reply = std::nullopt,
                                       };
                                   }));
        assert(retried_future.wait_for(5s) == std::future_status::ready);
        const auto started = retried_future.get();
        assert(started.status == snf::server::RoomCommandStatus::Applied);
        assert(started.phase == snf::server::RoomPhase::Running);
        assert(started.deadline_after == 200ms);

        runtime.close();
        runtime.join();
        assert(completion.failed.load() == 0);
        assert(binding.stats().deadline_schedule_rejections == 1);
        assert(runtime.getStats().workers.front().timers_rejected_full >= 1);
    }

    void test_tick_schedule_rejection_keeps_deadline_backstop()
    {
        RecordingPlayerBinding player_binding;
        TerminalPhaseWatch terminal;
        auto terminal_future = terminal.reached();
        std::promise<void> joined;
        auto joined_future = joined.get_future();
        bool join_signalled = false;

        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::RoomActorBinding binding{
            snf::server::RoomActorBindingConfig{
                .actor =
                    snf::server::RoomConfig{
                        .battle_duration = 30ms,
                        .tick_interval = 5ms,
                        .wave_interval = 1s,
                        .wave_count = 0,
                        .minions_per_wave = 0,
                        .boss_spawn_after = 10ms,
                        .max_spawned_enemies = 1,
                    },
                .on_result =
                    [&terminal, &joined, &join_signalled](const snf::server::RoomInboundCommand&, const snf::server::RoomResult& result)
                {
                    terminal.observe(result);
                    if (!join_signalled && result.phase == snf::server::RoomPhase::Waiting && result.player)
                    {
                        join_signalled = true;
                        joined.set_value();
                    }
                },
            },
            lifecycle
        };
        RecordingCompletion completion;
        auto config = runtime_config();
        config.queue_capacity_per_worker = 2;
        snf::runtime::ActorRuntime runtime{config, completion};
        runtime.registerBinding(binding);
        runtime.registerBinding(player_binding);
        snf::server::RoomActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const snf::server::RoomId room{.value = 13};
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command =
                    snf::server::JoinRoom{
                        .player = snf::server::PlayerId{.value = 10},
                        .stats = {.attack = 50, .health = 100},
                    },
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );
        assert(joined_future.wait_for(5s) == std::future_status::ready);
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command = snf::server::StartBattle{},
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );

        assert(terminal_future.wait_for(5s) == std::future_status::ready);
        assert(terminal_future.get().phase == snf::server::RoomPhase::Failed);
        runtime.close();
        runtime.join();

        const auto stats = binding.stats();
        assert(stats.tick_schedule_rejections == 1);
        assert(stats.tick_execution_nanoseconds.sample_count == 0);
        assert(completion.failed.load() == 0);
    }

    void test_a_room_that_never_started_passivates()
    {
        RecordingPlayerBinding player_binding;
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::RoomActorBinding binding{snf::server::RoomActorBindingConfig{}, lifecycle};
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{runtime_config(), completion};
        runtime.registerBinding(binding);
        runtime.registerBinding(player_binding);
        snf::server::RoomActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = snf::server::RoomId{.value = 11},
                .command = snf::server::StartBattle{},
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );

        runtime.close();
        runtime.join();

        const auto stats = runtime.getStats();
        std::uint64_t evicted = 0;
        for (const auto& worker : stats.workers)
        {
            evicted += worker.evicted_actors;
        }
        assert(evicted >= 1);
        assert(completion.failed.load() == 0);
    }

    void test_a_rejected_reward_tell_is_counted()
    {
        RecordingPlayerBinding player_binding;
        BossSpawnWatch boss_spawn;
        auto boss_reached = boss_spawn.reached();
        snf::runtime::ActorRuntime* runtime_pointer = nullptr;
        snf::server::RoomActorBinding* binding_pointer = nullptr;
        std::atomic<bool> fill_armed{true};

        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::RoomActorBinding binding{
            snf::server::RoomActorBindingConfig{
                .actor =
                    snf::server::RoomConfig{
                        .battle_duration = 5s,
                        .clear_experience = 300,
                        .boss_health = 50,
                        .tick_interval = 5ms,
                        .wave_interval = 1s,
                        .wave_count = 0,
                        .minions_per_wave = 0,
                        .boss_spawn_after = 10ms,
                        .max_spawned_enemies = 1,
                        .arena_width = 20,
                        .arena_height = 20,
                        .participant_spawn_spacing = 2,
                        .minion_spawn_radius = 5,
                    },
                .on_result =
                    [&boss_spawn, &runtime_pointer, &binding_pointer, &fill_armed](
                        const snf::server::RoomInboundCommand&, const snf::server::RoomResult& result
                    )
                {
                    boss_spawn.observe(result);
                    if (result.grants.empty() || !fill_armed.exchange(false) || runtime_pointer == nullptr || binding_pointer == nullptr)
                    {
                        return;
                    }

                    for (std::uint64_t attempt = 0; attempt < 64; ++attempt)
                    {
                        const auto posted = runtime_pointer->tryPost(binding_pointer->makeCommand(snf::server::RoomInboundCommand{
                            .room = snf::server::RoomId{.value = 1000 + attempt},
                            .command = snf::server::StartBattle{},
                            .reply = std::nullopt,
                        }));
                        if (posted != snf::runtime::PostResult::Accepted)
                        {
                            break;
                        }
                    }
                },
            },
            lifecycle
        };
        binding_pointer = &binding;
        RecordingCompletion completion;
        auto config = runtime_config();
        config.worker_count = 1;
        config.queue_capacity_per_worker = 8;
        snf::runtime::ActorRuntime runtime{config, completion};
        runtime_pointer = &runtime;
        runtime.registerBinding(binding);
        runtime.registerBinding(player_binding);
        snf::server::RoomActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const snf::server::RoomId room{.value = 14};
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command =
                    snf::server::JoinRoom{
                        .player = snf::server::PlayerId{.value = 10},
                        .stats = {.attack = 50, .health = 100},
                    },
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command = snf::server::StartBattle{},
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );
        assert(boss_reached.wait_for(5s) == std::future_status::ready);
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command =
                    snf::server::UseSkill{
                        .player = snf::server::PlayerId{.value = 10},
                        .skill_id = snf::server::SLASH,
                        .request_sequence = 1,
                    },
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );

        bool counted = false;
        for (int attempt = 0; attempt < 500 && !counted; ++attempt)
        {
            counted = binding.stats().grant_tell_rejections == 1;
            if (!counted)
            {
                std::this_thread::sleep_for(10ms);
            }
        }
        assert(counted);
        runtime.close();
        runtime.join();

        std::lock_guard lock{player_binding.mutex};
        assert(player_binding.grants.empty());
        assert(completion.failed.load() == 0);
    }
}

void test_a_join_naming_another_room_is_refused()
{
    RecordingPlayerBinding player_binding;
    snf::server::CountingCommandLifecycleSink lifecycle;
    snf::server::RoomActorBinding binding{snf::server::RoomActorBindingConfig{}, lifecycle};
    RecordingCompletion completion;
    snf::runtime::ActorRuntime runtime{runtime_config(), completion};
    runtime.registerBinding(binding);
    runtime.registerBinding(player_binding);
    runtime.start();

    bool refused = false;
    try
    {
        static_cast<void>(runtime.tryTell(
            snf::runtime::ActorKey{
                .kind = snf::runtime::ActorKind::Room,
                .entity = 11,
            },
            snf::runtime::TellPayload::of(snf::server::RoomJoinTell{
                .player = snf::server::PlayerId{.value = 5},
                .request =
                    snf::server::RoomJoinRequest{
                        .room = snf::server::RoomId{.value = 99},
                        .stats = {.attack = 10, .health = 100},
                    },
                .reply =
                    snf::server::RoomReplyContext{
                        .connection = {.descriptor = 4, .generation = 1},
                        .request_id = 3,
                        .kind = snf::server::RoomReplyKind::Joined,
                    },
            })
        ));
    }
    catch (const std::logic_error&)
    {
        refused = true;
    }

    runtime.close();
    runtime.join();
    assert(refused);
}
void run_room_actor_binding_tests()
{
    test_a_negative_tick_budget_is_rejected();
    test_room_join_tell_preserves_the_equipped_skill_id();
    test_a_join_naming_another_room_is_refused();
    test_killing_the_boss_rewards_every_participant();
    test_a_room_fails_from_its_own_timer_when_the_boss_survives();
    test_deadline_schedule_rejection_refuses_start_and_keeps_the_runtime_alive();
    test_tick_schedule_rejection_keeps_deadline_backstop();
    test_a_room_that_never_started_passivates();
    test_a_rejected_reward_tell_is_counted();
}
