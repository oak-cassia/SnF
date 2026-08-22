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
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <variant>
#include <vector>

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

    // Stands in for PlayerActorBinding so this test covers the Room's half of the
    // tell: that the payload the Room emits is one the target binding can restore.
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
        // Called on whichever Worker owns the sender, so this stays a read-only
        // transform: no cache, no counter, nothing for TSan to find.
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

    // A failure emits no tell, so there is nothing to wait on the way a clear's
    // rewards can be waited on. This watches the result stream instead.
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
                        // Far longer than this test takes, so the deadline cannot be
                        // what ends the battle. One cast is enough to kill the boss.
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

        // The boss does not exist until the one-shot Tick chain reaches its
        // absolute spawn time. Wait for that digest before casting.
        assert(boss_reached.wait_for(5s) == std::future_status::ready);
        assert(
            ingress.tryPost(snf::server::RoomInboundCommand{
                .room = room,
                .command =
                    snf::server::UseSkill{
                        .player = snf::server::PlayerId{.value = 10},
                        .skill = snf::server::SLASH,
                        .request_sequence = 1,
                    },
                .reply = std::nullopt,
            }) == snf::runtime::PostResult::Accepted
        );

        // One player landed the killing blow; both are rewarded, and the reward
        // reaches the Player through a tell the target binding restores.
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

        // Nobody casts anything. The only thing that can decide this battle is the
        // deadline the Room armed for itself, and it decides against the party.
        assert(reached.wait_for(5s) == std::future_status::ready);
        const snf::server::RoomResult result = reached.get();
        assert(result.phase == snf::server::RoomPhase::Failed);
        assert(result.grants.empty());
        // Still names everyone, because the return to a Zone reads this list.
        assert((result.audience == std::vector<snf::server::PlayerId>{snf::server::PlayerId{.value = 10}}));

        runtime.close();
        runtime.join();

        std::lock_guard lock{player_binding.mutex};
        // A failure pays nothing, so no tell was ever sent.
        assert(player_binding.grants.empty());
        assert(completion.failed.load() == 0);

        const auto stats = binding.stats();
        assert(stats.tick_execution_nanoseconds.sample_count > 0);
        assert(stats.tick_publish_nanoseconds.sample_count == stats.tick_execution_nanoseconds.sample_count);
        assert(stats.tick_turn_nanoseconds.sample_count == stats.tick_execution_nanoseconds.sample_count);
        assert(stats.tick_overruns == stats.tick_turn_nanoseconds.sample_count);
        assert(stats.tick_schedule_rejections == 0);
    }

    void test_deadline_schedule_rejection_fails_the_runtime()
    {
        RecordingPlayerBinding player_binding;
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
                    [&joined, &join_signalled](const snf::server::RoomInboundCommand&, const snf::server::RoomResult& result)
                {
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
        auto failed = completion.failureReported();
        auto config = runtime_config();
        config.queue_capacity_per_worker = 1;
        snf::runtime::ActorRuntime runtime{config, completion};
        runtime.registerBinding(binding);
        runtime.registerBinding(player_binding);
        snf::server::RoomActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const snf::server::RoomId room{.value = 12};
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

        // The active command owns the only capacity slot, so the mandatory
        // deadline cannot be registered and the Worker fails loudly.
        assert(failed.wait_for(5s) == std::future_status::ready);
        runtime.close();
        bool join_failed = false;
        try
        {
            runtime.join();
        }
        catch (const std::runtime_error&)
        {
            join_failed = true;
        }
        assert(join_failed);
        assert(completion.failed.load() == 1);
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

        // Start owns one slot and its deadline reserves the second. Tick
        // registration is rejected, but the deadline still terminates the Room.
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

        // An empty Room is refused a start, so it must not be left resident holding
        // a slot for a battle that will never run.
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

                    // on_result runs immediately before the Room publishes grants.
                    // Fill this one Worker's outstanding budget here so the following
                    // tell is deterministically refused without giving the Worker a
                    // chance to drain the filler commands first.
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
                        .skill = snf::server::SLASH,
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
        // The key says Room 11, the request says Room 99. Only a routing bug does
        // that, and entering the wrong Room is worse than failing loudly.
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
    test_a_join_naming_another_room_is_refused();
    test_killing_the_boss_rewards_every_participant();
    test_a_room_fails_from_its_own_timer_when_the_boss_survives();
    test_deadline_schedule_rejection_fails_the_runtime();
    test_tick_schedule_rejection_keeps_deadline_backstop();
    test_a_room_that_never_started_passivates();
    test_a_rejected_reward_tell_is_counted();
}
