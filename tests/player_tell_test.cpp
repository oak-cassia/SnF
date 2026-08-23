#include "outbound_reservation_test_support.hpp"
#include "snf/game/street_experience_grant.hpp"
#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/runtime/tell_payload.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_persistence_service.hpp"
#include "snf/server/player_repository.hpp"
#include "snf/server/protocol_player_response_sink.hpp"
#include "snf/server/room_actor_binding.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>

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
            ++failed;
        }

        std::atomic<int> drained{0};
        std::atomic<int> failed{0};
    };

    class GatedRepository final : public snf::server::PlayerRepository
    {
    public:
        void asyncLoad(const snf::server::PlayerId player, snf::server::PlayerLoadCompletion completion) override
        {
            snf::server::PlayerLoadResult result;
            {
                std::lock_guard lock{_mutex};
                result.status = _load_unavailable ? snf::server::PlayerRepositoryStatus::Unavailable : snf::server::PlayerRepositoryStatus::Success;
                if (const auto record = _records.find(player); record != _records.end())
                {
                    result.record = record->second;
                }
            }
            completion(std::move(result));
        }

        void asyncSave(snf::server::PlayerRecord record, snf::server::PlayerSaveCompletion completion) override
        {
            {
                std::unique_lock lock{_mutex};
                ++_save_calls;
                if (_block_next_save)
                {
                    _block_next_save = false;
                    _save_blocked = true;
                    _wake.notify_all();
                    _wake.wait(
                        lock,
                        [this]
                        {
                            return _release_blocked_save;
                        }
                    );
                    _release_blocked_save = false;
                    _save_blocked = false;
                }
                _records.insert_or_assign(record.player, record);
                _wake.notify_all();
            }
            completion(snf::server::PlayerSaveResult{.status = snf::server::PlayerRepositoryStatus::Success});
        }

        void seed(snf::server::PlayerRecord record)
        {
            std::lock_guard lock{_mutex};
            _records.insert_or_assign(record.player, std::move(record));
        }

        void failLoads(const bool unavailable)
        {
            std::lock_guard lock{_mutex};
            _load_unavailable = unavailable;
        }

        void blockNextSave()
        {
            std::lock_guard lock{_mutex};
            _block_next_save = true;
        }

        void waitForBlockedSave()
        {
            std::unique_lock lock{_mutex};
            assert(_wake.wait_for(
                lock,
                5s,
                [this]
                {
                    return _save_blocked;
                }
            ));
        }

        void releaseBlockedSave()
        {
            std::lock_guard lock{_mutex};
            _release_blocked_save = true;
            _wake.notify_all();
        }

        [[nodiscard]] std::optional<snf::server::PlayerRecord> find(const snf::server::PlayerId player) const
        {
            std::lock_guard lock{_mutex};
            const auto record = _records.find(player);
            return record == _records.end() ? std::nullopt : std::optional{record->second};
        }

    private:
        mutable std::mutex _mutex;
        std::condition_variable _wake;
        std::unordered_map<snf::server::PlayerId, snf::server::PlayerRecord, snf::server::PlayerIdHash> _records;
        std::size_t _save_calls{0};
        bool _load_unavailable{false};
        bool _block_next_save{false};
        bool _save_blocked{false};
        bool _release_blocked_save{false};
    };

    struct RetryHarness
    {
        RetryHarness(const std::chrono::milliseconds retry_delay = 20ms, const int retry_limit = 5, const std::size_t runtime_capacity = 8)
            : persistence(
                  repository,
                  snf::server::PlayerPersistenceServiceConfig{
                      .queue_capacity = 1,
                      .flush_interval = 1s,
                  }
              )
            , outbound_event(snf::test::make_wake_descriptor())
            , outbound(
                  snf::server::OutboundChannelConfig{
                      .capacity = 8,
                      .max_slots_per_connection = 8,
                  },
                  outbound_event.getDescriptor()
              )
            , response_sink(outbound)
            , binding(
                  response_sink,
                  outbound,
                  lifecycle,
                  snf::server::PlayerActorBindingConfig{
                      .actor_kind = snf::runtime::ActorKind::Player,
                      .repository = &repository,
                      .persistence_service = &persistence,
                      .snapshot_retry_delay = retry_delay,
                      .snapshot_retry_limit = retry_limit,
                  }
              )
            , runtime(
                  snf::runtime::ActorRuntimeConfig{
                      .worker_count = 1,
                      .queue_capacity_per_worker = runtime_capacity,
                      .max_in_flight_operations_per_worker = 4,
                      .on_worker_start = {},
                      .on_before_dispatch = {},
                      .on_worker_failure = {},
                  },
                  completion
              )
        {
            runtime.registerBinding(binding);
        }

        GatedRepository repository;
        snf::server::PlayerPersistenceService persistence;
        snf::net::UniqueFileDescriptor outbound_event;
        snf::server::OutboundChannel outbound;
        snf::server::ProtocolPlayerResponseSink response_sink;
        snf::server::CountingCommandLifecycleSink lifecycle;
        RecordingCompletion completion;
        snf::server::PlayerActorBinding binding;
        snf::runtime::ActorRuntime runtime;
    };

    template <typename Predicate> bool wait_until(Predicate&& predicate, const std::chrono::milliseconds timeout = 5s)
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (predicate())
            {
                return true;
            }
            std::this_thread::sleep_for(1ms);
        }
        return predicate();
    }

    [[nodiscard]] std::size_t actor_count(const snf::runtime::ActorRuntime& runtime)
    {
        std::size_t count = 0;
        for (const auto& worker : runtime.getStats().workers)
        {
            count += worker.actor_count;
        }
        return count;
    }

    [[nodiscard]] snf::runtime::PostResult
    tell_grant(snf::runtime::ActorRuntime& runtime, const snf::server::PlayerId player, const std::uint64_t experience)
    {
        return runtime.tryTell(
            snf::runtime::ActorKey{
                .kind = snf::runtime::ActorKind::Player,
                .entity = player.value,
            },
            snf::runtime::TellPayload::of(snf::server::StreetExperienceGrant{.player = player, .experience = experience})
        );
    }

    void fill_persistence_queue(RetryHarness& harness)
    {
        harness.repository.blockNextSave();
        assert(harness.persistence.tryEnqueue(snf::server::PlayerRecord{.player = snf::server::PlayerId{.value = 900}}));
        harness.repository.waitForBlockedSave();
        assert(harness.persistence.tryEnqueue(snf::server::PlayerRecord{.player = snf::server::PlayerId{.value = 901}}));
        assert(harness.persistence.stats().queue_depth == 1);
    }

    struct Harness
    {
        Harness()
            : outbound_event(snf::test::make_wake_descriptor())
            , outbound(
                  snf::server::OutboundChannelConfig{
                      .capacity = 8,
                      .max_slots_per_connection = 8,
                  },
                  outbound_event.getDescriptor())
            , response_sink(outbound)
            , binding(response_sink,
                      outbound,
                      lifecycle,
                      snf::server::PlayerActorBindingConfig{
                          .actor_kind = snf::runtime::ActorKind::Player,
                          .repository = &repository,
                      })
            , runtime(
                  snf::runtime::ActorRuntimeConfig{
                      .worker_count = 1,
                      .queue_capacity_per_worker = 8,
                      .max_in_flight_operations_per_worker = 4,
                      .on_worker_start = {},
                      .on_before_dispatch = {},
                      .on_worker_failure = {},
                  },
                  completion)
        {
            runtime.registerBinding(binding);
        }

        snf::net::UniqueFileDescriptor outbound_event;
        snf::server::OutboundChannel outbound;
        snf::server::ProtocolPlayerResponseSink response_sink;
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::InMemoryPlayerRepository repository;
        RecordingCompletion completion;
        snf::server::PlayerActorBinding binding;
        snf::runtime::ActorRuntime runtime;
    };

    [[nodiscard]] std::optional<snf::server::PlayerRecord>
    wait_for_experience(const snf::server::InMemoryPlayerRepository& repository, const snf::server::PlayerId player, const std::uint64_t experience)
    {
        for (int attempt = 0; attempt < 500; ++attempt)
        {
            const auto record = repository.find(player);
            if (record && record->street_experience == experience)
            {
                return record;
            }
            std::this_thread::sleep_for(10ms);
        }
        return std::nullopt;
    }

    void test_a_grant_to_an_offline_player_loads_the_record_before_applying()
    {
        const snf::server::PlayerId player{.value = 42};
        Harness harness;
        harness.repository.asyncSave(
            snf::server::PlayerRecord{
                .player = player,
                .handled_command_count = 9,
                .last_location = std::nullopt,
                .currency_balance = 700,
                .purchased_item_count = 3,
                .street_experience = 1000,
            },
            [](snf::server::PlayerSaveResult) noexcept {});
        harness.runtime.start();

        assert(harness.runtime.tryTell(
                   snf::runtime::ActorKey{
                       .kind = snf::runtime::ActorKind::Player,
                       .entity = player.value,
                   },
                   snf::runtime::TellPayload::of(snf::server::StreetExperienceGrant{.player = player, .experience = 300})) == snf::runtime::PostResult::Accepted);

        const auto record = wait_for_experience(harness.repository, player, 1300);
        harness.runtime.close();
        harness.runtime.join();

        assert(record);
        assert(record->currency_balance == 700);
        assert(record->purchased_item_count == 3);
        assert(record->handled_command_count == 9);
        assert(harness.completion.failed.load() == 0);
    }

    void test_grants_accumulate_across_tells()
    {
        const snf::server::PlayerId player{.value = 43};
        Harness harness;
        harness.repository.asyncSave(
            snf::server::PlayerRecord{
                .player = player,
                .currency_balance = 500,
                .street_experience = 0,
            },
            [](snf::server::PlayerSaveResult) noexcept {});
        harness.runtime.start();

        for (int i = 0; i < 3; ++i)
        {
            assert(harness.runtime.tryTell(
                       snf::runtime::ActorKey{
                           .kind = snf::runtime::ActorKind::Player,
                           .entity = player.value,
                       },
                       snf::runtime::TellPayload::of(snf::server::StreetExperienceGrant{.player = player, .experience = 300})) == snf::runtime::PostResult::Accepted);
        }

        const auto record = wait_for_experience(harness.repository, player, 900);
        harness.runtime.close();
        harness.runtime.join();

        assert(record);
        assert(record->currency_balance == 500);
        assert(harness.completion.failed.load() == 0);
    }

    void test_a_grant_naming_another_player_is_refused()
    {
        Harness harness;
        harness.runtime.start();

        bool refused = false;
        try
        {
            static_cast<void>(harness.runtime.tryTell(
                snf::runtime::ActorKey{
                    .kind = snf::runtime::ActorKind::Player,
                    .entity = 44,
                },
                snf::runtime::TellPayload::of(snf::server::StreetExperienceGrant{
                    .player = snf::server::PlayerId{.value = 45},
                    .experience = 300,
                })));
        }
        catch (const std::logic_error&)
        {
            refused = true;
        }

        harness.runtime.close();
        harness.runtime.join();
        assert(refused);
    }

    void test_a_refused_room_join_reports_the_entry_it_belonged_to()
    {
        const snf::server::PlayerId player{.value = 46};
        const snf::server::RoomId room{.value = 90};
        const snf::net::ConnectionId connection{.descriptor = 7, .generation = 3};
        const snf::server::RoomEntryContext entry{
            .entry_id = snf::server::RoomEntryId{.value = 11},
            .return_id = {},
            .ticket = snf::server::RoomTransitionTicket{.value = 22},
            .connection = connection,
            .player = player,
            .step = snf::server::RoomEntryStep::JoinRoom,
        };

        std::mutex reported_mutex;
        std::optional<std::pair<snf::server::RoomEntryContext, snf::server::RoomId>> reported;

        snf::net::UniqueFileDescriptor outbound_event{snf::test::make_wake_descriptor()};
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{
                .capacity = 8,
                .max_slots_per_connection = 8,
            },
            outbound_event.getDescriptor()};
        snf::server::ProtocolPlayerResponseSink response_sink{outbound};
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::InMemoryPlayerRepository repository;
        RecordingCompletion completion;

        snf::server::PlayerActorBinding binding{
            response_sink,
            outbound,
            lifecycle,
            snf::server::PlayerActorBindingConfig{
                .actor_kind = snf::runtime::ActorKind::Player,
                .repository = &repository,
                .on_room_join_undelivered =
                    [&reported_mutex, &reported](const snf::server::RoomEntryContext& context, const snf::server::RoomId undelivered_room)
                {
                    const std::lock_guard guard{reported_mutex};
                    reported.emplace(context, undelivered_room);
                },
            }};
        snf::server::RoomActorBinding room_binding{snf::server::RoomActorBindingConfig{}};

        std::atomic<bool> fill_armed{false};
        snf::runtime::ActorRuntime* runtime_pointer = nullptr;
        snf::runtime::ActorRuntime runtime{
            snf::runtime::ActorRuntimeConfig{
                .worker_count = 1,
                .queue_capacity_per_worker = 2,
                .max_in_flight_operations_per_worker = 4,
                .on_worker_start = {},
                .on_before_dispatch =
                    [&fill_armed, &runtime_pointer, &room_binding, room](std::size_t, const snf::runtime::ActorKey& key, const snf::runtime::ActorSubmission&)
                {
                    if (!fill_armed.load() || key.kind != snf::runtime::ActorKind::Player || runtime_pointer == nullptr)
                    {
                        return;
                    }
                    fill_armed.store(false);
                    for (int attempt = 0; attempt < 64; ++attempt)
                    {
                        const auto posted = runtime_pointer->tryPost(room_binding.makeCommand(snf::server::RoomInboundCommand{
                            .room = room,
                            .command = snf::server::StartBattle{},
                            .reply = std::nullopt,
                        }));
                        if (posted != snf::runtime::PostResult::Accepted)
                        {
                            return;
                        }
                    }
                },
                .on_worker_failure = {},
            },
            completion};
        runtime_pointer = &runtime;
        runtime.registerBinding(binding);
        runtime.registerBinding(room_binding);
        runtime.start();

        assert(runtime.tryPost(binding.makeCommand(snf::server::PlayerInboundCommand{
                   .actor = snf::server::PlayerActorId{player},
                   .connection = connection,
                   .command = snf::server::PingCommand{},
                   .request_id = 1,
               })) == snf::runtime::PostResult::Accepted);
        bool ponged = false;
        for (int attempt = 0; attempt < 500 && !ponged; ++attempt)
        {
            ponged = outbound.tryPop().has_value();
            if (!ponged)
            {
                std::this_thread::sleep_for(10ms);
            }
        }
        assert(ponged);

        fill_armed.store(true);
        assert(runtime.tryPost(binding.makeCommand(snf::server::PlayerInboundCommand{
                   .actor = snf::server::PlayerActorId{player},
                   .connection = connection,
                   .command = snf::server::JoinRoomRequest{.room = room},
                   .request_id = 2,
                   .room_entry = entry,
               })) == snf::runtime::PostResult::Accepted);

        std::optional<std::pair<snf::server::RoomEntryContext, snf::server::RoomId>> observed;
        for (int attempt = 0; attempt < 500 && !observed; ++attempt)
        {
            {
                const std::lock_guard guard{reported_mutex};
                observed = reported;
            }
            if (!observed)
            {
                std::this_thread::sleep_for(10ms);
            }
        }

        runtime.close();
        runtime.join();

        assert(observed);
        assert(observed->first == entry);
        assert(observed->second == room);
        assert(completion.failed.load() == 0);
    }

    void test_a_rejected_reward_snapshot_retries_then_passivates()
    {
        const snf::server::PlayerId player{.value = 47};
        RetryHarness harness{200ms};
        harness.repository.seed(snf::server::PlayerRecord{
            .player = player,
            .street_experience = 100,
        });
        fill_persistence_queue(harness);
        harness.runtime.start();

        assert(tell_grant(harness.runtime, player, 300) == snf::runtime::PostResult::Accepted);
        assert(wait_until(
            [&harness]
            {
                return harness.binding.stats().reward_snapshot_admission_rejections == 1;
            }
        ));
        assert(actor_count(harness.runtime) == 1);

        harness.repository.releaseBlockedSave();
        assert(wait_until(
            [&harness]
            {
                return harness.persistence.stats().queue_depth == 0;
            }
        ));
        assert(wait_until(
            [&harness, player]
            {
                const auto record = harness.repository.find(player);
                return record && record->street_experience == 400;
            }
        ));
        assert(wait_until(
            [&harness]
            {
                return actor_count(harness.runtime) == 0;
            }
        ));

        const auto stats = harness.binding.stats();
        assert(stats.reward_snapshot_admission_rejections == 1);
        assert(stats.reward_snapshot_retry_giveups == 0);
        harness.runtime.close();
        harness.runtime.join();
        harness.persistence.flush();
        harness.persistence.stop();
        assert(harness.completion.failed.load() == 0);
    }

    void test_reward_snapshot_retry_limit_is_five_attempts_after_initial_rejection()
    {
        const snf::server::PlayerId player{.value = 48};
        RetryHarness harness{1ms, 5};
        harness.repository.seed(snf::server::PlayerRecord{
            .player = player,
            .street_experience = 100,
        });
        harness.persistence.stop();
        harness.runtime.start();

        assert(tell_grant(harness.runtime, player, 300) == snf::runtime::PostResult::Accepted);
        assert(wait_until(
            [&harness]
            {
                return harness.binding.stats().reward_snapshot_retry_giveups == 1;
            }
        ));
        const auto stats = harness.binding.stats();
        assert(stats.reward_snapshot_admission_rejections == 6);
        assert(stats.reward_snapshot_retry_giveups == 1);
        assert(harness.repository.find(player)->street_experience == 100);
        assert(wait_until(
            [&harness]
            {
                return actor_count(harness.runtime) == 0;
            }
        ));

        harness.runtime.close();
        harness.runtime.join();
        assert(harness.completion.failed.load() == 0);
    }

    void test_reward_snapshot_schedule_rejection_gives_up()
    {
        const snf::server::PlayerId player{.value = 49};
        RetryHarness harness{20ms, 5, 1};
        harness.repository.seed(snf::server::PlayerRecord{
            .player = player,
            .street_experience = 100,
        });
        harness.persistence.stop();
        harness.runtime.start();

        assert(tell_grant(harness.runtime, player, 300) == snf::runtime::PostResult::Accepted);
        assert(wait_until(
            [&harness]
            {
                return harness.binding.stats().reward_snapshot_retry_giveups == 1;
            }
        ));
        const auto stats = harness.binding.stats();
        assert(stats.reward_snapshot_admission_rejections == 1);
        assert(stats.reward_snapshot_retry_giveups == 1);
        assert(harness.runtime.getStats().workers.front().timers_rejected_full == 1);
        assert(wait_until(
            [&harness]
            {
                return actor_count(harness.runtime) == 0;
            }
        ));

        harness.runtime.close();
        harness.runtime.join();
        assert(harness.completion.failed.load() == 0);
    }

    void test_an_offline_grant_load_failure_is_counted_and_passivates()
    {
        const snf::server::PlayerId player{.value = 50};
        RetryHarness harness;
        harness.repository.failLoads(true);
        harness.runtime.start();

        assert(tell_grant(harness.runtime, player, 300) == snf::runtime::PostResult::Accepted);
        assert(wait_until(
            [&harness]
            {
                return harness.binding.stats().grant_load_failures == 1;
            }
        ));
        const auto stats = harness.binding.stats();
        assert(stats.reward_snapshot_admission_rejections == 0);
        assert(stats.reward_snapshot_retry_giveups == 0);
        assert(!harness.repository.find(player));
        assert(wait_until(
            [&harness]
            {
                return actor_count(harness.runtime) == 0;
            }
        ));

        harness.runtime.close();
        harness.runtime.join();
        harness.persistence.stop();
        assert(harness.completion.failed.load() == 0);
    }

    void test_a_session_command_during_retry_prevents_late_retry_passivation()
    {
        const snf::server::PlayerId player{.value = 51};
        const snf::net::ConnectionId connection{.descriptor = 8, .generation = 2};
        RetryHarness harness{200ms};
        harness.repository.seed(snf::server::PlayerRecord{
            .player = player,
            .street_experience = 100,
        });
        fill_persistence_queue(harness);
        harness.runtime.start();

        assert(tell_grant(harness.runtime, player, 300) == snf::runtime::PostResult::Accepted);
        assert(wait_until(
            [&harness]
            {
                return harness.binding.stats().reward_snapshot_admission_rejections == 1;
            }
        ));
        assert(
            harness.runtime.tryPost(harness.binding.makeCommand(snf::server::PlayerInboundCommand{
                .actor = snf::server::PlayerActorId{player},
                .connection = connection,
                .command = snf::server::PingCommand{},
                .request_id = 1,
            })) == snf::runtime::PostResult::Accepted
        );
        assert(wait_until(
            [&harness]
            {
                return harness.outbound.tryPop().has_value();
            }
        ));
        assert(harness.binding.stats().reward_snapshot_admission_rejections == 2);

        harness.repository.releaseBlockedSave();
        assert(wait_until(
            [&harness, player]
            {
                const auto record = harness.repository.find(player);
                return record && record->street_experience == 400;
            }
        ));
        assert(wait_until(
            [&harness]
            {
                return harness.runtime.getStats().workers.front().timers_fired >= 1;
            }
        ));
        assert(actor_count(harness.runtime) == 1);

        assert(
            harness.runtime.tryPost(harness.binding.makeConnectionClosed(
                snf::server::PlayerActorId{player},
                snf::server::ConnectionClosed{
                    .connection = connection,
                    .cause = snf::server::ConnectionCloseCause::PeerClosed,
                    .last_location = std::nullopt,
                }
            )) == snf::runtime::PostResult::Accepted
        );
        assert(wait_until(
            [&harness]
            {
                return actor_count(harness.runtime) == 0;
            }
        ));
        harness.runtime.close();
        harness.runtime.join();
        harness.persistence.flush();
        harness.persistence.stop();
        assert(harness.binding.stats().reward_snapshot_retry_giveups == 0);
        assert(harness.completion.failed.load() == 0);
    }
}

void run_player_tell_tests()
{
    test_a_grant_to_an_offline_player_loads_the_record_before_applying();
    test_grants_accumulate_across_tells();
    test_a_grant_naming_another_player_is_refused();
    test_a_refused_room_join_reports_the_entry_it_belonged_to();
    test_a_rejected_reward_snapshot_retries_then_passivates();
    test_reward_snapshot_retry_limit_is_five_attempts_after_initial_rejection();
    test_reward_snapshot_schedule_rejection_gives_up();
    test_an_offline_grant_load_failure_is_counted_and_passivates();
    test_a_session_command_during_retry_prevents_late_retry_passivation();
}
