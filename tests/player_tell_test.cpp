#include "outbound_reservation_test_support.hpp"
#include "snf/game/street_experience_grant.hpp"
#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/runtime/tell_payload.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_repository.hpp"
#include "snf/server/protocol_player_response_sink.hpp"
#include "snf/server/room_actor_binding.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <thread>
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

        // Nobody is connected: this slot is activated by the tell alone.
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
        // The whole point of loading first: a grant applied to a default-constructed
        // actor would have saved zeros over everything else the record holds.
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
            // The key says actor 44, the grant says player 45. Only a routing bug can
            // produce that, and crediting the wrong account is worse than failing loud.
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

    // A room join whose Room mailbox refused it. The entry saga that started the join is
    // waiting on a completion that will now never arrive and has no timeout, so a dropped
    // tell would strand the connection in a hidden route forever.
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
        // The tell needs a target binding to assemble it, and Room commands are what
        // fills the worker's ingress below.
        snf::server::RoomActorBinding room_binding{snf::server::RoomActorBindingConfig{}};

        // Armed by the test thread and read by the Worker, so it has to be atomic: this
        // test is meant to run clean under TSan.
        std::atomic<bool> fill_armed{false};
        // Set before start(), so the threads that read it are created afterwards.
        snf::runtime::ActorRuntime* runtime_pointer = nullptr;
        snf::runtime::ActorRuntime runtime{
            snf::runtime::ActorRuntimeConfig{
                .worker_count = 1,
                .queue_capacity_per_worker = 2,
                .max_in_flight_operations_per_worker = 4,
                .on_worker_start = {},
                // Runs on the Worker with the join already dequeued, so filling the
                // ingress here is what the handler's tell runs into. Saturating from the
                // test thread could not do it: the slot the join occupied is free again
                // by the time the handler runs.
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

        // First command loads the record, so the join below runs in one turn and the
        // tell happens inside the dispatch the hook above saturated.
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

        // The refusal carries the saga's whole identity back, which is what lets the
        // reactor turn it into the completion the entry is waiting for.
        assert(observed);
        assert(observed->first == entry);
        assert(observed->second == room);
        assert(completion.failed.load() == 0);
    }
}

void run_player_tell_tests()
{
    test_a_grant_to_an_offline_player_loads_the_record_before_applying();
    test_grants_accumulate_across_tells();
    test_a_grant_naming_another_player_is_refused();
    test_a_refused_room_join_reports_the_entry_it_belonged_to();
}
