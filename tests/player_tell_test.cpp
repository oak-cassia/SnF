#include "outbound_reservation_test_support.hpp"
#include "snf/game/street_experience_grant.hpp"
#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/runtime/tell_payload.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_repository.hpp"
#include "snf/server/protocol_player_response_sink.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <thread>

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
}

void run_player_tell_tests()
{
    test_a_grant_to_an_offline_player_loads_the_record_before_applying();
    test_grants_accumulate_across_tells();
    test_a_grant_naming_another_player_is_refused();
}
