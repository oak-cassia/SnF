#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/room_actor_binding.hpp"
#include "snf/server/room_actor_ingress.hpp"
#include "snf/server/street_experience_grant.hpp"

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
            ++failed;
        }

        std::atomic<int> drained{0};
        std::atomic<int> failed{0};
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
        [[nodiscard]] std::optional<snf::runtime::ActorSubmission> makeTell(const snf::runtime::ActorKey target, snf::runtime::TellPayload payload) override
        {
            auto grant = payload.take<snf::server::StreetExperienceGrant>();
            if (!grant)
            {
                return std::nullopt;
            }
            return makeSubmission(target, snf::runtime::ActorActivation::ActivateIfMissing, snf::runtime::ActorAccounting::Command, GrantPayload{.experience = grant->experience});
        }

        [[nodiscard]] std::unique_ptr<snf::runtime::ActorSlot> activate(snf::runtime::EntityId) override
        {
            return std::make_unique<Slot>();
        }

        [[nodiscard]] snf::runtime::ActorDispatchResult dispatch(snf::runtime::ActorSlot&, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext&, std::stop_token) override
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

        [[nodiscard]] snf::runtime::ActorDispatchResult resume(snf::runtime::ActorSlot&, snf::runtime::ActorContext&, std::stop_token) override
        {
            throw std::logic_error{"RecordingPlayerBinding has no suspension point"};
        }

    private:
        struct GrantPayload
        {
            std::uint64_t experience{0};
        };

        struct Slot final : snf::runtime::ActorSlot
        {
        };
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

    void test_a_room_clears_from_its_own_timer_and_rewards_every_participant()
    {
        RecordingPlayerBinding player_binding;
        player_binding.expected = 2;
        auto arrived = player_binding.all_arrived.get_future();

        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::RoomActorBinding binding{snf::server::RoomActorBindingConfig{
                                                  .actor =
                                                      snf::server::RoomActorConfig{
                                                          .battle_duration = 30ms,
                                                          .max_participants = 4,
                                                          .clear_experience = 300,
                                                      },
                                                  .on_result = {},
                                              },
                                              lifecycle};
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{runtime_config(), completion};
        runtime.registerBinding(binding);
        runtime.registerBinding(player_binding);
        snf::server::RoomActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const snf::server::RoomId room{.value = 10};
        for (const std::uint64_t player : {std::uint64_t{20}, std::uint64_t{10}})
        {
            assert(ingress.tryPost(snf::server::RoomInboundCommand{
                       .room = room,
                       .command = snf::server::JoinRoom{.player = snf::server::PlayerId{.value = player}},
                       .reply = std::nullopt,
                   }) == snf::runtime::PostResult::Accepted);
        }
        assert(ingress.tryPost(snf::server::RoomInboundCommand{
                   .room = room,
                   .command = snf::server::StartBattle{},
                   .reply = std::nullopt,
               }) == snf::runtime::PostResult::Accepted);

        // Nothing else posts BattleCompleted: the only thing that can finish this
        // battle is the timer the Room armed for itself.
        assert(arrived.wait_for(5s) == std::future_status::ready);

        runtime.close();
        runtime.join();

        std::lock_guard lock{player_binding.mutex};
        std::ranges::sort(player_binding.grants, {}, &RecordingPlayerBinding::Grant::entity);
        assert((player_binding.grants == std::vector<RecordingPlayerBinding::Grant>{
                                             {.entity = 10, .experience = 300},
                                             {.entity = 20, .experience = 300},
                                         }));
        assert(completion.drained.load() == 1);
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
        assert(ingress.tryPost(snf::server::RoomInboundCommand{
                   .room = snf::server::RoomId{.value = 11},
                   .command = snf::server::StartBattle{},
                   .reply = std::nullopt,
               }) == snf::runtime::PostResult::Accepted);

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
}

void run_room_actor_binding_tests()
{
    test_a_room_clears_from_its_own_timer_and_rewards_every_participant();
    test_a_room_that_never_started_passivates();
}
