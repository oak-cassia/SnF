#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/party_actor_binding.hpp"
#include "snf/server/party_actor_ingress.hpp"

#include <atomic>
#include <cassert>
#include <mutex>
#include <thread>
#include <vector>

namespace
{
    class RecordingCompletion final : public snf::runtime::RuntimeCompletionSink
    {
    public:
        void notifyDrained(snf::runtime::RuntimeId runtime) noexcept override
        {
            assert(runtime == snf::runtime::RuntimeId::Logic);
            ++drained;
        }

        void notifyFailed(snf::runtime::RuntimeId runtime) noexcept override
        {
            assert(runtime == snf::runtime::RuntimeId::Logic);
            ++failed;
        }

        std::atomic<int> drained{0};
        std::atomic<int> failed{0};
    };

    void test_party_binding_shares_one_mailbox_and_passivates_when_empty()
    {
        struct Recorded
        {
            std::mutex mutex;
            std::vector<snf::server::PartyResult> results;
            std::vector<std::thread::id> threads;
        } recorded;

        const std::thread::id caller = std::this_thread::get_id();
        snf::server::CountingCommandLifecycleSink lifecycle;
        snf::server::PartyActorBinding binding{snf::server::PartyActorBindingConfig{
                                                   .actor = snf::server::PartyActorConfig{.max_members = 2},
                                                   .on_result =
                                                       [&recorded](const snf::server::PartyInboundCommand&, const snf::server::PartyResult& result)
                                                   {
                                                       std::lock_guard lock{recorded.mutex};
                                                       recorded.results.push_back(result);
                                                       recorded.threads.push_back(std::this_thread::get_id());
                                                   },
                                               },
                                               lifecycle};
        RecordingCompletion completion;
        snf::runtime::ActorRuntime runtime{snf::runtime::ActorRuntimeConfig{
                                               .worker_count = 1,
                                               .queue_capacity_per_worker = 8,
                                               .max_in_flight_operations_per_worker = 2,
                                               .on_worker_start = {},
                                               .on_before_dispatch = {},
                                               .on_worker_failure = {},
                                           },
                                           completion};
        runtime.registerBinding(binding);
        snf::server::PartyActorIngress ingress{runtime, binding, lifecycle};
        runtime.start();

        const snf::server::PartyId party{.value = 10};
        const snf::server::PlayerId first{.value = 1};
        const snf::server::PlayerId second{.value = 2};
        const auto post = [&ingress, party](snf::server::PartyCommand command, const std::uint32_t request_id)
        {
            return ingress.tryPost(snf::server::PartyInboundCommand{
                .party = party,
                .connection = {.descriptor = 4, .generation = 1},
                .command = std::move(command),
                .reply =
                    snf::server::PartyReplyContext{
                        .connection = {.descriptor = 4, .generation = 1},
                        .request_id = request_id,
                        .kind = request_id <= 2 ? snf::server::PartyReplyKind::Joined : snf::server::PartyReplyKind::Left,
                    },
            });
        };

        assert(post(snf::server::JoinPartyCommand{.player = first, .membership_epoch = 1}, 1) == snf::runtime::PostResult::Accepted);
        assert(post(snf::server::JoinPartyCommand{.player = second, .membership_epoch = 1}, 2) == snf::runtime::PostResult::Accepted);
        assert(post(snf::server::LeavePartyCommand{.player = first, .membership_epoch = 1}, 3) == snf::runtime::PostResult::Accepted);
        assert(post(snf::server::LeavePartyCommand{.player = second, .membership_epoch = 1}, 4) == snf::runtime::PostResult::Accepted);
        runtime.close();
        runtime.join();

        {
            std::lock_guard lock{recorded.mutex};
            assert(recorded.results.size() == 4);
            assert(recorded.results[1].members == std::vector<snf::server::PlayerId>({first, second}));
            assert(recorded.results.back().members.empty());
            for (const std::thread::id thread : recorded.threads)
            {
                assert(thread != caller);
                assert(thread == recorded.threads.front());
            }
        }
        const auto runtime_stats = runtime.getStats().workers.front();
        assert(runtime_stats.processed == 4);
        assert(runtime_stats.evicted_actors == 1);
        assert(runtime_stats.actor_count == 0);
        const auto binding_stats = binding.stats();
        assert(binding_stats.commands == 4);
        assert(binding_stats.rejected == 0);
        assert(binding_stats.passivation_requests == 1);
        assert(lifecycle.terminalCount() == 4);
        assert(completion.drained.load() == 1);
        assert(completion.failed.load() == 0);
    }
}

void run_party_actor_binding_tests()
{
    test_party_binding_shares_one_mailbox_and_passivates_when_empty();
}
