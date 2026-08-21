#include "snf/runtime/actor_key.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/actor_task.hpp"
#include "snf/runtime/async_operation.hpp"
#include "snf/runtime/runtime_completion.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <thread>
#include <utility>
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
    using snf::runtime::ActorRuntimeStats;
    using snf::runtime::ActorState;
    using snf::runtime::ActorSubmission;
    using snf::runtime::ActorTask;
    using snf::runtime::ActorTaskStatus;
    using snf::runtime::AsyncOperationCancelled;
    using snf::runtime::AsyncOperationProducer;
    using snf::runtime::AsyncOperationRejected;
    using snf::runtime::awaitAsyncOperation;
    using snf::runtime::EntityId;
    using snf::runtime::PostResult;

    class RecordingRuntimeCompletion final : public snf::runtime::RuntimeCompletionSink
    {
    public:
        void notifyDrained(snf::runtime::RuntimeId) noexcept override
        {
            drained_count.fetch_add(1);
        }

        void notifyFailed(snf::runtime::RuntimeId) noexcept override
        {
            failed_count.fetch_add(1);
        }

        std::atomic<int> drained_count{0};
        std::atomic<int> failed_count{0};
    };

    struct ClaimWindowGate
    {
        std::atomic<int> move_count{0};
        std::atomic<bool> result_store_entered{false};
        std::atomic<bool> release_result_store{false};
    };

    // Test-only result that widens the otherwise non-blocking claim-to-publish
    // window. Its second move is AsyncOperationState's result store, after the
    // Pending -> Completed CAS but before continuation publication.
    struct ClaimWindowResult
    {
        ClaimWindowResult(int result_value, std::shared_ptr<ClaimWindowGate> result_gate) noexcept
            : value(result_value)
            , gate(std::move(result_gate))
        {
        }

        ClaimWindowResult(const ClaimWindowResult&) = delete;
        ClaimWindowResult& operator=(const ClaimWindowResult&) = delete;

        ClaimWindowResult(ClaimWindowResult&& other) noexcept
            : value(other.value)
            , gate(std::move(other.gate))
        {
            const int move = gate->move_count.fetch_add(1, std::memory_order_acq_rel) + 1;
            if (move == 2)
            {
                gate->result_store_entered.store(true, std::memory_order_release);
                while (!gate->release_result_store.load(std::memory_order_acquire))
                {
                    std::this_thread::yield();
                }
            }
        }

        ClaimWindowResult& operator=(ClaimWindowResult&&) = delete;

        int value{0};
        std::shared_ptr<ClaimWindowGate> gate;
    };

    // Holds producers until the test thread decides to complete them, so a
    // completion always arrives from a thread other than the owning Worker.
    class TestAsyncService final
    {
    public:
        void submit(const int value, AsyncOperationProducer<int> producer)
        {
            std::lock_guard lock{_mutex};
            _pending.push_back(Pending{.value = value, .producer = std::move(producer)});
        }

        void submitClaimWindow(std::shared_ptr<ClaimWindowGate> gate, AsyncOperationProducer<ClaimWindowResult> producer)
        {
            std::lock_guard lock{_mutex};
            _claim_window_pending.emplace(ClaimWindowPending{.gate = std::move(gate), .producer = std::move(producer)});
        }

        [[nodiscard]] std::size_t pendingCount() const
        {
            std::lock_guard lock{_mutex};
            return _pending.size();
        }

        [[nodiscard]] bool hasClaimWindowPending() const
        {
            std::lock_guard lock{_mutex};
            return _claim_window_pending.has_value();
        }

        void completeAll()
        {
            for (auto& pending : take())
            {
                pending.producer.complete(pending.value * 10);
            }
        }

        void failAll()
        {
            for (auto& pending : take())
            {
                pending.producer.fail(std::make_exception_ptr(std::runtime_error{"test async service failure"}));
            }
        }

        // Publishes the same operation twice. The second publish must lose the
        // terminal claim rather than resume the actor again.
        void completeAllTwice()
        {
            for (auto& pending : take())
            {
                pending.producer.complete(pending.value * 10);
                pending.producer.complete(pending.value * 10);
            }
        }

        void completeClaimWindow(const int value)
        {
            std::optional<ClaimWindowPending> pending;
            {
                std::lock_guard lock{_mutex};
                pending = std::move(_claim_window_pending);
                _claim_window_pending.reset();
            }

            assert(pending.has_value());
            pending->producer.complete(ClaimWindowResult{value, std::move(pending->gate)});
        }

    private:
        struct Pending
        {
            int value{0};
            AsyncOperationProducer<int> producer;
        };

        struct ClaimWindowPending
        {
            std::shared_ptr<ClaimWindowGate> gate;
            AsyncOperationProducer<ClaimWindowResult> producer;
        };

        [[nodiscard]] std::vector<Pending> take()
        {
            std::lock_guard lock{_mutex};
            std::vector<Pending> taken;
            taken.reserve(_pending.size());
            for (auto& pending : _pending)
            {
                taken.push_back(std::move(pending));
            }
            _pending.clear();
            return taken;
        }

        mutable std::mutex _mutex;
        std::deque<Pending> _pending;
        std::optional<ClaimWindowPending> _claim_window_pending;
    };

    enum class Behavior : std::uint8_t
    {
        AwaitOnce,
        AwaitTwice,
        CompleteInline,
        ThrowBeforeAwait,
        ThrowAfterAwait,
        ThrowOnSubmit,
        PauseAfterTerminalClaim,
        BlockWorker,
        NoAwait,
    };

    enum class OutcomeKind : std::uint8_t
    {
        Completed,
        Cancelled,
        Rejected,
    };

    struct Outcome
    {
        OutcomeKind kind{OutcomeKind::Completed};
        int value{0};
    };

    // Records the thread that touched each stage of a task's life. Worker affinity
    // is only meaningful if these all match and differ from the completing thread.
    struct AffinityRecord
    {
        std::atomic<std::thread::id> dispatched{};
        std::atomic<std::thread::id> resumed{};
        std::atomic<std::thread::id> frame_destroyed{};
        std::atomic<int> payload_value_at_frame_destruction{-1};
    };

    // Its destructor runs when the coroutine frame is destroyed, which is how a
    // test observes where destruction happened even when the frame is dropped
    // without being resumed.
    class FrameProbe final
    {
    public:
        FrameProbe(AffinityRecord* record, const int* payload_value) noexcept
            : _record(record)
            , _payload_value(payload_value)
        {
        }

        FrameProbe(const FrameProbe&) = delete;
        FrameProbe& operator=(const FrameProbe&) = delete;

        ~FrameProbe()
        {
            if (_record != nullptr)
            {
                _record->payload_value_at_frame_destruction.store(*_payload_value);
                _record->frame_destroyed.store(std::this_thread::get_id());
            }
        }

    private:
        AffinityRecord* _record;
        const int* _payload_value;
    };

    class AwaitingBinding final : public ActorBinding
    {
    public:
        struct State
        {
            std::mutex mutex;
            std::vector<Outcome> outcomes;
            AffinityRecord affinity;
            std::atomic<int> submit_calls{0};
            std::atomic<bool> block_started{false};
            std::atomic<bool> release_block{false};
            std::shared_ptr<ClaimWindowGate> claim_window_gate = std::make_shared<ClaimWindowGate>();
        };

        AwaitingBinding(TestAsyncService& service, std::shared_ptr<State> state)
            : _service(service)
            , _state(std::move(state))
        {
        }

        [[nodiscard]] ActorKind kind() const noexcept override
        {
            return ActorKind::Zone;
        }

        [[nodiscard]] ActorSubmission post(const EntityId entity, const int value, const Behavior behavior) const
        {
            return makeSubmission(ActorKey{.kind = kind(), .entity = entity}, ActorActivation::ActivateIfMissing, ActorAccounting::Command, Payload{.value = value, .behavior = behavior});
        }

        [[nodiscard]] std::vector<Outcome> outcomes() const
        {
            std::lock_guard lock{_state->mutex};
            return _state->outcomes;
        }

    protected:
        [[nodiscard]] std::unique_ptr<ActorState> activate(EntityId) override
        {
            return std::make_unique<Slot>();
        }

        [[nodiscard]] ActorDispatchResult dispatch(ActorState& slot, const ActorSubmission& submission, ActorContext& context, std::stop_token stop_token) override
        {
            auto& typed_slot = dynamic_cast<Slot&>(slot);
            _state->affinity.dispatched.store(std::this_thread::get_id());

            const Payload& payload = payloadAs<Payload>(submission);
            typed_slot.task = runCommand(_service, context, payload, *_state);
            return advance(typed_slot, stop_token);
        }

        [[nodiscard]] ActorDispatchResult resume(ActorState& slot, ActorContext&, std::stop_token stop_token) override
        {
            _state->affinity.resumed.store(std::this_thread::get_id());
            return advance(dynamic_cast<Slot&>(slot), stop_token);
        }

    private:
        struct Slot final : ActorState
        {
            ActorTask<Outcome> task;
        };

        struct Payload
        {
            int value{0};
            Behavior behavior{Behavior::AwaitOnce};
        };

        [[nodiscard]] static auto awaitOne(TestAsyncService& service, ActorContext& context, const Payload& payload, State& state)
        {
            return awaitAsyncOperation<int>(context,
                                            [&service, &payload, &state](AsyncOperationProducer<int> producer)
                                            {
                                                state.submit_calls.fetch_add(1);
                                                if (payload.behavior == Behavior::ThrowOnSubmit)
                                                {
                                                    throw std::runtime_error{"submitting the operation failed"};
                                                }

                                                if (payload.behavior == Behavior::CompleteInline)
                                                {
                                                    // Completes before await_suspend returns. The publish has
                                                    // to wait for the owning Worker rather than resume here.
                                                    producer.complete(payload.value * 10);
                                                    return;
                                                }

                                                service.submit(payload.value, std::move(producer));
                                            });
        }

        [[nodiscard]] static auto awaitClaimWindow(TestAsyncService& service, ActorContext& context, State& state)
        {
            return awaitAsyncOperation<ClaimWindowResult>(context,
                                                          [&service, &state](AsyncOperationProducer<ClaimWindowResult> producer)
                                                          {
                                                              state.submit_calls.fetch_add(1);
                                                              service.submitClaimWindow(state.claim_window_gate, std::move(producer));
                                                          });
        }

        static ActorTask<Outcome> runCommand(TestAsyncService& service, ActorContext& context, const Payload& payload, State& state)
        {
            const FrameProbe probe{&state.affinity, &payload.value};

            if (payload.behavior == Behavior::ThrowBeforeAwait)
            {
                throw std::runtime_error{"handler failed before awaiting"};
            }

            if (payload.behavior == Behavior::NoAwait)
            {
                co_return Outcome{.kind = OutcomeKind::Completed, .value = payload.value};
            }

            if (payload.behavior == Behavior::BlockWorker)
            {
                state.block_started.store(true, std::memory_order_release);
                while (!state.release_block.load(std::memory_order_acquire))
                {
                    std::this_thread::yield();
                }
                co_return Outcome{.kind = OutcomeKind::Completed, .value = payload.value};
            }

            try
            {
                if (payload.behavior == Behavior::PauseAfterTerminalClaim)
                {
                    ClaimWindowResult result = co_await awaitClaimWindow(service, context, state);
                    co_return Outcome{.kind = OutcomeKind::Completed, .value = result.value};
                }

                int result = co_await awaitOne(service, context, payload, state);
                if (payload.behavior == Behavior::AwaitTwice)
                {
                    result += co_await awaitOne(service, context, payload, state);
                }

                if (payload.behavior == Behavior::ThrowAfterAwait)
                {
                    throw std::runtime_error{"handler failed after awaiting"};
                }

                co_return Outcome{.kind = OutcomeKind::Completed, .value = result};
            }
            catch (const AsyncOperationCancelled&)
            {
                co_return Outcome{.kind = OutcomeKind::Cancelled};
            }
            catch (const AsyncOperationRejected&)
            {
                co_return Outcome{.kind = OutcomeKind::Rejected};
            }
        }

        [[nodiscard]] ActorDispatchResult advance(Slot& slot, const std::stop_token stop_token)
        {
            if (slot.task.resume() == ActorTaskStatus::Suspended)
            {
                return ActorDispatchResult::Suspended;
            }

            Outcome outcome = slot.task.takeResult();
            slot.task = {};

            {
                std::lock_guard lock{_state->mutex};
                _state->outcomes.push_back(outcome);
            }

            static_cast<void>(stop_token);
            return ActorDispatchResult::KeepActive;
        }

        TestAsyncService& _service;
        std::shared_ptr<AwaitingBinding::State> _state;
    };

    struct Harness
    {
        explicit Harness(const std::size_t workers = 1, const std::size_t max_in_flight = 8)
            : state(std::make_shared<AwaitingBinding::State>())
            , binding(service, state)
            , runtime(makeConfig(workers, max_in_flight), completion)
        {
            runtime.registerBinding(binding);
            runtime.start();
        }

        static ActorRuntimeConfig makeConfig(const std::size_t workers, const std::size_t max_in_flight)
        {
            ActorRuntimeConfig config;
            config.worker_count = workers;
            config.queue_capacity_per_worker = 16;
            config.max_in_flight_operations_per_worker = max_in_flight;
            return config;
        }

        [[nodiscard]] snf::runtime::ActorRuntimeWorkerStats workerStats(const ActorKey& key) const
        {
            return runtime.getStats().workers[runtime.workerIndexFor(key)];
        }

        TestAsyncService service;
        RecordingRuntimeCompletion completion;
        std::shared_ptr<AwaitingBinding::State> state;
        AwaitingBinding binding;
        ActorRuntime runtime;
    };

    template <typename Predicate> bool wait_until(Predicate predicate)
    {
        // Deliberately generous: a slow sanitizer build must not turn a correct
        // interleaving into a failure.
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < deadline)
        {
            if (predicate())
            {
                return true;
            }
            std::this_thread::sleep_for(200us);
        }

        return predicate();
    }

    ActorKey zone_key(const EntityId entity)
    {
        return ActorKey{.kind = ActorKind::Zone, .entity = entity};
    }

    EntityId entity_for_worker(const ActorRuntime& runtime, const std::size_t worker_index)
    {
        for (EntityId entity = 1; entity < 10'000; ++entity)
        {
            if (runtime.workerIndexFor(zone_key(entity)) == worker_index)
            {
                return entity;
            }
        }

        throw std::logic_error{"Could not find a test ActorKey for the requested Worker"};
    }

    void test_suspend_lets_other_actors_run_and_queues_the_same_actor()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 5, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        // Actor 1 is suspended. A second command for the same actor must wait in
        // its mailbox while a different actor makes progress on the same Worker.
        assert(harness.runtime.tryPost(harness.binding.post(1, 6, Behavior::NoAwait)) == PostResult::Accepted);
        assert(harness.runtime.tryPost(harness.binding.post(2, 7, Behavior::NoAwait)) == PostResult::Accepted);

        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));
        const auto during_suspension = harness.binding.outcomes();
        assert(during_suspension.size() == 1);
        assert(during_suspension.front().value == 7);

        const auto suspended_stats = harness.workerStats(zone_key(1));
        assert(suspended_stats.suspended_task_count == 1);
        assert(suspended_stats.in_flight_operations == 1);
        assert(suspended_stats.suspended_commands == 1);

        harness.service.completeAll();
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 3; }));

        const auto outcomes = harness.binding.outcomes();
        assert(outcomes[1].kind == OutcomeKind::Completed);
        assert(outcomes[1].value == 50);
        // The queued command runs only after the suspended one reached terminal.
        assert(outcomes[2].value == 6);

        harness.runtime.close();
        harness.runtime.join();
        assert(harness.completion.drained_count.load() == 1);

        const auto final_stats = harness.workerStats(zone_key(1));
        assert(final_stats.suspended_task_count == 0);
        assert(final_stats.in_flight_operations == 0);
    }

    void test_resume_and_frame_destruction_stay_on_the_owning_worker()
    {
        Harness harness{1};
        const auto completing_thread = std::this_thread::get_id();

        assert(harness.runtime.tryPost(harness.binding.post(1, 3, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        harness.service.completeAll();
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));

        harness.runtime.close();
        harness.runtime.join();

        const auto dispatched = harness.state->affinity.dispatched.load();
        const auto resumed = harness.state->affinity.resumed.load();
        const auto destroyed = harness.state->affinity.frame_destroyed.load();
        assert(dispatched == resumed);
        assert(dispatched == destroyed);
        assert(dispatched != completing_thread);
    }

    void test_immediate_completion_resumes_through_the_worker()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 4, Behavior::CompleteInline)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));

        const auto outcomes = harness.binding.outcomes();
        assert(outcomes.front().kind == OutcomeKind::Completed);
        assert(outcomes.front().value == 40);

        // The producer ran on the Worker itself, yet the resume still went through
        // the continuation queue rather than happening inline.
        const auto dispatched = harness.state->affinity.dispatched.load();
        assert(harness.state->affinity.resumed.load() == dispatched);

        harness.runtime.close();
        harness.runtime.join();
        assert(harness.completion.drained_count.load() == 1);
    }

    void test_sequential_awaits_keep_one_command_accounting()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 2, Behavior::AwaitTwice)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));
        harness.service.completeAll();
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));
        harness.service.completeAll();
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));

        assert(harness.binding.outcomes().front().value == 40);
        assert(harness.state->submit_calls.load() == 2);

        harness.runtime.close();
        harness.runtime.join();

        const auto stats = harness.workerStats(zone_key(1));
        // One acceptance, one terminal: two suspensions must not double-count.
        assert(stats.accepted == 1);
        assert(stats.processed == 1);
        assert(stats.queue_wait_nanoseconds.sample_count == 1);
        assert(stats.suspended_commands == 1);
        assert(stats.suspend_duration_nanoseconds.sample_count == 2);
        assert(stats.queue_depth == 0);
        assert(stats.in_flight_operations == 0);
    }

    void test_reservation_saturation_rejects_before_the_operation_starts()
    {
        Harness harness{1, 1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 1, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        // The single in-flight slot is taken, so the next actor cannot start an
        // operation at all.
        assert(harness.runtime.tryPost(harness.binding.post(2, 2, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));

        assert(harness.binding.outcomes().front().kind == OutcomeKind::Rejected);
        assert(harness.workerStats(zone_key(2)).reservation_rejections == 1);
        // Nothing was submitted for the rejected operation beyond the attempt.
        assert(harness.service.pendingCount() == 1);

        harness.service.completeAll();
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 2; }));

        harness.runtime.close();
        harness.runtime.join();
    }

    void test_double_completion_is_counted_and_applied_once()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 8, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        harness.service.completeAllTwice();
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));

        harness.runtime.close();
        harness.runtime.join();

        const auto stats = harness.workerStats(zone_key(1));
        assert(stats.double_completions == 1);
        assert(stats.discarded_late_completions == 0);
        assert(harness.binding.outcomes().size() == 1);
        assert(harness.binding.outcomes().front().value == 80);
    }

    void test_producer_failure_propagates_into_the_handler()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 9, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        harness.service.failAll();

        // The handler does not catch a producer failure, so it reaches the Worker
        // as a failure exactly like a synchronous handler throw would.
        bool threw = false;
        try
        {
            harness.runtime.join();
        }
        catch (const std::runtime_error&)
        {
            threw = true;
        }

        assert(threw);
        assert(harness.completion.failed_count.load() == 1);
        assert(harness.workerStats(zone_key(1)).queue_depth == 0);
    }

    void test_per_operation_cancel_resumes_the_handler_with_a_cancellation()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 11, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        harness.runtime.requestActorOperationCancel(zone_key(1));
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));
        assert(harness.binding.outcomes().front().kind == OutcomeKind::Cancelled);
        assert(harness.workerStats(zone_key(1)).cancelled_operations == 1);

        // A completion that arrives after the cancellation won the claim is a late
        // completion, not a second terminal outcome.
        harness.service.completeAll();
        assert(wait_until([&harness] { return harness.workerStats(zone_key(1)).discarded_late_completions == 1; }));
        assert(harness.binding.outcomes().size() == 1);

        harness.runtime.close();
        harness.runtime.join();
        assert(harness.completion.drained_count.load() == 1);
    }

    void test_cancel_losing_the_claim_waits_for_the_guaranteed_publish()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 12, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        // The completion claims the terminal transition first, so the cancel must
        // lose and the Worker must consume the already-published continuation
        // instead of resuming with a cancellation.
        harness.service.completeAll();
        harness.runtime.requestActorOperationCancel(zone_key(1));

        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));
        assert(harness.binding.outcomes().front().kind == OutcomeKind::Completed);
        assert(harness.binding.outcomes().front().value == 120);

        harness.runtime.close();
        harness.runtime.join();
        assert(harness.completion.drained_count.load() == 1);
    }

    void test_cancel_waits_when_completion_claimed_but_has_not_published_yet()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 23, Behavior::PauseAfterTerminalClaim)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.hasClaimWindowPending(); }));

        std::jthread completer{[&harness] { harness.service.completeClaimWindow(230); }};
        assert(wait_until([&harness] { return harness.state->claim_window_gate->result_store_entered.load(std::memory_order_acquire); }));

        // Completed owns the terminal transition, but its result store and publish
        // are deliberately paused. Cancellation must leave the task suspended and
        // keep the reservation until that guaranteed publish arrives.
        harness.runtime.requestActorOperationCancel(zone_key(1));
        assert(harness.runtime.tryPost(harness.binding.post(2, 24, Behavior::NoAwait)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));
        assert(harness.binding.outcomes().front().value == 24);
        const auto waiting_stats = harness.workerStats(zone_key(1));
        assert(waiting_stats.suspended_task_count == 1);
        assert(waiting_stats.in_flight_operations == 1);
        assert(waiting_stats.cancelled_operations == 0);

        harness.state->claim_window_gate->release_result_store.store(true, std::memory_order_release);
        completer.join();
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 2; }));
        const auto outcomes = harness.binding.outcomes();
        assert(outcomes[1].kind == OutcomeKind::Completed);
        assert(outcomes[1].value == 230);

        harness.runtime.close();
        harness.runtime.join();
        const auto final_stats = harness.workerStats(zone_key(1));
        assert(final_stats.in_flight_operations == 0);
        assert(final_stats.cancelled_operations == 0);
        assert(harness.completion.drained_count.load() == 1);
    }

    void test_completion_and_cancel_race_produce_exactly_one_terminal()
    {
        for (int attempt = 0; attempt < 40; ++attempt)
        {
            Harness harness{1};

            assert(harness.runtime.tryPost(harness.binding.post(1, 13, Behavior::AwaitOnce)) == PostResult::Accepted);
            assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

            std::jthread canceller{[&harness] { harness.runtime.requestActorOperationCancel(zone_key(1)); }};
            harness.service.completeAll();
            canceller.join();

            assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));
            const auto outcomes = harness.binding.outcomes();
            assert(outcomes.size() == 1);
            const bool completed = outcomes.front().kind == OutcomeKind::Completed && outcomes.front().value == 130;
            const bool cancelled = outcomes.front().kind == OutcomeKind::Cancelled;
            assert(completed != cancelled);

            harness.runtime.close();
            harness.runtime.join();
            assert(harness.completion.drained_count.load() == 1);

            const auto stats = harness.workerStats(zone_key(1));
            assert(stats.double_completions == 0);
            assert(stats.processed == 1);
            assert(stats.in_flight_operations == 0);
            assert(stats.queue_depth == 0);
        }
    }

    void test_graceful_close_waits_for_the_last_continuation()
    {
        for (int attempt = 0; attempt < 20; ++attempt)
        {
            Harness harness{1};

            assert(harness.runtime.tryPost(harness.binding.post(1, 14, Behavior::AwaitOnce)) == PostResult::Accepted);
            assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

            // A graceful close only shuts the external ingress. An operation that
            // was already admitted still has to reach its terminal outcome.
            std::jthread closer{[&harness] { harness.runtime.close(); }};
            harness.service.completeAll();
            closer.join();

            harness.runtime.join();
            assert(harness.completion.drained_count.load() == 1);
            assert(harness.binding.outcomes().size() == 1);
            assert(harness.binding.outcomes().front().kind == OutcomeKind::Completed);

            const auto stats = harness.workerStats(zone_key(1));
            assert(stats.processed == 1);
            assert(stats.in_flight_operations == 0);
        }
    }

    void test_handler_exception_before_and_after_a_suspension_reaches_the_worker()
    {
        {
            Harness harness{1};
            assert(harness.runtime.tryPost(harness.binding.post(1, 1, Behavior::ThrowBeforeAwait)) == PostResult::Accepted);

            bool threw = false;
            try
            {
                harness.runtime.join();
            }
            catch (const std::runtime_error&)
            {
                threw = true;
            }
            assert(threw);
            assert(harness.completion.failed_count.load() == 1);
        }

        {
            Harness harness{1};
            assert(harness.runtime.tryPost(harness.binding.post(1, 1, Behavior::ThrowAfterAwait)) == PostResult::Accepted);
            assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));
            harness.service.completeAll();

            bool threw = false;
            try
            {
                harness.runtime.join();
            }
            catch (const std::runtime_error&)
            {
                threw = true;
            }
            assert(threw);
            assert(harness.completion.failed_count.load() == 1);
        }
    }

    void test_submit_failure_rolls_back_the_reservation()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 1, Behavior::ThrowOnSubmit)) == PostResult::Accepted);

        bool threw = false;
        try
        {
            harness.runtime.join();
        }
        catch (const std::runtime_error&)
        {
            threw = true;
        }

        assert(threw);
        // The operation never started, so its reservation must not stay held.
        const auto stats = harness.workerStats(zone_key(1));
        assert(stats.in_flight_operations == 0);
        assert(stats.reservation_rejections == 0);
        assert(stats.queue_depth == 0);
    }

    void test_worker_failure_cancels_a_suspended_task_on_another_actor()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 15, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        // Actor 2 fails the Worker while actor 1 is still suspended. The failing
        // Worker never returns to its pump, so it has to transition and destroy
        // actor 1's task itself.
        assert(harness.runtime.tryPost(harness.binding.post(2, 1, Behavior::ThrowBeforeAwait)) == PostResult::Accepted);

        bool threw = false;
        try
        {
            harness.runtime.join();
        }
        catch (const std::runtime_error&)
        {
            threw = true;
        }

        assert(threw);
        assert(harness.completion.failed_count.load() == 1);
        assert(harness.state->affinity.frame_destroyed.load() == harness.state->affinity.dispatched.load());

        // A completion arriving after the failure has nothing left to resume.
        harness.service.completeAll();
    }

    void test_worker_failure_wakes_another_worker_to_clean_up_its_own_task()
    {
        Harness harness{2};
        const EntityId suspended_entity = entity_for_worker(harness.runtime, 0);
        const EntityId failing_entity = entity_for_worker(harness.runtime, 1);

        assert(harness.runtime.tryPost(harness.binding.post(suspended_entity, 21, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        // Worker 1 fails while Worker 0 is asleep with a suspended task. The
        // failure path may only request global cancellation; Worker 0 must wake,
        // claim cancellation, and destroy its own frame itself.
        assert(harness.runtime.tryPost(harness.binding.post(failing_entity, 22, Behavior::ThrowBeforeAwait)) == PostResult::Accepted);

        bool threw = false;
        try
        {
            harness.runtime.join();
        }
        catch (const std::runtime_error&)
        {
            threw = true;
        }

        assert(threw);
        assert(harness.completion.failed_count.load() == 1);
        const auto suspended_worker_stats = harness.workerStats(zone_key(suspended_entity));
        assert(suspended_worker_stats.in_flight_operations == 0);
        assert(suspended_worker_stats.queue_depth == 0);
        assert(suspended_worker_stats.cancelled_operations == 1);

        // The external operation may outlive both Workers, but its late completion
        // cannot resume the destroyed frame.
        harness.service.completeAll();
    }

    void test_hard_cancel_releases_a_suspended_command()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 16, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        harness.runtime.cancel();
        harness.runtime.join();

        const auto stats = harness.workerStats(zone_key(1));
        assert(stats.in_flight_operations == 0);
        assert(stats.queue_depth == 0);
        assert(stats.cancelled_operations == 1);
        assert(harness.state->affinity.payload_value_at_frame_destruction.load() == 16);
        // The task was abandoned rather than resumed, so no outcome was recorded.
        assert(harness.binding.outcomes().empty());
        assert(harness.completion.drained_count.load() == 0);

        harness.service.completeAll();
    }

    void test_hard_cancel_consumes_a_completion_that_already_won_terminal_claim()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 19, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));

        // Keep the owning Worker inside another Actor while actor 1's completion
        // claims terminal and publishes. Hard cancel must consume that reserved
        // continuation instead of destroying the frame and leaking the queue item.
        assert(harness.runtime.tryPost(harness.binding.post(2, 20, Behavior::BlockWorker)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.state->block_started.load(std::memory_order_acquire); }));

        harness.service.completeAll();
        assert(harness.workerStats(zone_key(1)).continuation_queue_depth == 1);

        harness.runtime.cancel();
        harness.state->release_block.store(true, std::memory_order_release);
        harness.runtime.join();

        const auto stats = harness.workerStats(zone_key(1));
        assert(stats.in_flight_operations == 0);
        assert(stats.continuation_queue_depth == 0);
        assert(stats.queue_depth == 0);
        assert(harness.binding.outcomes().size() == 1);
        assert(harness.binding.outcomes().front().value == 20);
    }

    void test_completion_outliving_the_runtime_is_discarded()
    {
        TestAsyncService service;
        auto state = std::make_shared<AwaitingBinding::State>();

        {
            RecordingRuntimeCompletion completion;
            AwaitingBinding binding{service, state};
            ActorRuntime runtime{Harness::makeConfig(1, 8), completion};
            runtime.registerBinding(binding);
            runtime.start();

            assert(runtime.tryPost(binding.post(1, 17, Behavior::AwaitOnce)) == PostResult::Accepted);
            assert(wait_until([&service] { return service.pendingCount() == 1; }));

            runtime.cancel();
            runtime.join();
        }

        // The producer outlived the runtime, which is exactly what the coroutine
        // contract allows for a blocking operation. Its endpoint is deactivated,
        // so completing now must be a safe no-op rather than a dangling write.
        service.completeAll();
        assert(state->outcomes.empty());
    }

    void test_scheduler_passivatable_count_excludes_a_suspended_actor()
    {
        Harness harness{1};

        assert(harness.runtime.tryPost(harness.binding.post(1, 18, Behavior::AwaitOnce)) == PostResult::Accepted);
        assert(wait_until([&harness] { return harness.service.pendingCount() == 1; }));
        assert(harness.workerStats(zone_key(1)).scheduler_passivatable_actor_count == 0);

        harness.service.completeAll();
        assert(wait_until([&harness] { return harness.binding.outcomes().size() == 1; }));
        assert(wait_until([&harness] { return harness.workerStats(zone_key(1)).scheduler_passivatable_actor_count == 1; }));

        harness.runtime.close();
        harness.runtime.join();
    }
}

void run_actor_coroutine_tests()
{
    test_suspend_lets_other_actors_run_and_queues_the_same_actor();
    test_resume_and_frame_destruction_stay_on_the_owning_worker();
    test_immediate_completion_resumes_through_the_worker();
    test_sequential_awaits_keep_one_command_accounting();
    test_reservation_saturation_rejects_before_the_operation_starts();
    test_double_completion_is_counted_and_applied_once();
    test_producer_failure_propagates_into_the_handler();
    test_per_operation_cancel_resumes_the_handler_with_a_cancellation();
    test_cancel_losing_the_claim_waits_for_the_guaranteed_publish();
    test_cancel_waits_when_completion_claimed_but_has_not_published_yet();
    test_completion_and_cancel_race_produce_exactly_one_terminal();
    test_graceful_close_waits_for_the_last_continuation();
    test_handler_exception_before_and_after_a_suspension_reaches_the_worker();
    test_submit_failure_rolls_back_the_reservation();
    test_worker_failure_cancels_a_suspended_task_on_another_actor();
    test_worker_failure_wakes_another_worker_to_clean_up_its_own_task();
    test_hard_cancel_releases_a_suspended_command();
    test_hard_cancel_consumes_a_completion_that_already_won_terminal_claim();
    test_completion_outliving_the_runtime_is_discarded();
    test_scheduler_passivatable_count_excludes_a_suspended_actor();
}
