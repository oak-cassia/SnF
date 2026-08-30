#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/actor.hpp"
#include "snf/worker/actor_table.hpp"
#include "snf/worker/worker.hpp"
#include "snf/worker/worker_group.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <utility>
#include <vector>

namespace snf::worker
{
    struct WorkerActorTestAccess
    {
        static void runReadyActors(Worker& worker, const CountTimeBudget& budget)
        {
            worker.runReadyActors(budget);
        }

        static bool tryMarkSyntheticCommandReady(Worker& worker, const AwaitKey key, const SyntheticAwaitOutcome outcome)
        {
            return worker.tryMarkSyntheticCommandReady(key, outcome);
        }

        static bool completeSyntheticCommand(Worker& worker, const AwaitKey key, const SyntheticAwaitOutcome outcome)
        {
            return worker.completeSyntheticCommand(key, outcome);
        }

        static DeliveryResult beginActivationLoad(Worker& worker, const ActorKey key, ActorEnvelope&& first_message)
        {
            return worker.beginActivationLoad(key, std::move(first_message));
        }

        static MailboxUsage discardMailbox(Worker& worker, const ActorKey key)
        {
            auto* slot = worker._actors->find(key);
            assert(slot != nullptr);
            return worker.discardMailbox(*slot);
        }

        static void completeSyntheticActivation(Worker& worker, const AwaitKey key, const SyntheticActivationOutcome outcome)
        {
            worker.completeSyntheticActivation(key, outcome);
        }

        static const std::optional<BlockedTask>& blocked(Worker& worker, const ActorKey key)
        {
            auto* slot = worker._actors->find(key);
            assert(slot != nullptr);
            return slot->blocked();
        }

        static ActorSlot* slot(Worker& worker, const ActorKey key)
        {
            return worker._actors->find(key);
        }

        static ActorHandle handle(Worker& worker, const ActorKey key)
        {
            auto* slot = worker._actors->find(key);
            assert(slot != nullptr);
            return slot->handle();
        }

        static void expireTimers(Worker& worker, const TimePoint now, const CountTimeBudget& budget)
        {
            worker.expireTimers(now, budget);
        }

        static void drainInbox(Worker& worker, const InboxBudget& budget)
        {
            worker.drainInbox(budget);
        }

        static PollEvent installReadableConnection(Worker& worker, snf::net::UniqueFileDescriptor socket)
        {
            const int descriptor = socket.getDescriptor();
            auto connection = worker._connections->tryReserve(std::move(socket), worker._id);
            assert(connection.has_value());
            const ConnectionHandle handle = connection->handle();

            auto registration = worker._registrations->tryReserve(descriptor, PollTargetKind::ClientConnection, handle);
            assert(registration.has_value());
            const PollToken token = registration->token();
            const PollRegistrationHandle registration_handle = registration->handle();

            registration->commit();
            connection->commit();
            worker._connection_registrations[handle.id.value] = registration_handle;

            return PollEvent{.token = token, .readable = true};
        }

        static void beginShutdownPhaseA(Worker& worker)
        {
            worker.beginShutdownPhaseA();
        }

        static void processPollEvents(Worker& worker, const std::span<const PollEvent> events, const IoBudget& budget)
        {
            worker.processPollEvents(events, budget);
        }

        static bool readWorkQueueEmpty(const Worker& worker)
        {
            return worker._read_work_queue == nullptr || worker._read_work_queue->empty();
        }

        static void closeInbox(Worker& worker)
        {
            worker._inbox.close();
        }

        static void removeActor(Worker& worker, const ActorHandle handle, const ActorRemovalReason reason)
        {
            worker.removeActor(handle, reason);
        }
    };

    struct WorkerGroupTestAccess
    {
        static Worker& worker(WorkerGroup& group, const std::size_t index)
        {
            return *group._workers[index];
        }
    };
}

namespace
{
    using namespace snf::worker;
    using namespace std::chrono_literals;
    using snf::protocol::Frame;
    using snf::protocol::MessageType;

    using Clock = std::chrono::steady_clock;

    struct SocketPair
    {
        snf::net::UniqueFileDescriptor left;
        snf::net::UniqueFileDescriptor right;
    };

    [[nodiscard]] SocketPair makeSocketPair()
    {
        int descriptors[2]{};
        assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, descriptors) == 0);
        return SocketPair{snf::net::UniqueFileDescriptor{descriptors[0]}, snf::net::UniqueFileDescriptor{descriptors[1]}};
    }

    [[nodiscard]] Frame makeFrame(const std::size_t payload_size, const MessageType type = MessageType::Ping, const std::uint32_t req_id = 77)
    {
        return Frame{
            .type = type,
            .request_id = req_id,
            .payload = std::vector<std::byte>(payload_size, std::byte{0x5A}),
        };
    }

    struct TestPingPayload
    {
        std::optional<ConnectionRef> connection{};
        Frame frame{};
        std::uint64_t explicit_charge{0};
    };
}

namespace snf::worker
{
    template <> struct ActorPayloadTraits<TestPingPayload>
    {
        static constexpr std::uint32_t TAG = 1;
        static std::uint64_t calculateCharge(const TestPingPayload& p) noexcept
        {
            const auto frame_bytes =
                static_cast<std::uint64_t>(p.frame.payload.size()) + snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE;
            return std::max<std::uint64_t>(p.explicit_charge, frame_bytes);
        }
    };
}

namespace
{
    using TestActorPayloadRegistry = snf::worker::ActorPayloadRegistry<TestPingPayload>;

    [[nodiscard]] ActorEnvelope makeEnvelope(
        const std::size_t payload_size = 16,
        const MessageType type = MessageType::Ping,
        const std::uint32_t req_id = 1
    )
    {
        return TestActorPayloadRegistry::create(TestPingPayload{
            .connection = std::nullopt,
            .frame = makeFrame(payload_size, type, req_id),
            .explicit_charge = 0,
        });
    }

    [[nodiscard]] ActorEnvelope makeEnvelopeWithCharge(const std::size_t payload_size, const std::uint64_t explicit_charge)
    {
        return TestActorPayloadRegistry::create(TestPingPayload{
            .connection = std::nullopt,
            .frame = makeFrame(payload_size, MessageType::Ping, 1),
            .explicit_charge = explicit_charge,
        });
    }

    class CountingRequestSink final : public RequestSink
    {
    public:
        [[nodiscard]] RequestPostResult tryPost(ConnectionRef, Frame&&) override
        {
            ++posts;
            return RequestPostResult::Accepted;
        }

        std::atomic<std::size_t> posts{0};
    };

    struct ShutdownActorGate
    {
        std::mutex mutex;
        std::condition_variable changed;
        bool source_dispatch_started{false};
        bool release_source{false};
        std::atomic<std::size_t> target_turns{0};
    };

    struct SuspensionObservation
    {
        std::mutex mutex;
        std::condition_variable changed;
        std::atomic<std::size_t> suspended{0};
        std::atomic<std::size_t> cancelled{0};
    };

    class FunctionalActor final : public ActorInstance
    {
    public:
        using DispatchFn = std::function<TurnResult(ActorEnvelope&&, const ActorTurnContext&)>;

        explicit FunctionalActor(DispatchFn fn)
            : _fn(std::move(fn))
        {
        }

        TurnResult dispatch(ActorEnvelope&& envelope, const ActorTurnContext& context) override
        {
            ++reentrancy_depth;
            if (reentrancy_depth > max_reentrancy_depth)
            {
                max_reentrancy_depth = reentrancy_depth;
            }

            TurnResult res;
            if (_fn)
            {
                res = _fn(std::move(envelope), context);
            }
            else
            {
                res = CompletedTurn{.effects = EffectBatch{}};
            }

            --reentrancy_depth;
            ++turns_executed;
            return res;
        }

        int reentrancy_depth{0};
        int max_reentrancy_depth{0};
        std::size_t turns_executed{0};

    private:
        DispatchFn _fn;
    };

    class FunctionalActorFactory final : public ActorFactory
    {
    public:
        using ConstructFn = std::function<ActorConstructionResult(ActorKey)>;

        explicit FunctionalActorFactory(ConstructFn fn)
            : _fn(std::move(fn))
        {
        }

        ActorConstructionResult construct(const ActorKey key) override
        {
            ++construct_calls;
            last_key = key;
            if (_fn)
            {
                return _fn(key);
            }
            return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(nullptr));
        }

        std::size_t construct_calls{0};
        std::optional<ActorKey> last_key{};

    private:
        ConstructFn _fn;
    };

    // =========================================================================
    // 1. Core and Construction Tests
    // =========================================================================

    void test_actor_table_hard_cap_and_reservation_rollback()
    {
        ActorTable table(2);
        assert(table.capacity() == 2);
        assert(table.hasCapacity());
        assert(table.activeCount() == 0);
        assert(table.reservedCount() == 0);

        const ActorKey key1{.kind = ActorKind::Player, .entity = 101};
        const ActorKey key2{.kind = ActorKind::Player, .entity = 102};
        const ActorKey key3{.kind = ActorKind::Player, .entity = 103};

        auto res1 = table.tryReserve(key1);
        assert(res1.has_value());
        assert(table.reservedCount() == 1);
        assert(!table.tryReserve(key1).has_value()); // duplicate key rejected

        auto res2 = table.tryReserve(key2);
        assert(res2.has_value());
        assert(table.reservedCount() == 2);
        assert(!table.hasCapacity());

        // Over-capacity rejected
        assert(!table.tryReserve(key3).has_value());

        // Rollback res2
        res2->rollback();
        assert(table.reservedCount() == 1);
        assert(table.hasCapacity());

        // Now key3 can be reserved
        auto res3 = table.tryReserve(key3);
        assert(res3.has_value());

        res1->commit();
        res3->commit();
        assert(table.activeCount() == 2);
        assert(table.reservedCount() == 0);

        assert(table.find(key1) != nullptr);
        assert(table.find(key2) == nullptr);
        assert(table.find(key3) != nullptr);
    }

    void test_incarnation_never_reused_after_rollback()
    {
        ActorTable table(1);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        auto res1 = table.tryReserve(key);
        assert(res1.has_value());
        const ActorIncarnation inc1 = res1->handle().incarnation;
        assert(inc1.isValid());
        res1->rollback();

        auto res2 = table.tryReserve(key);
        assert(res2.has_value());
        const ActorIncarnation inc2 = res2->handle().incarnation;
        assert(inc2.isValid());
        assert(inc2.value > inc1.value); // Incarnation strictly monotonic, rollback does not rewind sequence!

        res2->commit();
        const ActorHandle handle2 = res2->handle();
        assert(table.find(handle2) != nullptr);

        const ActorHandle stale_handle{.slot_index = handle2.slot_index, .incarnation = inc1};
        assert(table.find(stale_handle) == nullptr); // Stale handle lookup fails
    }

    void test_stale_actor_handle_detection()
    {
        ActorTable table(1);
        const ActorKey key1{.kind = ActorKind::Player, .entity = 1};
        const ActorKey key2{.kind = ActorKind::Player, .entity = 2};

        auto res1 = table.tryReserve(key1);
        const ActorHandle handle1 = res1->handle();
        res1->commit();

        assert(table.release(handle1));
        assert(table.find(handle1) == nullptr);

        auto res2 = table.tryReserve(key2);
        const ActorHandle handle2 = res2->handle();
        res2->commit();

        // Same slot index, different incarnation
        assert(handle1.slot_index == handle2.slot_index);
        assert(handle1.incarnation.value != handle2.incarnation.value);

        assert(table.find(handle1) == nullptr);
        assert(table.find(handle2) != nullptr);
    }

    void test_actor_mailbox_limits_per_actor_and_worker_total()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 2,
            .max_mailbox_bytes_per_actor = 100,
            .max_mailbox_messages_total = 3,
            .max_mailbox_bytes_total = 150,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        FunctionalActorFactory factory(nullptr);
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);

        const ActorKey key1{.kind = ActorKind::Player, .entity = 1};
        const ActorKey key2{.kind = ActorKind::Player, .entity = 2};

        // 1st message to actor1 (40 bytes charged)
        assert(worker.tryDeliverLocal(key1, makeEnvelope(40)) == DeliveryResult::Accepted);
        assert(worker.totalMailboxMessages() == 1);

        // 2nd message to actor1 (40 bytes charged)
        assert(worker.tryDeliverLocal(key1, makeEnvelope(40)) == DeliveryResult::Accepted);
        assert(worker.totalMailboxMessages() == 2);

        // 3rd message to actor1 fails due to per-actor message count limit (limit is 2)
        assert(worker.tryDeliverLocal(key1, makeEnvelope(10)) == DeliveryResult::MailboxFull);

        // 1st message to actor2 (40 bytes charged) -> reaches worker total message limit (3)
        assert(worker.tryDeliverLocal(key2, makeEnvelope(40)) == DeliveryResult::Accepted);
        assert(worker.totalMailboxMessages() == 3);

        // Next message to actor2 fails due to worker total message limit (limit is 3)
        assert(worker.tryDeliverLocal(key2, makeEnvelope(10)) == DeliveryResult::MailboxFull);
    }

    void test_actor_mailbox_byte_limit_cannot_be_underreported()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 2,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 32,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 32,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        FunctionalActorFactory factory(nullptr);
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        ActorEnvelope underreported = makeEnvelopeWithCharge(64, 1);
        assert(underreported.chargedBytes() > config.max_mailbox_bytes_per_actor);
        assert(worker.tryDeliverLocal(key, std::move(underreported)) == DeliveryResult::MailboxFull);
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.totalMailboxBytes() == 0);
    }

    void test_actor_construction_rejected_rollback_and_retry()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 2,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        bool allow_construct = false;
        FunctionalActorFactory factory(
            [&allow_construct](ActorKey) -> ActorConstructionResult
            {
                if (!allow_construct)
                {
                    return ActorConstructionResult::rejected();
                }
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(nullptr));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // 1st delivery rejected by factory
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::ConstructionRejected);
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.totalMailboxBytes() == 0);
        assert(worker.metrics().actor.construction_rejections == 1);

        // Allow construct and retry
        allow_construct = true;
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.actorCount() == 1);
        assert(worker.totalMailboxMessages() == 1);
    }

    void test_actor_factory_exception_rollback_and_fail_fast()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 2,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        FunctionalActorFactory factory(
            [](ActorKey) -> ActorConstructionResult
            {
                throw std::runtime_error{"Simulated factory defect"};
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        bool caught = false;
        try
        {
            static_cast<void>(worker.tryDeliverLocal(key, makeEnvelope(16)));
        }
        catch (const std::runtime_error& err)
        {
            caught = true;
            assert(std::string(err.what()) == "Simulated factory defect");
        }
        assert(caught);

        // Invariant: slots and mailbox accounting are cleanly rolled back
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.totalMailboxBytes() == 0);
    }

    void test_actor_factory_invalid_ready_result_rolls_back_and_fails_fast()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 2,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        FunctionalActorFactory factory(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult{
                    .status = ActorConstructionResult::Status::Ready,
                    .instance = nullptr,
                };
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        bool caught = false;
        try
        {
            static_cast<void>(worker.tryDeliverLocal(key, makeEnvelope(16)));
        }
        catch (const std::logic_error&)
        {
            caught = true;
        }
        assert(caught);
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.totalMailboxBytes() == 0);
    }

    void test_missing_actor_first_message_exact_once()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 2,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        std::vector<uint32_t> received_requests;
        FunctionalActorFactory factory(
            [&received_requests](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [&received_requests](ActorEnvelope&& env, const ActorTurnContext&) -> TurnResult
                    {
                        received_requests.push_back(env.get<TestPingPayload>().frame.request_id);
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16, MessageType::Ping, 1001)) == DeliveryResult::Accepted);
        assert(worker.actorCount() == 1);
        assert(worker.totalMailboxMessages() == 1);

        // Run worker loop iteration to process actor turn
        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(20ms);
        worker.requestStop();
        th.join();

        assert(received_requests.size() == 1);
        assert(received_requests[0] == 1001);
    }

    void test_same_actor_key_construct_called_once()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        FunctionalActorFactory factory(nullptr);
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);

        assert(factory.construct_calls == 1);
    }

    // =========================================================================
    // 2. Turn and Scheduling Tests
    // =========================================================================

    void test_ready_actor_queue_bounded_and_fifo()
    {
        ReadyActorQueue queue(3);
        assert(queue.capacity() == 3);
        assert(queue.empty());

        const ActorHandle h1{.slot_index = 0, .incarnation = ActorIncarnation{1}};
        const ActorHandle h2{.slot_index = 1, .incarnation = ActorIncarnation{2}};
        const ActorHandle h3{.slot_index = 2, .incarnation = ActorIncarnation{3}};

        queue.push(h1);
        queue.push(h2);
        queue.push(h3);
        assert(queue.full());

        bool threw = false;
        try
        {
            queue.push(ActorHandle{.slot_index = 3, .incarnation = ActorIncarnation{4}});
        }
        catch (const std::logic_error&)
        {
            threw = true;
        }
        assert(threw);

        assert(queue.pop() == h1);
        assert(queue.pop() == h2);
        assert(queue.pop() == h3);
        assert(queue.empty());
    }

    void test_actor_slice_limit_max_30_turns()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 100,
            .max_mailbox_bytes_per_actor = 1024 * 1024,
            .max_mailbox_messages_total = 100,
            .max_mailbox_bytes_total = 1024 * 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        FunctionalActorFactory factory(nullptr);
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // Deliver 35 messages to a single actor
        for (std::size_t i = 0; i < 35; ++i)
        {
            assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        }
        assert(worker.totalMailboxMessages() == 35);

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(30ms);
        worker.requestStop();
        th.join();

        // All 35 messages processed across multiple slices
        assert(worker.metrics().actor.actor_turns == 35);
        assert(worker.totalMailboxMessages() == 0);
    }

    void test_fair_scheduling_hot_31_cold_1()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 100,
            .max_mailbox_bytes_per_actor = 1024 * 1024,
            .max_mailbox_messages_total = 100,
            .max_mailbox_bytes_total = 1024 * 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        std::vector<std::string> execution_order;
        std::mutex mtx;

        FunctionalActorFactory factory(
            [&execution_order, &mtx](ActorKey key) -> ActorConstructionResult
            {
                const std::string name = (key.entity == 1) ? "hot" : "cold";
                auto actor = std::make_unique<FunctionalActor>(
                    [name, &execution_order, &mtx](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        std::lock_guard lock{mtx};
                        execution_order.push_back(name);
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey hot_key{.kind = ActorKind::Player, .entity = 1};
        const ActorKey cold_key{.kind = ActorKind::Player, .entity = 2};

        // Enqueue 31 messages to hot_key
        for (std::size_t i = 0; i < 31; ++i)
        {
            assert(worker.tryDeliverLocal(hot_key, makeEnvelope(16)) == DeliveryResult::Accepted);
        }
        // Enqueue 1 message to cold_key
        assert(worker.tryDeliverLocal(cold_key, makeEnvelope(16)) == DeliveryResult::Accepted);

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(40ms);
        worker.requestStop();
        th.join();

        std::lock_guard lock{mtx};
        assert(execution_order.size() == 32);

        // First 30 turns must be "hot"
        for (std::size_t i = 0; i < 30; ++i)
        {
            assert(execution_order[i] == "hot");
        }
        // 31st turn must be "cold" (fair interleaving after slice limit 30)
        assert(execution_order[30] == "cold");
        // 32nd turn must be the remaining "hot"
        assert(execution_order[31] == "hot");
    }

    void test_duplicate_ready_entry_prevented()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        FunctionalActorFactory factory(nullptr);
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // Delivering 3 messages to an Idle actor -> Queued state -> ready queue has exactly 1 entry
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);

        // When loop runs, all 3 messages are processed in 1 slice
        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(20ms);
        worker.requestStop();
        th.join();

        assert(worker.metrics().actor.actor_turns == 3);
    }

    void test_non_reentrancy_self_tell_queued_not_inline()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        std::atomic<int> current_depth{0};
        std::atomic<int> max_depth{0};
        std::atomic<std::size_t> turns{0};

        FunctionalActorFactory factory(
            [&current_depth, &max_depth, &turns](ActorKey key) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [key, &current_depth, &max_depth, &turns](ActorEnvelope&& env, const ActorTurnContext&) -> TurnResult
                    {
                        const int depth = ++current_depth;
                        int prev_max = max_depth.load();
                        while (depth > prev_max && !max_depth.compare_exchange_weak(prev_max, depth))
                        {
                        }

                        EffectBatch batch;
                        if (env.get<TestPingPayload>().frame.request_id == 1)
                        {
                            // Self tell: send request 2 to self
                            batch.push(TellActorEffect{
                                .target = key,
                                .message = makeEnvelope(16, MessageType::Ping, 2),
                            });
                        }

                        --current_depth;
                        turns.fetch_add(1);
                        return CompletedTurn{.effects = std::move(batch)};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16, MessageType::Ping, 1)) == DeliveryResult::Accepted);

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(30ms);
        worker.requestStop();
        th.join();

        assert(turns.load() == 2);
        // Invariant: handler reentrancy depth is strictly 1 (INV-03, INV-04)
        assert(max_depth.load() == 1);
    }

    // =========================================================================
    // 3. Effect Application Tests
    // =========================================================================

    void test_effect_batch_hard_cap_64_throws_on_65()
    {
        EffectBatch batch;
        assert(batch.empty());
        assert(batch.size() == 0);

        for (std::size_t i = 0; i < 64; ++i)
        {
            assert(batch.tryPush(StopActorEffect{}));
        }
        assert(batch.size() == 64);
        assert(!batch.tryPush(StopActorEffect{}));

        bool threw = false;
        try
        {
            batch.push(StopActorEffect{});
        }
        catch (const std::overflow_error&)
        {
            threw = true;
        }
        assert(threw);
    }

    void test_stop_effect_rejects_subsequent_self_tell_but_applies_send()
    {
        WorkerNetworkConfig net_config{
            .table = {.capacity = 10},
        };
        NullRequestSink sink;

        WorkerActorConfig actor_config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        auto socket_pair = makeSocketPair();
        ConnectionRef conn_ref{ConnectionId{0}, ConnectionGeneration{1}, WorkerId{0}};

        FunctionalActorFactory factory(
            [conn_ref](ActorKey key) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [key, conn_ref](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        EffectBatch batch;
                        // 1. Stop actor
                        batch.push(StopActorEffect{});
                        // 2. Self tell (should be rejected since actor is now Stopping)
                        batch.push(TellActorEffect{
                            .target = key,
                            .message = makeEnvelope(16, MessageType::Ping, 99),
                        });
                        // 3. Send frame (should succeed even after stop effect)
                        batch.push(SendFrameEffect{
                            .connection = conn_ref,
                            .frame = makeFrame(8, MessageType::Pong, 77),
                            .critical = false,
                        });
                        return CompletedTurn{.effects = std::move(batch)};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, net_config, sink, actor_config, factory);
        // Reserve connection 0 so conn_ref matches
        // Worker::configureNetwork initialized ConnectionTable
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(30ms);
        worker.requestStop();
        th.join();

        // Self tell failed because actor was Stopping
        assert(worker.metrics().actor.effect_tell_failures == 1);
        // Actor was stopped and removed
        assert(worker.metrics().actor.stopped_actors == 1);
        assert(worker.actorCount() == 0);
    }

    void test_wrong_owner_tell_fails_explicitly_without_remote_transport()
    {
        WorkerActorConfig actor_config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        FunctionalActorFactory factory(nullptr);
        // Worker count = 2. WorkerId = 0.
        Worker worker(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, actor_config, factory);

        // Find a key that belongs to Worker 1
        ActorKey remote_key{.kind = ActorKind::Player, .entity = 0};
        while (ownerOf(remote_key, 2, 0) == WorkerId{0})
        {
            remote_key.entity += 1;
        }
        assert(ownerOf(remote_key, 2, 0) == WorkerId{1});

        // tryDeliverLocal on wrong owner returns WrongOwner and increments metric
        assert(worker.tryDeliverLocal(remote_key, makeEnvelope(16)) == DeliveryResult::WrongOwner);
        assert(worker.metrics().actor.wrong_owner_tells == 1);
    }

    void test_stale_connection_send_close_continue_subsequent_effects()
    {
        WorkerActorConfig actor_config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        const ConnectionRef stale_conn{ConnectionId{99}, ConnectionGeneration{99}, WorkerId{0}};
        const ActorKey peer_key{.kind = ActorKind::Player, .entity = 2};
        std::atomic<bool> peer_executed{false};

        FunctionalActorFactory factory(
            [stale_conn, peer_key, &peer_executed](ActorKey key) -> ActorConstructionResult
            {
                if (key.entity == 1)
                {
                    auto actor = std::make_unique<FunctionalActor>(
                        [stale_conn, peer_key](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                        {
                            EffectBatch batch;
                            // 1. Send to stale connection (fails)
                            batch.push(SendFrameEffect{.connection = stale_conn, .frame = makeFrame(8)});
                            // 2. Close stale connection (fails)
                            batch.push(CloseConnectionEffect{.connection = stale_conn});
                            // 3. Tell peer actor (should continue and succeed!)
                            batch.push(TellActorEffect{.target = peer_key, .message = makeEnvelope(16)});
                            return CompletedTurn{.effects = std::move(batch)};
                        }
                    );
                    return ActorConstructionResult::ready(std::move(actor));
                }
                else
                {
                    auto actor = std::make_unique<FunctionalActor>(
                        [&peer_executed](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                        {
                            peer_executed.store(true, std::memory_order_release);
                            return CompletedTurn{.effects = EffectBatch{}};
                        }
                    );
                    return ActorConstructionResult::ready(std::move(actor));
                }
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, actor_config, factory);
        const ActorKey key1{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key1, makeEnvelope(16)) == DeliveryResult::Accepted);

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(30ms);
        worker.requestStop();
        th.join();

        assert(peer_executed.load(std::memory_order_acquire));
        assert(worker.metrics().actor.effect_send_failures == 1);
        assert(worker.metrics().actor.effect_close_failures == 1);
    }

    // =========================================================================
    // 4. Shutdown Tests
    // =========================================================================

    void test_shutdown_single_absolute_deadline_unification()
    {
        WorkerActorConfig actor_config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 200ms, // 200ms hard deadline
        };

        FunctionalActorFactory factory(nullptr);
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, actor_config, factory);

        const auto start = Clock::now();
        std::thread th(
            [&]()
            {
                worker.run();
            }
        );

        std::this_thread::sleep_for(10ms);
        worker.requestStop();
        th.join();

        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        // Shutdown should finish well within a reasonable tolerance around the deadline
        assert(elapsed < 400ms);
    }

    void test_shutdown_external_deliver_closed_internal_tell_allowed()
    {
        WorkerActorConfig actor_config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 500ms,
        };

        const ActorKey peer_key{.kind = ActorKind::Player, .entity = 2};
        std::atomic<bool> peer_received{false};

        FunctionalActorFactory factory(
            [peer_key, &peer_received](ActorKey key) -> ActorConstructionResult
            {
                if (key.entity == 1)
                {
                    auto actor = std::make_unique<FunctionalActor>(
                        [peer_key](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                        {
                            EffectBatch batch;
                            batch.push(TellActorEffect{
                                .target = peer_key,
                                .message = makeEnvelope(16, MessageType::Ping, 42),
                            });
                            return CompletedTurn{.effects = std::move(batch)};
                        }
                    );
                    return ActorConstructionResult::ready(std::move(actor));
                }
                else
                {
                    auto actor = std::make_unique<FunctionalActor>(
                        [&peer_received](ActorEnvelope&& env, const ActorTurnContext&) -> TurnResult
                        {
                            if (env.get<TestPingPayload>().frame.request_id == 42)
                            {
                                peer_received.store(true, std::memory_order_release);
                            }
                            return CompletedTurn{.effects = EffectBatch{}};
                        }
                    );
                    return ActorConstructionResult::ready(std::move(actor));
                }
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, actor_config, factory);
        const ActorKey key1{.kind = ActorKind::Player, .entity = 1};

        // Enqueue message to actor 1 before stop
        assert(worker.tryDeliverLocal(key1, makeEnvelope(16)) == DeliveryResult::Accepted);

        // Request stop immediately
        worker.requestStop();

        // External tryDeliverLocal is rejected with Closed after stop requested
        assert(worker.tryDeliverLocal(key1, makeEnvelope(16)) == DeliveryResult::Closed);

        // Run worker shutdown drain
        worker.run();

        // Internal TellActorEffect during quiescence drain succeeded!
        assert(peer_received.load(std::memory_order_acquire));
    }

    void test_nullable_actorslot_commit_instance_contract()
    {
        ActorTable table(1);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        auto res = table.tryReserve(key);
        assert(res.has_value());
        ActorSlot& slot = res->slot();

        // Before setting instance, hasInstance is false (representation exists for Step 5 Loading)
        assert(!slot.hasInstance());
        assert(slot.instance() == nullptr);

        // In Step 4, setting instance is required before commit
        slot.setInstance(std::make_unique<FunctionalActor>(nullptr));
        assert(slot.hasInstance());
        assert(slot.instance() != nullptr);

        res->commit();
        ActorSlot* committed_slot = table.find(key);
        assert(committed_slot != nullptr);
        assert(committed_slot->hasInstance());
        assert(committed_slot->instance() != nullptr);
    }

    void test_actor_phase_max_count_limits_turns()
    {
        WorkerBudgets budgets = WorkerBudgets::defaults();
        budgets.actors.max_count = 5; // Max 5 turns per phase

        WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 100,
            .max_mailbox_bytes_per_actor = 1024 * 1024,
            .max_mailbox_messages_total = 100,
            .max_mailbox_bytes_total = 1024 * 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        std::atomic<std::size_t> turns{0};
        FunctionalActorFactory factory(
            [&turns](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [&turns](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        turns.fetch_add(1);
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, budgets, WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // Enqueue 12 messages
        for (std::size_t i = 0; i < 12; ++i)
        {
            assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        }

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(30ms);
        worker.requestStop();
        th.join();

        // All 12 turns executed, and budget_stops was triggered because 12 > 5
        assert(turns.load() == 12);
        assert(worker.metrics().actor.budget_stops >= 2);
    }

    void test_actor_phase_duration_budget_stops_early()
    {
        WorkerBudgets budgets = WorkerBudgets::defaults();
        budgets.actors.max_count = 100;
        budgets.actors.max_duration = 500us; // 500us duration budget

        WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 100,
            .max_mailbox_bytes_per_actor = 1024 * 1024,
            .max_mailbox_messages_total = 100,
            .max_mailbox_bytes_total = 1024 * 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        std::atomic<std::size_t> turns{0};
        FunctionalActorFactory factory(
            [&turns](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [&turns](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        turns.fetch_add(1);
                        std::this_thread::sleep_for(200us); // Sleep 200us per turn
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, budgets, WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // Enqueue 10 messages
        for (std::size_t i = 0; i < 10; ++i)
        {
            assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        }

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(50ms);
        worker.requestStop();
        th.join();

        assert(turns.load() == 10);
        // Budget stops occurred due to duration exhaustion
        assert(worker.metrics().actor.budget_stops > 0);
        assert(worker.metrics().actor.total_slice_duration_ns > 0);
    }

    void test_all_four_effects_ordering_and_continue_after_failure()
    {
        WorkerNetworkConfig net_config{
            .table = {.capacity = 10},
        };
        NullRequestSink sink;

        WorkerActorConfig actor_config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
        };

        const ActorKey peer_key{.kind = ActorKind::Player, .entity = 2};
        const ConnectionRef stale_conn{ConnectionId{99}, ConnectionGeneration{99}, WorkerId{0}};
        std::atomic<bool> peer_received{false};

        FunctionalActorFactory factory(
            [peer_key, stale_conn, &peer_received](ActorKey key) -> ActorConstructionResult
            {
                if (key.entity == 1)
                {
                    auto actor = std::make_unique<FunctionalActor>(
                        [peer_key, stale_conn](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                        {
                            EffectBatch batch;
                            // 1. SendFrame to stale connection (fails)
                            batch.push(SendFrameEffect{.connection = stale_conn, .frame = makeFrame(8)});
                            // 2. CloseConnection on stale connection (fails)
                            batch.push(CloseConnectionEffect{.connection = stale_conn});
                            // 3. TellActor to peer (succeeds!)
                            batch.push(TellActorEffect{.target = peer_key, .message = makeEnvelope(16, MessageType::Ping, 88)});
                            // 4. StopActor (marks stopping)
                            batch.push(StopActorEffect{});
                            return CompletedTurn{.effects = std::move(batch)};
                        }
                    );
                    return ActorConstructionResult::ready(std::move(actor));
                }
                else
                {
                    auto actor = std::make_unique<FunctionalActor>(
                        [&peer_received](ActorEnvelope&& env, const ActorTurnContext&) -> TurnResult
                        {
                            if (env.get<TestPingPayload>().frame.request_id == 88)
                            {
                                peer_received.store(true, std::memory_order_release);
                            }
                            return CompletedTurn{.effects = EffectBatch{}};
                        }
                    );
                    return ActorConstructionResult::ready(std::move(actor));
                }
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, net_config, sink, actor_config, factory);
        const ActorKey key1{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key1, makeEnvelope(16)) == DeliveryResult::Accepted);

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(30ms);
        worker.requestStop();
        th.join();

        assert(peer_received.load(std::memory_order_acquire));
        assert(worker.metrics().actor.effect_send_failures == 1);
        assert(worker.metrics().actor.effect_close_failures == 1);
        assert(worker.metrics().actor.stopped_actors >= 1);
    }

    void test_shutdown_processes_accepted_inbox_events_in_quiescence_loop()
    {
        WorkerNetworkConfig net_config{
            .table = {.capacity = 10},
        };
        NullRequestSink sink;

        WorkerActorConfig actor_config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 500ms,
        };

        FunctionalActorFactory factory(nullptr);
        // Worker 0 with 2 workers in group topology
        Worker worker(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, net_config, sink, actor_config, factory);
        auto port = worker.bindInboxSource(WorkerId{1});

        // Push remote close event into Worker 0 inbox before run()
        WorkerEnvelope env{
            .event =
                RemoteConnectionClose{
                    .connection = ConnectionRef{ConnectionId{1}, ConnectionGeneration{1}, WorkerId{0}},
                    .reason = CloseReason::Shutdown,
                    .graceful = false,
                },
            .charged_bytes = static_cast<std::uint32_t>(sizeof(RemoteConnectionClose)),
        };
        assert(port.tryPush(std::move(env)) == InboxPushResult::Accepted);

        // Request stop immediately
        worker.requestStop();
        worker.run();

        // The accepted event must be consumed before Phase D tears resources down.
        assert(worker.metrics().inbox_events == 1);
        assert(worker.metrics().shutdown_inbox_events == 0);
    }

    void test_shutdown_cleans_up_all_resources_after_deadline()
    {
        WorkerActorConfig actor_config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 50ms,
        };

        FunctionalActorFactory factory(nullptr);
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, actor_config, factory);

        const ActorKey key{.kind = ActorKind::Player, .entity = 1};
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);

        worker.requestStop();
        worker.run();

        // After shutdown, all actors are cleaned up and mailboxes discarded
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.totalMailboxBytes() == 0);
    }

    void test_10000_actors_deterministic_execution()
    {
        constexpr std::size_t ACTOR_COUNT = 10000;
        WorkerActorConfig config{
            .actor_table_capacity = ACTOR_COUNT + 100,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = ACTOR_COUNT * 2,
            .max_mailbox_bytes_total = ACTOR_COUNT * 200,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 3000ms,
        };

        std::atomic<std::size_t> total_turns{0};

        FunctionalActorFactory factory(
            [&total_turns](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [&total_turns](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        total_turns.fetch_add(1, std::memory_order_relaxed);
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);

        // Activate and deliver 1 message to 10,000 actors
        for (std::uint64_t i = 1; i <= ACTOR_COUNT; ++i)
        {
            const ActorKey key{.kind = ActorKind::Player, .entity = i};
            assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        }

        assert(worker.actorCount() == ACTOR_COUNT);
        assert(worker.totalMailboxMessages() == ACTOR_COUNT);

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(150ms);
        worker.requestStop();
        th.join();

        assert(total_turns.load(std::memory_order_relaxed) == ACTOR_COUNT);
        assert(worker.metrics().actor.actor_turns == ACTOR_COUNT);
        assert(worker.totalMailboxMessages() == 0);
    }

    // =========================================================================
    // 5A — Suspended Turn & BlockedTask Tests
    // =========================================================================

    ActorTask makeOneShotSuspendingTask(std::atomic<int>& step, std::atomic<int>& destruct_count, const bool throw_on_resume = false)
    {
        struct DestructTracker
        {
            std::atomic<int>& count;
            ~DestructTracker()
            {
                ++count;
            }
        } tracker{destruct_count};

        step = 1;
        const auto outcome = co_await SyntheticAwait{};
        static_cast<void>(outcome);

        if (throw_on_resume)
        {
            throw std::runtime_error{"Coro exception"};
        }

        step = 2;
        co_return CompletedTurn{.effects = EffectBatch{}};
    }

    ActorTask makeTwoShotSuspendingTask(std::atomic<int>& step)
    {
        step = 1;
        const auto outcome1 = co_await SyntheticAwait{};
        static_cast<void>(outcome1);

        step = 2;
        const auto outcome2 = co_await SyntheticAwait{};
        static_cast<void>(outcome2);

        step = 3;
        co_return CompletedTurn{.effects = EffectBatch{}};
    }

    class SuspendingActor final : public ActorInstance
    {
    public:
        using TaskFactory = std::function<ActorTask()>;

        explicit SuspendingActor(TaskFactory factory)
            : _factory(std::move(factory))
        {
        }

        TurnResult dispatch(ActorEnvelope&&, const ActorTurnContext&) override
        {
            assert(_factory);
            ActorTask task = _factory();
            const auto status = task.resume();
            if (status == ActorTaskStatus::Suspended)
            {
                return SuspendedTurn{std::move(task)};
            }
            return task.takeCompleted();
        }

    private:
        TaskFactory _factory;
    };

    // Records the order in which the coroutine frame and the ActorInstance are destroyed.
    // A frame may reference the instance it ran on, so the frame must go first.
    struct TeardownOrder
    {
        std::atomic<int> next{1};
        std::atomic<int> frame{0};
        std::atomic<int> instance{0};
    };

    // Passed by value into the coroutine, so the frame copy is destroyed with the frame itself
    // rather than when the coroutine body returns. Moves null out the source so it counts once.
    class FrameDestructProbe final
    {
    public:
        explicit FrameDestructProbe(TeardownOrder& order) noexcept
            : _order(&order)
        {
        }

        FrameDestructProbe(FrameDestructProbe&& other) noexcept
            : _order(std::exchange(other._order, nullptr))
        {
        }

        FrameDestructProbe(const FrameDestructProbe&) = delete;
        FrameDestructProbe& operator=(const FrameDestructProbe&) = delete;
        FrameDestructProbe& operator=(FrameDestructProbe&&) = delete;

        ~FrameDestructProbe()
        {
            if (_order != nullptr)
            {
                _order->frame.store(_order->next.fetch_add(1));
            }
        }

    private:
        TeardownOrder* _order;
    };

    ActorTask makeSuspendingStopTask(std::atomic<int>& step, FrameDestructProbe probe)
    {
        static_cast<void>(probe);

        step = 1;
        static_cast<void>(co_await SyntheticAwait{});
        step = 2;

        EffectBatch effects;
        effects.push(StopActorEffect{});
        co_return CompletedTurn{.effects = std::move(effects)};
    }

    class StoppingSuspendingActor final : public ActorInstance
    {
    public:
        StoppingSuspendingActor(std::atomic<int>& step, TeardownOrder& order) noexcept
            : _step(step)
            , _order(order)
        {
        }

        ~StoppingSuspendingActor() override
        {
            _order.instance.store(_order.next.fetch_add(1));
        }

        TurnResult dispatch(ActorEnvelope&&, const ActorTurnContext&) override
        {
            ActorTask task = makeSuspendingStopTask(_step, FrameDestructProbe{_order});
            const auto status = task.resume();
            if (status == ActorTaskStatus::Suspended)
            {
                return SuspendedTurn{std::move(task)};
            }
            return task.takeCompleted();
        }

    private:
        std::atomic<int>& _step;
        TeardownOrder& _order;
    };

    void test_suspended_actor_does_not_dispatch_next_mailbox_command()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // 1. Deliver first message -> activates and queues
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);

        // Run ready actors -> dispatches and suspends
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        assert(step.load() == 1);
        assert(worker.metrics().actor.suspended_turns == 1);

        // 2. Deliver second message to suspended actor -> placed in mailbox
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.totalMailboxMessages() == 1);

        // Run ready actors again -> suspended actor must NOT dispatch 2nd message (INV-04)
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);
        assert(worker.totalMailboxMessages() == 1);
    }

    void test_completion_does_not_inline_resume_only_in_actor_phase()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);

        const auto& blocked = WorkerActorTestAccess::blocked(worker, key);
        assert(blocked.has_value());
        assert(std::holds_alternative<SyntheticSuspendedCommand>(*blocked));
        const AwaitKey await_key = std::get<SyntheticSuspendedCommand>(*blocked).key;

        // Mark completion ready -> does NOT resume inline!
        assert(WorkerActorTestAccess::tryMarkSyntheticCommandReady(worker, await_key, SyntheticAwaitOutcome::Completed));
        assert(step.load() == 1); // Still 1! Not inline resumed.

        // Run ready actors -> resumes continuation in actor phase
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 2);
        assert(worker.metrics().actor.resumed_turns == 1);
    }

    void test_duplicate_completion_dropped()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);

        const auto& blocked = WorkerActorTestAccess::blocked(worker, key);
        assert(blocked.has_value());
        const AwaitKey await_key = std::get<SyntheticSuspendedCommand>(*blocked).key;

        // First completion succeeds
        assert(WorkerActorTestAccess::completeSyntheticCommand(worker, await_key, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 0);

        // Second completion with the same key is dropped and counted by the completion source
        assert(!WorkerActorTestAccess::completeSyntheticCommand(worker, await_key, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 1);

        // The matcher itself stays metric-free so each source can raise its own stale counter
        assert(!WorkerActorTestAccess::tryMarkSyntheticCommandReady(worker, await_key, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 1);
    }

    void test_stale_operation_id_and_incarnation_completion_dropped()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);

        const auto& blocked = WorkerActorTestAccess::blocked(worker, key);
        assert(blocked.has_value());
        const AwaitKey real_key = std::get<SyntheticSuspendedCommand>(*blocked).key;

        // Stale operation id -> rejected
        const AwaitKey bad_op{real_key.actor, real_key.incarnation, OperationId{999}};
        assert(!WorkerActorTestAccess::completeSyntheticCommand(worker, bad_op, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 1);

        // Stale incarnation -> rejected
        const AwaitKey bad_inc{real_key.actor, ActorIncarnation{999}, real_key.operation};
        assert(!WorkerActorTestAccess::completeSyntheticCommand(worker, bad_inc, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 2);

        // Real key succeeds and leaves the stale counter alone
        assert(WorkerActorTestAccess::completeSyntheticCommand(worker, real_key, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 2);
    }

    void test_resume_only_occurs_via_ready_queue()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        assert(worker.metrics().actor.suspended_turns == 1);
        assert(worker.metrics().actor.resumed_turns == 0);

        const auto& blocked = WorkerActorTestAccess::blocked(worker, key);
        const AwaitKey await_key = std::get<SyntheticSuspendedCommand>(*blocked).key;
        assert(WorkerActorTestAccess::tryMarkSyntheticCommandReady(worker, await_key, SyntheticAwaitOutcome::Completed));

        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        assert(worker.metrics().actor.suspended_turns == 1);
        assert(worker.metrics().actor.resumed_turns == 1);
        assert(worker.metrics().actor.actor_turns == 2); // 1 initial dispatch turn + 1 resume turn
    }

    void test_chained_sequential_suspensions_replace_blocked_correctly()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};

        FunctionalActorFactory factory(
            [&step](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step]()
                    {
                        return makeTwoShotSuspendingTask(step);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        // 1st suspension (step == 1, op == 1)
        assert(step.load() == 1);
        const auto& blocked1 = WorkerActorTestAccess::blocked(worker, key);
        assert(blocked1.has_value());
        const AwaitKey key1 = std::get<SyntheticSuspendedCommand>(*blocked1).key;
        assert(key1.operation.value == 1);

        // Resume 1st suspension -> will advance to step 2 and suspend again (op == 2)
        assert(WorkerActorTestAccess::tryMarkSyntheticCommandReady(worker, key1, SyntheticAwaitOutcome::Completed));
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        assert(step.load() == 2);
        const auto& blocked2 = WorkerActorTestAccess::blocked(worker, key);
        assert(blocked2.has_value());
        const AwaitKey key2 = std::get<SyntheticSuspendedCommand>(*blocked2).key;
        assert(key2.operation.value == 2);

        // Resume 2nd suspension -> will advance to step 3 and complete turn
        assert(WorkerActorTestAccess::tryMarkSyntheticCommandReady(worker, key2, SyntheticAwaitOutcome::Completed));
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        assert(step.load() == 3);
        assert(worker.metrics().actor.suspended_turns == 2);
        assert(worker.metrics().actor.resumed_turns == 2);
    }

    void test_coroutine_frame_destroyed_exactly_once()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        // Suspended: frame is alive, destructs == 0
        assert(step.load() == 1);
        assert(destructs.load() == 0);

        const auto& blocked = WorkerActorTestAccess::blocked(worker, key);
        const AwaitKey await_key = std::get<SyntheticSuspendedCommand>(*blocked).key;
        assert(WorkerActorTestAccess::tryMarkSyntheticCommandReady(worker, await_key, SyntheticAwaitOutcome::Completed));

        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        // Completed: coroutine frame destroyed exactly once!
        assert(step.load() == 2);
        assert(destructs.load() == 1);
    }

    void test_exception_in_coroutine_rethrows_at_resume_boundary()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, true /* throw on resume */);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        const auto& blocked = WorkerActorTestAccess::blocked(worker, key);
        assert(blocked.has_value());
        const AwaitKey await_key = std::get<SyntheticSuspendedCommand>(*blocked).key;
        assert(WorkerActorTestAccess::tryMarkSyntheticCommandReady(worker, await_key, SyntheticAwaitOutcome::Completed));

        // Resuming will rethrow the exception from the coroutine frame
        bool caught = false;
        try
        {
            WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        }
        catch (const std::runtime_error& err)
        {
            caught = true;
            assert(std::string(err.what()) == "Coro exception");
        }
        assert(caught);
    }

    void test_resumed_stop_effect_destroys_frame_before_actor_instance()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};
        TeardownOrder order;

        FunctionalActorFactory factory(
            [&step, &order](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<StoppingSuspendingActor>(step, order));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);
        assert(worker.actorCount() == 1);

        // Queued mail must be returned to the Worker accounting when the actor is removed.
        assert(worker.tryDeliverLocal(key, makeEnvelope(24)) == DeliveryResult::Accepted);
        assert(worker.totalMailboxMessages() == 1);

        const AwaitKey await_key = std::get<SyntheticSuspendedCommand>(*WorkerActorTestAccess::blocked(worker, key)).key;
        assert(WorkerActorTestAccess::completeSyntheticCommand(worker, await_key, SyntheticAwaitOutcome::Completed));

        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        assert(step.load() == 2);
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.totalMailboxBytes() == 0);
        assert(worker.metrics().actor.stopped_actors == 1);

        // The resumed turn stopped the actor: the coroutine frame must be gone before the
        // ActorInstance it ran on is destroyed.
        assert(order.frame.load() != 0);
        assert(order.instance.load() != 0);
        assert(order.frame.load() < order.instance.load());
    }

    void test_suspended_actor_removed_resets_mailbox_accounting()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);

        // Enqueue extra mailbox message
        assert(worker.tryDeliverLocal(key, makeEnvelope(24)) == DeliveryResult::Accepted);
        assert(worker.totalMailboxMessages() == 1);
        assert(worker.totalMailboxBytes() > 0);

        const ActorHandle handle = WorkerActorTestAccess::handle(worker, key);
        WorkerActorTestAccess::removeActor(worker, handle, ActorRemovalReason::Stopped);

        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.totalMailboxBytes() == 0);
        assert(destructs.load() == 1); // coroutine frame cleanly destroyed during removeActor
    }

    // =========================================================================
    // 5B — Loading Foundation Tests
    // =========================================================================

    void test_loading_actor_does_not_dispatch_messages_and_enqueues_to_mailbox()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<std::size_t> turns{0};
        FunctionalActorFactory factory(
            [&turns](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [&turns](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        turns.fetch_add(1, std::memory_order_relaxed);
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // 1. Begin activation load
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.loadingCount() == 1);
        assert(worker.actorCount() == 1);
        assert(worker.totalMailboxMessages() == 1);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Loading);
        assert(worker.metrics().actor.activation_loads_started == 1);

        // 2. Deliver second message -> enqueued to mailbox only
        assert(worker.tryDeliverLocal(key, makeEnvelope(20)) == DeliveryResult::Accepted);
        assert(worker.totalMailboxMessages() == 2);
        assert(worker.loadingCount() == 1);

        // 3. Run ready actors -> loading actor does NOT run any turns
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(turns.load() == 0);
        assert(worker.metrics().actor.actor_turns == 0);
        assert(worker.totalMailboxMessages() == 2);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Loading);
    }

    void test_activation_load_enforces_the_same_mailbox_byte_cap_as_local_delivery()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 2,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 32,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 32,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        FunctionalActorFactory factory(nullptr);
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // The caller cannot under-report the charge to slip past the cap on the activation path
        // any more than it can on tryDeliverLocal().
        ActorEnvelope underreported = makeEnvelopeWithCharge(64, 1);
        assert(underreported.chargedBytes() > config.max_mailbox_bytes_per_actor);
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key, std::move(underreported)) == DeliveryResult::MailboxFull);

        assert(worker.actorCount() == 0);
        assert(worker.loadingCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.totalMailboxBytes() == 0);
        assert(worker.metrics().actor.activation_loads_started == 0);
    }

    void test_activation_success_transitions_to_idle_or_queued_based_on_mailbox()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<std::size_t> turns{0};
        FunctionalActorFactory factory(
            [&turns](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [&turns](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        turns.fetch_add(1, std::memory_order_relaxed);
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);

        // Case A: Non-empty mailbox -> transitions to Queued and pushes to ready queue
        const ActorKey key1{.kind = ActorKind::Player, .entity = 1};
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key1, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.loadingCount() == 1);

        const AwaitKey await_key1 = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, key1)).key;
        WorkerActorTestAccess::completeSyntheticActivation(worker, await_key1, SyntheticActivationOutcome::Ready);

        assert(worker.loadingCount() == 0);
        assert(WorkerActorTestAccess::slot(worker, key1)->state() == ActorState::Queued);

        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(turns.load() == 1);
        assert(WorkerActorTestAccess::slot(worker, key1)->state() == ActorState::Idle);

        // Case B: Empty mailbox -> transitions directly to Idle
        const ActorKey key2{.kind = ActorKind::Player, .entity = 2};
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key2, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.loadingCount() == 1);

        // Drain through the Worker helper to simulate an empty mailbox. Calling
        // ActorSlot::clearMailbox() directly would leave _total_mailbox_messages inflated.
        static_cast<void>(WorkerActorTestAccess::discardMailbox(worker, key2));
        assert(worker.totalMailboxMessages() == 0);

        const AwaitKey await_key2 = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, key2)).key;
        WorkerActorTestAccess::completeSyntheticActivation(worker, await_key2, SyntheticActivationOutcome::Ready);

        assert(worker.loadingCount() == 0);
        assert(WorkerActorTestAccess::slot(worker, key2)->state() == ActorState::Idle);

        // Running ready queue does not run turns for Idle actor
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(turns.load() == 1);
    }

    void test_activation_failure_discards_mailbox_releases_slot_and_resets_accounting()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        bool factory_reject = false;
        FunctionalActorFactory factory(
            [&factory_reject](ActorKey) -> ActorConstructionResult
            {
                if (factory_reject)
                {
                    return ActorConstructionResult::rejected();
                }
                auto actor = std::make_unique<FunctionalActor>(
                    [](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);

        // 1. Rejected outcome
        const ActorKey key1{.kind = ActorKind::Player, .entity = 1};
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key1, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.tryDeliverLocal(key1, makeEnvelope(20)) == DeliveryResult::Accepted);
        assert(worker.loadingCount() == 1);
        assert(worker.totalMailboxMessages() == 2);

        const AwaitKey await_key1 = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, key1)).key;
        WorkerActorTestAccess::completeSyntheticActivation(worker, await_key1, SyntheticActivationOutcome::Rejected);

        assert(worker.loadingCount() == 0);
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.totalMailboxBytes() == 0);
        assert(worker.metrics().actor.activation_load_failures == 1);

        // 2. TimedOut outcome
        const ActorKey key2{.kind = ActorKind::Player, .entity = 2};
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key2, makeEnvelope(16)) == DeliveryResult::Accepted);
        const AwaitKey await_key2 = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, key2)).key;
        WorkerActorTestAccess::completeSyntheticActivation(worker, await_key2, SyntheticActivationOutcome::TimedOut);

        assert(worker.loadingCount() == 0);
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.metrics().actor.activation_load_failures == 2);

        // 3. Cancelled outcome
        const ActorKey key3{.kind = ActorKind::Player, .entity = 3};
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key3, makeEnvelope(16)) == DeliveryResult::Accepted);
        const AwaitKey await_key3 = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, key3)).key;
        WorkerActorTestAccess::completeSyntheticActivation(worker, await_key3, SyntheticActivationOutcome::Cancelled);

        assert(worker.loadingCount() == 0);
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.metrics().actor.activation_load_failures == 3);

        // 4. construct() returns Rejected when outcome is Ready
        factory_reject = true;
        const ActorKey key4{.kind = ActorKind::Player, .entity = 4};
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key4, makeEnvelope(16)) == DeliveryResult::Accepted);
        const AwaitKey await_key4 = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, key4)).key;
        WorkerActorTestAccess::completeSyntheticActivation(worker, await_key4, SyntheticActivationOutcome::Ready);

        assert(worker.loadingCount() == 0);
        assert(worker.actorCount() == 0);
        assert(worker.totalMailboxMessages() == 0);
        assert(worker.metrics().actor.activation_load_failures == 4);
    }

    void test_concurrent_loading_cap_exceeded_returns_false_and_increments_metric()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 2,
        };

        FunctionalActorFactory factory(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(
                    [](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                ));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);

        const ActorKey key1{.kind = ActorKind::Player, .entity = 1};
        const ActorKey key2{.kind = ActorKind::Player, .entity = 2};
        const ActorKey key3{.kind = ActorKind::Player, .entity = 3};

        assert(WorkerActorTestAccess::beginActivationLoad(worker, key1, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.loadingCount() == 1);

        assert(WorkerActorTestAccess::beginActivationLoad(worker, key2, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.loadingCount() == 2);

        // 3rd concurrent loading exceeds cap of 2 -> rejected
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key3, makeEnvelope(16)) == DeliveryResult::ActivationLimit);
        assert(worker.loadingCount() == 2);
        assert(worker.actorCount() == 2);
        assert(worker.metrics().actor.loading_limit_rejections == 1);
    }

    void test_loading_stale_and_duplicate_completions_dropped()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        FunctionalActorFactory factory(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(
                    [](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                ));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(WorkerActorTestAccess::beginActivationLoad(worker, key, makeEnvelope(16)) == DeliveryResult::Accepted);
        const AwaitKey real_key = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, key)).key;

        // 1. Stale operation id -> dropped
        const AwaitKey bad_op{real_key.actor, real_key.incarnation, OperationId{999}};
        WorkerActorTestAccess::completeSyntheticActivation(worker, bad_op, SyntheticActivationOutcome::Ready);
        assert(worker.metrics().actor.stale_activation_completions == 1);
        assert(worker.loadingCount() == 1);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Loading);

        // 2. Stale incarnation -> dropped
        const AwaitKey bad_inc{real_key.actor, ActorIncarnation{999}, real_key.operation};
        WorkerActorTestAccess::completeSyntheticActivation(worker, bad_inc, SyntheticActivationOutcome::Ready);
        assert(worker.metrics().actor.stale_activation_completions == 2);
        assert(worker.loadingCount() == 1);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Loading);

        // 3. Valid completion -> succeeds
        WorkerActorTestAccess::completeSyntheticActivation(worker, real_key, SyntheticActivationOutcome::Ready);
        assert(worker.loadingCount() == 0);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Queued);

        // 4. Duplicate completion with old key -> dropped
        WorkerActorTestAccess::completeSyntheticActivation(worker, real_key, SyntheticActivationOutcome::Ready);
        assert(worker.metrics().actor.stale_activation_completions == 3);
    }

    void test_loading_count_invariant_across_all_lifecycle_paths()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 50ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        FunctionalActorFactory factory(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(
                    [](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                ));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);

        const ActorKey k1{.kind = ActorKind::Player, .entity = 1};
        const ActorKey k2{.kind = ActorKind::Player, .entity = 2};
        const ActorKey k3{.kind = ActorKind::Player, .entity = 3};

        assert(WorkerActorTestAccess::beginActivationLoad(worker, k1, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(WorkerActorTestAccess::beginActivationLoad(worker, k2, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(WorkerActorTestAccess::beginActivationLoad(worker, k3, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.loadingCount() == 3);

        // 1 completes Ready -> loadingCount becomes 2
        const AwaitKey await_k1 = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, k1)).key;
        WorkerActorTestAccess::completeSyntheticActivation(worker, await_k1, SyntheticActivationOutcome::Ready);
        assert(worker.loadingCount() == 2);

        // 2 fails TimedOut -> loadingCount becomes 1
        const AwaitKey await_k2 = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, k2)).key;
        WorkerActorTestAccess::completeSyntheticActivation(worker, await_k2, SyntheticActivationOutcome::TimedOut);
        assert(worker.loadingCount() == 1);

        // 3 is removed directly via shutdown -> loadingCount becomes 0
        worker.requestStop();
        worker.run();
        assert(worker.loadingCount() == 0);
        assert(worker.actorCount() == 0);
    }

    // =========================================================================
    // 5C — Timeout / Cancellation / Race Tests
    // =========================================================================

    void test_completion_first_then_late_timeout_stale_drop()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 50ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> step{0};
        SyntheticAwaitOutcome received_outcome{SyntheticAwaitOutcome::Rejected};

        FunctionalActorFactory factory(
            [&step, &received_outcome](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &received_outcome]() -> ActorTask
                    {
                        step.store(1);
                        SyntheticAwait awaiter;
                        auto outcome = co_await awaiter;
                        received_outcome = outcome;
                        step.store(2);
                        co_return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);

        const AwaitKey await_key = std::get<SyntheticSuspendedCommand>(*WorkerActorTestAccess::blocked(worker, key)).key;

        // 1. Completion arrives first
        assert(WorkerActorTestAccess::completeSyntheticCommand(worker, await_key, SyntheticAwaitOutcome::Completed));
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Queued);

        // 2. Late timeout fires after deadline
        WorkerActorTestAccess::expireTimers(worker, std::chrono::steady_clock::now() + 100ms, WorkerBudgets::defaults().timers);
        assert(worker.metrics().actor.stale_await_timeouts == 1);

        // 3. Resumed turn completes with original Completed outcome
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 2);
        assert(received_outcome == SyntheticAwaitOutcome::Completed);
    }

    void test_timeout_first_then_late_backend_completion_stale_drop()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 50ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> step{0};
        SyntheticAwaitOutcome received_outcome{SyntheticAwaitOutcome::Rejected};

        FunctionalActorFactory factory(
            [&step, &received_outcome](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &received_outcome]() -> ActorTask
                    {
                        step.store(1);
                        SyntheticAwait awaiter;
                        auto outcome = co_await awaiter;
                        received_outcome = outcome;
                        step.store(2);
                        co_return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);

        const AwaitKey await_key = std::get<SyntheticSuspendedCommand>(*WorkerActorTestAccess::blocked(worker, key)).key;

        // 1. Timeout fires first
        WorkerActorTestAccess::expireTimers(worker, std::chrono::steady_clock::now() + 100ms, WorkerBudgets::defaults().timers);
        assert(worker.metrics().actor.stale_await_timeouts == 0);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Queued);

        // 2. Late backend completion arrives -> dropped as stale
        assert(!WorkerActorTestAccess::completeSyntheticCommand(worker, await_key, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 1);

        // 3. Resumed turn receives TimedOut outcome
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 2);
        assert(received_outcome == SyntheticAwaitOutcome::TimedOut);
    }

    void test_loading_actor_timeout_then_late_activation_completion_stale_drop()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 50ms,
            .max_concurrent_loading = 10,
        };

        FunctionalActorFactory factory(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(
                    [](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                ));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(WorkerActorTestAccess::beginActivationLoad(worker, key, makeEnvelope(16)) == DeliveryResult::Accepted);
        const AwaitKey await_key = std::get<ActivationLoad>(*WorkerActorTestAccess::blocked(worker, key)).key;
        assert(worker.loadingCount() == 1);

        // 1. Timeout fires for Loading actor -> removes actor
        WorkerActorTestAccess::expireTimers(worker, std::chrono::steady_clock::now() + 100ms, WorkerBudgets::defaults().timers);
        assert(worker.loadingCount() == 0);
        assert(worker.actorCount() == 0);
        assert(worker.metrics().actor.activation_load_failures == 1);

        // 2. Late completion arrives -> dropped as stale
        WorkerActorTestAccess::completeSyntheticActivation(worker, await_key, SyntheticActivationOutcome::Ready);
        assert(worker.metrics().actor.stale_activation_completions == 1);
    }

    void test_actor_removal_then_late_completion_drop()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);

        const AwaitKey await_key = std::get<SyntheticSuspendedCommand>(*WorkerActorTestAccess::blocked(worker, key)).key;
        const ActorHandle handle = WorkerActorTestAccess::handle(worker, key);

        // Remove actor while suspended
        WorkerActorTestAccess::removeActor(worker, handle, ActorRemovalReason::Stopped);
        assert(worker.actorCount() == 0);

        // Late completion -> dropped
        assert(!WorkerActorTestAccess::completeSyntheticCommand(worker, await_key, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 1);
    }

    void test_slot_reuse_old_incarnation_completion_drop()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 1,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // Incarnation 1 starts and suspends
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        const AwaitKey await_key1 = std::get<SyntheticSuspendedCommand>(*WorkerActorTestAccess::blocked(worker, key)).key;
        const ActorHandle handle1 = WorkerActorTestAccess::handle(worker, key);

        // Incarnation 1 removed
        WorkerActorTestAccess::removeActor(worker, handle1, ActorRemovalReason::Stopped);

        // Incarnation 2 starts and suspends
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        const AwaitKey await_key2 = std::get<SyntheticSuspendedCommand>(*WorkerActorTestAccess::blocked(worker, key)).key;
        assert(await_key2.incarnation != await_key1.incarnation);

        // Late completion for incarnation 1 -> dropped
        assert(!WorkerActorTestAccess::completeSyntheticCommand(worker, await_key1, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 1);

        // Valid completion for incarnation 2 -> succeeds
        assert(WorkerActorTestAccess::completeSyntheticCommand(worker, await_key2, SyntheticAwaitOutcome::Completed));
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Idle);
    }

    void test_operation_N_timeout_then_operation_N_plus_1_completion_race()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 50ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> stage{0};
        FunctionalActorFactory factory(
            [&stage](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&stage]() -> ActorTask
                    {
                        stage.store(1);
                        SyntheticAwait awaiter1;
                        auto outcome1 = co_await awaiter1;
                        (void)outcome1;

                        stage.store(2);
                        SyntheticAwait awaiter2;
                        auto outcome2 = co_await awaiter2;
                        (void)outcome2;

                        stage.store(3);
                        co_return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(stage.load() == 1);

        const AwaitKey await_key1 = std::get<SyntheticSuspendedCommand>(*WorkerActorTestAccess::blocked(worker, key)).key;

        // 1. Timeout fires for Op 1 -> resumes and enters Op 2
        WorkerActorTestAccess::expireTimers(worker, std::chrono::steady_clock::now() + 100ms, WorkerBudgets::defaults().timers);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(stage.load() == 2);

        const AwaitKey await_key2 = std::get<SyntheticSuspendedCommand>(*WorkerActorTestAccess::blocked(worker, key)).key;
        assert(await_key2.operation != await_key1.operation);

        // 2. Late backend completion for Op 1 arrives -> dropped as stale
        assert(!WorkerActorTestAccess::completeSyntheticCommand(worker, await_key1, SyntheticAwaitOutcome::Completed));
        assert(worker.metrics().actor.stale_completions == 1);

        // 3. Valid completion for Op 2 arrives -> succeeds
        assert(WorkerActorTestAccess::completeSyntheticCommand(worker, await_key2, SyntheticAwaitOutcome::Completed));
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(stage.load() == 3);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Idle);
    }

    void test_timer_reservation_failure_command_resumes_rejected_activation_fails_fast()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> step{0};
        SyntheticAwaitOutcome received_outcome{SyntheticAwaitOutcome::Completed};

        FunctionalActorFactory factory(
            [&step, &received_outcome](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &received_outcome]() -> ActorTask
                    {
                        step.store(1);
                        SyntheticAwait awaiter;
                        auto outcome = co_await awaiter;
                        received_outcome = outcome;
                        step.store(2);
                        co_return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);

        // Exhaust timer queue capacity
        for (std::size_t i = 0; i < TimerQueue::CAPACITY; ++i)
        {
            const auto deadline = std::chrono::steady_clock::now() + 1000s;
            assert(worker.trySchedule(deadline, TimerPayload{AwaitTimeout{AwaitKey{}}}));
        }

        // Case A: Suspending command turn fails timer reservation -> resumes with Rejected
        const ActorKey key1{.kind = ActorKind::Player, .entity = 1};
        assert(worker.tryDeliverLocal(key1, makeEnvelope(16)) == DeliveryResult::Accepted);

        // Dispatch runs turn, tryReserve fails -> Queued with Rejected
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 2);
        assert(received_outcome == SyntheticAwaitOutcome::Rejected);
        assert(WorkerActorTestAccess::slot(worker, key1)->state() == ActorState::Idle);

        // Case B: beginActivationLoad fails timer reservation -> rejected immediately
        const ActorKey key2{.kind = ActorKind::Player, .entity = 2};
        assert(WorkerActorTestAccess::beginActivationLoad(worker, key2, makeEnvelope(16)) != DeliveryResult::Accepted);
        assert(worker.loadingCount() == 0);
    }

    void test_shutdown_logical_cancel_unwinds_coroutine_and_applies_effects()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> step{0};
        std::atomic<bool> cancelled_seen{false};

        FunctionalActorFactory factory(
            [&step, &cancelled_seen](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &cancelled_seen]() -> ActorTask
                    {
                        step.store(1);
                        SyntheticAwait awaiter;
                        auto outcome = co_await awaiter;
                        if (outcome == SyntheticAwaitOutcome::Cancelled)
                        {
                            cancelled_seen.store(true);
                        }
                        step.store(2);
                        EffectBatch effects;
                        effects.push(StopActorEffect{});
                        co_return CompletedTurn{.effects = std::move(effects)};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Suspended);

        // Request stop and run shutdown
        worker.requestStop();
        worker.run();

        assert(cancelled_seen.load());
        assert(step.load() == 2);
        assert(worker.metrics().actor.cancelled_blocked_actors == 1);
        assert(worker.actorCount() == 0);
    }

    void test_shutdown_logical_cancel_reaches_a_suspended_actor_with_queued_mail()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 300ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> step{0};
        std::atomic<bool> cancelled_seen{false};

        FunctionalActorFactory factory(
            [&step, &cancelled_seen](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &cancelled_seen]() -> ActorTask
                    {
                        step.store(1);
                        SyntheticAwait awaiter;
                        auto outcome = co_await awaiter;
                        if (outcome == SyntheticAwaitOutcome::Cancelled)
                        {
                            cancelled_seen.store(true);
                        }
                        step.store(2);
                        EffectBatch effects;
                        effects.push(StopActorEffect{});
                        co_return CompletedTurn{.effects = std::move(effects)};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);
        assert(WorkerActorTestAccess::slot(worker, key)->state() == ActorState::Suspended);

        // A blocked actor's mailbox is not runnable work: it cannot drain until the actor unblocks,
        // so quiescence must not wait on it before cancelling.
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.totalMailboxMessages() == 1);

        worker.requestStop();
        worker.run();

        assert(cancelled_seen.load());
        assert(step.load() == 2);
        assert(worker.metrics().actor.cancelled_blocked_actors == 1);
        assert(worker.metrics().actor.forced_blocked_destructions == 0);
        assert(worker.actorCount() == 0);
    }

    void test_shutdown_new_suspension_immediately_cancelled_no_long_term_blocked()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> suspensions{0};
        std::atomic<int> completions{0};

        FunctionalActorFactory factory(
            [&suspensions, &completions](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&suspensions, &completions]() -> ActorTask
                    {
                        suspensions.fetch_add(1);
                        SyntheticAwait awaiter;
                        auto outcome = co_await awaiter;
                        if (outcome == SyntheticAwaitOutcome::Cancelled)
                        {
                            completions.fetch_add(1);
                        }
                        co_return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        // Queue message in mailbox before shutdown
        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);

        worker.requestStop();
        worker.run();

        assert(suspensions.load() == 1);
        assert(completions.load() == 1);
        assert(worker.metrics().actor.cancelled_blocked_actors >= 1);
        assert(worker.actorCount() == 0);
    }

    void test_shutdown_forced_destruction_on_deadline_expiry_metrics()
    {
        WorkerActorConfig config{
            .actor_table_capacity = 5,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 10,
            .max_mailbox_bytes_total = 1024,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> step{0};
        std::atomic<int> destructs{0};

        FunctionalActorFactory factory(
            [&step, &destructs](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<SuspendingActor>(
                    [&step, &destructs]()
                    {
                        return makeOneShotSuspendingTask(step, destructs, false);
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tryDeliverLocal(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(step.load() == 1);

        const ActorHandle handle = WorkerActorTestAccess::handle(worker, key);
        assert(WorkerActorTestAccess::slot(worker, key)->hasBlocked());

        // Simulate forced shutdown destruction in Phase D
        WorkerActorTestAccess::removeActor(worker, handle, ActorRemovalReason::ShutdownForced);
        assert(worker.metrics().actor.forced_blocked_destructions == 1);
        assert(worker.metrics().actor.stopped_actors == 1);
        assert(worker.actorCount() == 0);
        assert(destructs.load() == 1);
    }

    void test_tell_local_mailbox_non_reentrancy_and_no_self_lane()
    {
        const WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 20,
            .max_mailbox_bytes_total = 2048,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::atomic<int> turns_executed{0};
        FunctionalActorFactory factory(
            [&turns_executed](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [&turns_executed](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        ++turns_executed;
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        const ActorKey key{.kind = ActorKind::Player, .entity = 1};

        assert(worker.tell(key, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(worker.metrics().actor.remote_tells_sent == 0);
        assert(worker.totalMailboxMessages() == 1);

        WorkerActorTestAccess::runReadyActors(worker, WorkerBudgets::defaults().actors);
        assert(turns_executed.load() == 1);
        assert(worker.totalMailboxMessages() == 0);
    }

    void test_tell_remote_routes_to_target_inbox_fifo_and_bounds()
    {
        const WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 20,
            .max_mailbox_bytes_total = 2048,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        std::vector<std::size_t> executed_payload_sizes;
        FunctionalActorFactory factory0(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(nullptr));
            }
        );
        FunctionalActorFactory factory1(
            [&executed_payload_sizes](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [&executed_payload_sizes](ActorEnvelope&& envelope, const ActorTurnContext&) -> TurnResult
                    {
                        executed_payload_sizes.push_back(envelope.get<TestPingPayload>().frame.payload.size());
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker w0(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory0);
        Worker w1(WorkerId{1}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory1);

        w0.bindRemoteTarget(WorkerId{1}, w1.bindInboxSource(WorkerId{0}));
        w1.bindRemoteTarget(WorkerId{0}, w0.bindInboxSource(WorkerId{1}));

        ActorKey k1{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k1, 2, config.placement_seed) != WorkerId{1})
        {
            ++k1.entity;
        }

        assert(w0.tell(k1, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(w0.tell(k1, makeEnvelope(32)) == DeliveryResult::Accepted);
        assert(w0.metrics().actor.remote_tells_sent == 2);
        assert(w1.metrics().actor.remote_tells_received == 0);

        WorkerActorTestAccess::drainInbox(w1, WorkerBudgets::defaults().inbox);
        assert(w1.metrics().actor.remote_tells_received == 2);
        assert(w1.metrics().actor.remote_tells_delivered == 2);
        assert(w1.metrics().actor.remote_tell_delivery_failures == 0);
        assert(w1.totalMailboxMessages() == 2);

        WorkerActorTestAccess::runReadyActors(w1, WorkerBudgets::defaults().actors);
        assert(executed_payload_sizes.size() == 2);
        assert(executed_payload_sizes[0] == 16);
        assert(executed_payload_sizes[1] == 32);
    }

    void test_tell_remote_inbox_full_and_charge_overflow_rejections()
    {
        const WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 20,
            .max_mailbox_bytes_total = 2048,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        const WorkerInboxConfig small_inbox{
            .max_bytes_per_worker = 60,
            .max_workers = 32,
        };

        FunctionalActorFactory factory0(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(nullptr));
            }
        );
        FunctionalActorFactory factory1(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(nullptr));
            }
        );

        Worker w0(WorkerId{0}, 2, WorkerBudgets::defaults(), small_inbox, config, factory0);
        Worker w1(WorkerId{1}, 2, WorkerBudgets::defaults(), small_inbox, config, factory1);

        w0.bindRemoteTarget(WorkerId{1}, w1.bindInboxSource(WorkerId{0}));

        ActorKey k1{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k1, 2, config.placement_seed) != WorkerId{1})
        {
            ++k1.entity;
        }

        assert(w0.tell(k1, makeEnvelope(16)) == DeliveryResult::Accepted);
        // 2nd push exceeds byte capacity 40 -> RemoteInboxFull
        assert(w0.tell(k1, makeEnvelope(16)) == DeliveryResult::RemoteInboxFull);
        assert(w0.metrics().actor.remote_tell_rejections == 1);

        ActorEnvelope huge_env = makeEnvelopeWithCharge(16, static_cast<std::uint64_t>(std::numeric_limits<std::uint32_t>::max()) + 100ULL);
        assert(w0.tell(k1, std::move(huge_env)) == DeliveryResult::RemoteInboxFull);
        assert(w0.metrics().actor.remote_tell_rejections == 2);
    }

    void test_tell_actor_effect_remote_routing()
    {
        const WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 20,
            .max_mailbox_bytes_total = 2048,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        ActorKey k1{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k1, 2, config.placement_seed) != WorkerId{1})
        {
            ++k1.entity;
        }

        ActorKey k0{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k0, 2, config.placement_seed) != WorkerId{0})
        {
            ++k0.entity;
        }

        FunctionalActorFactory factory0(
            [k1](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [k1](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                    {
                        EffectBatch effects;
                        effects.push(TellActorEffect{.target = k1, .message = makeEnvelope(64)});
                        return CompletedTurn{.effects = std::move(effects)};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        std::atomic<bool> received_on_w1{false};
        FunctionalActorFactory factory1(
            [&received_on_w1](ActorKey) -> ActorConstructionResult
            {
                auto actor = std::make_unique<FunctionalActor>(
                    [&received_on_w1](ActorEnvelope&& envelope, const ActorTurnContext&) -> TurnResult
                    {
                        if (envelope.get<TestPingPayload>().frame.payload.size() == 64)
                        {
                            received_on_w1.store(true);
                        }
                        return CompletedTurn{.effects = EffectBatch{}};
                    }
                );
                return ActorConstructionResult::ready(std::move(actor));
            }
        );

        Worker w0(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory0);
        Worker w1(WorkerId{1}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory1);

        w0.bindRemoteTarget(WorkerId{1}, w1.bindInboxSource(WorkerId{0}));
        w1.bindRemoteTarget(WorkerId{0}, w0.bindInboxSource(WorkerId{1}));

        assert(w0.tell(k0, makeEnvelope(16)) == DeliveryResult::Accepted);
        WorkerActorTestAccess::runReadyActors(w0, WorkerBudgets::defaults().actors);
        assert(w0.metrics().actor.remote_tells_sent == 1);

        WorkerActorTestAccess::drainInbox(w1, WorkerBudgets::defaults().inbox);
        assert(w1.metrics().actor.remote_tells_received == 1);
        assert(w1.metrics().actor.remote_tells_delivered == 1);

        WorkerActorTestAccess::runReadyActors(w1, WorkerBudgets::defaults().actors);
        assert(received_on_w1.load() == true);
    }

    void test_remote_actor_message_without_runtime_and_misrouted_metrics()
    {
        const WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 20,
            .max_mailbox_bytes_total = 2048,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        NullRequestSink sink;
        Worker w_no_actor(WorkerId{1}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, WorkerNetworkConfig{}, sink);
        auto port = w_no_actor.bindInboxSource(WorkerId{0});

        ActorKey k1{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k1, 2, config.placement_seed) != WorkerId{1})
        {
            ++k1.entity;
        }

        assert(
            port.tryPush(WorkerEnvelope{
                .event = RemoteActorMessage{.target = k1, .message = makeEnvelope(16)},
                .charged_bytes = 64,
            }) == InboxPushResult::Accepted
        );

        WorkerActorTestAccess::drainInbox(w_no_actor, WorkerBudgets::defaults().inbox);
        assert(w_no_actor.metrics().actor.remote_tells_received == 1);
        assert(w_no_actor.metrics().actor.remote_tell_delivery_failures == 1);
        assert(w_no_actor.metrics().actor.actor_events_without_runtime == 1);

        FunctionalActorFactory factory(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(nullptr));
            }
        );
        Worker w1(WorkerId{1}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);
        auto port1 = w1.bindInboxSource(WorkerId{0});

        ActorKey k0{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k0, 2, config.placement_seed) != WorkerId{0})
        {
            ++k0.entity;
        }

        assert(
            port1.tryPush(WorkerEnvelope{
                .event = RemoteActorMessage{.target = k0, .message = makeEnvelope(16)},
                .charged_bytes = 64,
            }) == InboxPushResult::Accepted
        );

        WorkerActorTestAccess::drainInbox(w1, WorkerBudgets::defaults().inbox);
        assert(w1.metrics().actor.remote_tells_received == 1);
        assert(w1.metrics().actor.remote_tell_delivery_failures == 1);
        assert(w1.metrics().actor.misrouted_actor_events == 1);
    }

    void test_tell_closed_when_stopping_or_port_unbound()
    {
        const WorkerActorConfig config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 1024,
            .max_mailbox_messages_total = 20,
            .max_mailbox_bytes_total = 2048,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        FunctionalActorFactory factory(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(nullptr));
            }
        );
        Worker w0(WorkerId{0}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory);

        ActorKey k1{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k1, 2, config.placement_seed) != WorkerId{1})
        {
            ++k1.entity;
        }

        ActorKey k0{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k0, 2, config.placement_seed) != WorkerId{0})
        {
            ++k0.entity;
        }

        assert(w0.tell(k1, makeEnvelope(16)) == DeliveryResult::Closed);
        assert(w0.metrics().actor.remote_tell_rejections == 1);

        FunctionalActorFactory factory1(
            [](ActorKey) -> ActorConstructionResult
            {
                return ActorConstructionResult::ready(std::make_unique<FunctionalActor>(nullptr));
            }
        );
        Worker w1(WorkerId{1}, 2, WorkerBudgets::defaults(), WorkerInboxConfig{}, config, factory1);
        w0.bindRemoteTarget(WorkerId{1}, w1.bindInboxSource(WorkerId{0}));
        WorkerActorTestAccess::closeInbox(w1);
        assert(w0.tell(k1, makeEnvelope(16)) == DeliveryResult::Closed);
        assert(w0.metrics().actor.remote_tell_rejections == 2);

        w0.requestStop();
        assert(w0.tell(k0, makeEnvelope(16)) == DeliveryResult::Closed);
        assert(w0.tell(k1, makeEnvelope(16)) == DeliveryResult::Closed);
    }

    void test_shutdown_phase_a_rejects_stale_readable_events()
    {
        CountingRequestSink sink;
        Worker worker(WorkerId{0}, 1, WorkerBudgets::defaults(), WorkerInboxConfig{}, WorkerNetworkConfig{}, sink);

        auto sockets = makeSocketPair();
        const PollEvent stale_readable = WorkerActorTestAccess::installReadableConnection(worker, std::move(sockets.left));
        const auto encoded = snf::protocol::encode_frame(makeFrame(16));
        const auto sent = ::send(sockets.right.getDescriptor(), encoded.data(), encoded.size(), 0);
        assert(sent == static_cast<ssize_t>(encoded.size()));

        WorkerActorTestAccess::beginShutdownPhaseA(worker);
        WorkerActorTestAccess::processPollEvents(worker, std::span<const PollEvent>{&stale_readable, 1}, WorkerBudgets::defaults().poll);

        assert(sink.posts.load() == 0);
        assert(WorkerActorTestAccess::readWorkQueueEmpty(worker));
    }

    void test_barrier_unit_contracts()
    {
        bool zero_rejected = false;
        try
        {
            WorkerQuiescenceBarrier invalid{0};
            static_cast<void>(invalid);
        }
        catch (const std::invalid_argument&)
        {
            zero_rejected = true;
        }
        assert(zero_rejected);

        bool over_limit_rejected = false;
        try
        {
            WorkerQuiescenceBarrier invalid{33};
            static_cast<void>(invalid);
        }
        catch (const std::invalid_argument&)
        {
            over_limit_rejected = true;
        }
        assert(over_limit_rejected);

        WorkerGroupConfig invalid_group_config;
        invalid_group_config.worker_count = 33;
        invalid_group_config.max_workers = 33;
        invalid_group_config.inbox.max_workers = 33;
        assert(!isValid(invalid_group_config));

        WorkerQuiescenceBarrier barrier(2);
        assert(!barrier.armed());
        assert(!barrier.snapshot().armed);
        assert(!barrier.snapshot().all_quiescent);
        assert(barrier.snapshot().epoch == 0);

        barrier.arm();
        assert(barrier.armed());
        assert(barrier.snapshot().armed);
        assert(!barrier.snapshot().all_quiescent);

        // Worker 0 marks quiescent with epoch 0
        assert(barrier.tryMarkQuiescent(WorkerId{0}, 0) == true);
        assert(!barrier.snapshot().all_quiescent);

        // Worker 1 publishes an event to Worker 0
        barrier.notePublished(WorkerId{0});
        assert(barrier.snapshot().epoch == 1);
        assert(!barrier.snapshot().all_quiescent);

        // Worker 0 tries to commit with stale epoch 0 -> fails!
        assert(barrier.tryMarkQuiescent(WorkerId{0}, 0) == false);

        // Worker 0 commits with new epoch 1 -> succeeds
        assert(barrier.tryMarkQuiescent(WorkerId{0}, 1) == true);
        assert(!barrier.snapshot().all_quiescent);

        // Worker 1 commits with epoch 1 -> all quiescent!
        assert(barrier.tryMarkQuiescent(WorkerId{1}, 1) == true);
        assert(barrier.snapshot().all_quiescent);

        // Worker 0 finds work and marks active -> all quiescent is false
        barrier.markActive(WorkerId{0});
        assert(!barrier.snapshot().all_quiescent);

        // Abort
        barrier.abort();
        assert(barrier.snapshot().aborted);
        assert(!barrier.snapshot().all_quiescent);
        assert(barrier.tryMarkQuiescent(WorkerId{0}, 1) == false);
    }

    void test_barrier_note_published_increments_epoch_even_when_already_active()
    {
        WorkerQuiescenceBarrier barrier(2);
        barrier.arm();
        assert(barrier.snapshot().epoch == 0);

        // Worker 0 is active (initial state)
        barrier.notePublished(WorkerId{0});
        assert(barrier.snapshot().epoch == 1);

        barrier.notePublished(WorkerId{0});
        assert(barrier.snapshot().epoch == 2);
    }

    void test_worker_group_cross_worker_tell_exact_accounting_and_quiescence()
    {
        const WorkerActorConfig actor_config{
            .actor_table_capacity = 20,
            .max_mailbox_messages_per_actor = 20,
            .max_mailbox_bytes_per_actor = 4096,
            .max_mailbox_messages_total = 40,
            .max_mailbox_bytes_total = 8192,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            .await_timeout = 2000ms,
            .max_concurrent_loading = 10,
        };

        const WorkerGroupConfig group_config{
            .worker_count = 2,
            .max_workers = 32,
            .port = 0,
            .budgets = WorkerBudgets::defaults(),
            .inbox = WorkerInboxConfig{},
            .network = WorkerNetworkConfig{},
            .actor = actor_config,
        };

        auto gate = std::make_shared<ShutdownActorGate>();

        ActorKey k0{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k0, 2, actor_config.placement_seed) != WorkerId{0})
        {
            ++k0.entity;
        }

        ActorKey k1{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k1, 2, actor_config.placement_seed) != WorkerId{1})
        {
            ++k1.entity;
        }

        auto factory_factory = [gate, k1](const WorkerId id) -> std::unique_ptr<ActorFactory>
        {
            if (id == WorkerId{0})
            {
                return std::make_unique<FunctionalActorFactory>(
                    [gate, k1](ActorKey) -> ActorConstructionResult
                    {
                        auto actor = std::make_unique<FunctionalActor>(
                            [gate, k1](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                            {
                                {
                                    std::unique_lock lock{gate->mutex};
                                    gate->source_dispatch_started = true;
                                    gate->changed.notify_all();
                                    gate->changed.wait(
                                        lock,
                                        [gate]
                                        {
                                            return gate->release_source;
                                        }
                                    );
                                }

                                EffectBatch effects;
                                effects.push(TellActorEffect{.target = k1, .message = makeEnvelope(32)});
                                return CompletedTurn{.effects = std::move(effects)};
                            }
                        );
                        return ActorConstructionResult::ready(std::move(actor));
                    }
                );
            }
            return std::make_unique<FunctionalActorFactory>(
                [gate](ActorKey) -> ActorConstructionResult
                {
                    auto actor = std::make_unique<FunctionalActor>(
                        [gate](ActorEnvelope&&, const ActorTurnContext&) -> TurnResult
                        {
                            gate->target_turns.fetch_add(1, std::memory_order_release);
                            gate->changed.notify_all();
                            return CompletedTurn{.effects = EffectBatch{}};
                        }
                    );
                    return ActorConstructionResult::ready(std::move(actor));
                }
            );
        };

        WorkerGroup group(group_config, {}, factory_factory);
        Worker& source = WorkerGroupTestAccess::worker(group, 0);
        Worker& target = WorkerGroupTestAccess::worker(group, 1);
        assert(source.tell(k0, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(target.tell(k1, makeEnvelope(16)) == DeliveryResult::Accepted);

        group.start();

        {
            std::unique_lock lock{gate->mutex};
            assert(gate->changed.wait_for(
                lock,
                2s,
                [gate]
                {
                    return gate->source_dispatch_started && gate->target_turns.load(std::memory_order_acquire) == 1;
                }
            ));
        }

        group.requestStop();
        {
            std::lock_guard lock{gate->mutex};
            gate->release_source = true;
        }
        gate->changed.notify_all();
        group.join();

        const auto& w0 = group.worker(0);
        const auto& w1 = group.worker(1);

        const std::uint64_t total_sent = w0.metrics().actor.remote_tells_sent + w1.metrics().actor.remote_tells_sent;
        const std::uint64_t total_received = w0.metrics().actor.remote_tells_received + w1.metrics().actor.remote_tells_received;
        const std::uint64_t total_delivered = w0.metrics().actor.remote_tells_delivered + w1.metrics().actor.remote_tells_delivered;

        assert(total_sent == 1);
        assert(total_received == total_sent);
        assert(total_delivered == total_received);
        assert(gate->target_turns.load(std::memory_order_acquire) == 2);
        assert(w0.metrics().actor.remote_tell_delivery_failures == 0);
        assert(w1.metrics().actor.remote_tell_delivery_failures == 0);
        assert(w0.metrics().shutdown_barrier_timeouts == 0);
        assert(w1.metrics().shutdown_barrier_timeouts == 0);
    }

    void test_worker_group_quiescence_with_suspended_actors_logical_cancel()
    {
        const WorkerActorConfig actor_config{
            .actor_table_capacity = 20,
            .max_mailbox_messages_per_actor = 20,
            .max_mailbox_bytes_per_actor = 4096,
            .max_mailbox_messages_total = 40,
            .max_mailbox_bytes_total = 8192,
            .max_turns_per_actor_slice = 30,
            .placement_seed = 0,
            .worker_shutdown_timeout = 2000ms,
            // Keep the natural await deadline well outside this test's 2-second
            // observation window so shutdown cancellation is the only completion source.
            .await_timeout = 30s,
            .max_concurrent_loading = 10,
        };

        const WorkerGroupConfig group_config{
            .worker_count = 2,
            .max_workers = 32,
            .port = 0,
            .budgets = WorkerBudgets::defaults(),
            .inbox = WorkerInboxConfig{},
            .network = WorkerNetworkConfig{},
            .actor = actor_config,
        };

        auto observation = std::make_shared<SuspensionObservation>();

        ActorKey k0{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k0, 2, actor_config.placement_seed) != WorkerId{0})
        {
            ++k0.entity;
        }

        ActorKey k1{.kind = ActorKind::Player, .entity = 1};
        while (ownerOf(k1, 2, actor_config.placement_seed) != WorkerId{1})
        {
            ++k1.entity;
        }

        auto factory_factory = [observation](WorkerId) -> std::unique_ptr<ActorFactory>
        {
            return std::make_unique<FunctionalActorFactory>(
                [observation](ActorKey) -> ActorConstructionResult
                {
                    auto actor = std::make_unique<SuspendingActor>(
                        [observation]() -> ActorTask
                        {
                            observation->suspended.fetch_add(1, std::memory_order_release);
                            observation->changed.notify_all();
                            SyntheticAwait awaiter;
                            auto outcome = co_await awaiter;
                            if (outcome == SyntheticAwaitOutcome::Cancelled)
                            {
                                observation->cancelled.fetch_add(1, std::memory_order_release);
                                observation->changed.notify_all();
                            }
                            co_return CompletedTurn{.effects = EffectBatch{}};
                        }
                    );
                    return ActorConstructionResult::ready(std::move(actor));
                }
            );
        };

        WorkerGroup group(group_config, {}, factory_factory);
        assert(WorkerGroupTestAccess::worker(group, 0).tell(k0, makeEnvelope(16)) == DeliveryResult::Accepted);
        assert(WorkerGroupTestAccess::worker(group, 1).tell(k1, makeEnvelope(16)) == DeliveryResult::Accepted);
        group.start();

        {
            std::unique_lock lock{observation->mutex};
            assert(observation->changed.wait_for(
                lock,
                2s,
                [observation]
                {
                    return observation->suspended.load(std::memory_order_acquire) == 2;
                }
            ));
        }

        group.requestStop();
        group.join();

        const auto& w0 = group.worker(0);
        const auto& w1 = group.worker(1);
        assert(observation->suspended.load(std::memory_order_acquire) == 2);
        assert(observation->cancelled.load(std::memory_order_acquire) == 2);
        assert(w0.metrics().shutdown_barrier_timeouts == 0);
        assert(w1.metrics().shutdown_barrier_timeouts == 0);
    }
}

void run_worker_actor_tests()
{
    test_actor_table_hard_cap_and_reservation_rollback();
    test_incarnation_never_reused_after_rollback();
    test_stale_actor_handle_detection();
    test_nullable_actorslot_commit_instance_contract();
    test_actor_mailbox_limits_per_actor_and_worker_total();
    test_actor_mailbox_byte_limit_cannot_be_underreported();
    test_actor_construction_rejected_rollback_and_retry();
    test_actor_factory_exception_rollback_and_fail_fast();
    test_actor_factory_invalid_ready_result_rolls_back_and_fails_fast();
    test_missing_actor_first_message_exact_once();
    test_same_actor_key_construct_called_once();

    test_ready_actor_queue_bounded_and_fifo();
    test_actor_slice_limit_max_30_turns();
    test_fair_scheduling_hot_31_cold_1();
    test_duplicate_ready_entry_prevented();
    test_non_reentrancy_self_tell_queued_not_inline();
    test_actor_phase_max_count_limits_turns();
    test_actor_phase_duration_budget_stops_early();

    test_effect_batch_hard_cap_64_throws_on_65();
    test_stop_effect_rejects_subsequent_self_tell_but_applies_send();
    test_wrong_owner_tell_fails_explicitly_without_remote_transport();
    test_stale_connection_send_close_continue_subsequent_effects();
    test_all_four_effects_ordering_and_continue_after_failure();

    test_shutdown_single_absolute_deadline_unification();
    test_shutdown_external_deliver_closed_internal_tell_allowed();
    test_shutdown_processes_accepted_inbox_events_in_quiescence_loop();
    test_shutdown_cleans_up_all_resources_after_deadline();

    test_10000_actors_deterministic_execution();

    // 5A Tests
    test_suspended_actor_does_not_dispatch_next_mailbox_command();
    test_completion_does_not_inline_resume_only_in_actor_phase();
    test_duplicate_completion_dropped();
    test_stale_operation_id_and_incarnation_completion_dropped();
    test_resume_only_occurs_via_ready_queue();
    test_chained_sequential_suspensions_replace_blocked_correctly();
    test_coroutine_frame_destroyed_exactly_once();
    test_exception_in_coroutine_rethrows_at_resume_boundary();
    test_suspended_actor_removed_resets_mailbox_accounting();
    test_resumed_stop_effect_destroys_frame_before_actor_instance();

    // 5B Tests
    test_loading_actor_does_not_dispatch_messages_and_enqueues_to_mailbox();
    test_activation_success_transitions_to_idle_or_queued_based_on_mailbox();
    test_activation_load_enforces_the_same_mailbox_byte_cap_as_local_delivery();
    test_activation_failure_discards_mailbox_releases_slot_and_resets_accounting();
    test_concurrent_loading_cap_exceeded_returns_false_and_increments_metric();
    test_loading_stale_and_duplicate_completions_dropped();
    test_loading_count_invariant_across_all_lifecycle_paths();

    // 5C Tests
    test_completion_first_then_late_timeout_stale_drop();
    test_timeout_first_then_late_backend_completion_stale_drop();
    test_loading_actor_timeout_then_late_activation_completion_stale_drop();
    test_actor_removal_then_late_completion_drop();
    test_slot_reuse_old_incarnation_completion_drop();
    test_operation_N_timeout_then_operation_N_plus_1_completion_race();
    test_timer_reservation_failure_command_resumes_rejected_activation_fails_fast();
    test_shutdown_logical_cancel_unwinds_coroutine_and_applies_effects();
    test_shutdown_logical_cancel_reaches_a_suspended_actor_with_queued_mail();
    test_shutdown_new_suspension_immediately_cancelled_no_long_term_blocked();
    test_shutdown_forced_destruction_on_deadline_expiry_metrics();

    // 6A Tests
    test_tell_local_mailbox_non_reentrancy_and_no_self_lane();
    test_tell_remote_routes_to_target_inbox_fifo_and_bounds();
    test_tell_remote_inbox_full_and_charge_overflow_rejections();
    test_tell_actor_effect_remote_routing();
    test_remote_actor_message_without_runtime_and_misrouted_metrics();
    test_tell_closed_when_stopping_or_port_unbound();

    // 6B Tests
    test_shutdown_phase_a_rejects_stale_readable_events();
    test_barrier_unit_contracts();
    test_barrier_note_published_increments_epoch_even_when_already_active();
    test_worker_group_cross_worker_tell_exact_accounting_and_quiescence();
    test_worker_group_quiescence_with_suspended_actors_logical_cancel();
}
