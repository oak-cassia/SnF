#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/actor.hpp"
#include "snf/worker/actor_table.hpp"
#include "snf/worker/worker.hpp"
#include "snf/worker/worker_group.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <utility>
#include <vector>

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

    [[nodiscard]] ActorEnvelope makeEnvelope(const std::size_t payload_size = 16, const MessageType type = MessageType::Ping, const std::uint32_t req_id = 1)
    {
        return ActorEnvelope::fromFrame(makeFrame(payload_size, type, req_id));
    }

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
                        received_requests.push_back(env.frame.request_id);
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
        std::thread th([&]() { worker.run(); });
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

        std::thread th([&]() { worker.run(); });
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

        std::thread th([&]() { worker.run(); });
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
        std::thread th([&]() { worker.run(); });
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
                        if (env.frame.request_id == 1)
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

        std::thread th([&]() { worker.run(); });
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

        std::thread th([&]() { worker.run(); });
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

        std::thread th([&]() { worker.run(); });
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
        std::thread th([&]() { worker.run(); });

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
                            if (env.frame.request_id == 42)
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

        std::thread th([&]() { worker.run(); });
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

        std::thread th([&]() { worker.run(); });
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
                            if (env.frame.request_id == 88)
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

        std::thread th([&]() { worker.run(); });
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
            .event = RemoteConnectionClose{
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

        // Inbox event was processed in shutdown quiescence loop!
        assert(worker.metrics().inbox_events == 1 || worker.metrics().shutdown_inbox_events == 1);
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

        std::thread th([&]() { worker.run(); });
        std::this_thread::sleep_for(150ms);
        worker.requestStop();
        th.join();

        assert(total_turns.load(std::memory_order_relaxed) == ACTOR_COUNT);
        assert(worker.metrics().actor.actor_turns == ACTOR_COUNT);
        assert(worker.totalMailboxMessages() == 0);
    }
}

void run_worker_actor_tests()
{
    test_actor_table_hard_cap_and_reservation_rollback();
    test_incarnation_never_reused_after_rollback();
    test_stale_actor_handle_detection();
    test_nullable_actorslot_commit_instance_contract();
    test_actor_mailbox_limits_per_actor_and_worker_total();
    test_actor_construction_rejected_rollback_and_retry();
    test_actor_factory_exception_rollback_and_fail_fast();
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
}
