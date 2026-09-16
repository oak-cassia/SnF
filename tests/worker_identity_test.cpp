#include "snf/worker/identity.hpp"
#include "snf/worker/stale.hpp"

#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>

namespace
{
    using namespace snf::worker;

    static_assert(static_cast<std::uint8_t>(ActorKind::ProvisionalPlayer) == 0);
    static_assert(static_cast<std::uint8_t>(ActorKind::Player) == 1);
    static_assert(static_cast<std::uint8_t>(ActorKind::Zone) == 2);
    static_assert(static_cast<std::uint8_t>(ActorKind::Room) == 3);

    // Placement 계약을 컴파일 타임에 고정한다. 값이 바뀌면 모든 Actor의 Worker 배치가 바뀐다.
    // static_assert이므로 stableActorHash와 ownerOf가 상수 문맥에서 평가 가능하다는 것도 함께 보장한다.
    static_assert(stableActorHash(ActorKey{ActorKind::Player, 1}, 0x1234ULL) == 0xde3cadc2daf870fdULL);
    static_assert(stableActorHash(ActorKey{ActorKind::Room, 42}, 0x1234ULL) == 0x177aaa504a0126caULL);
    static_assert(stableActorHash(ActorKey{ActorKind::Zone, 7}, 0x1234ULL) == 0x897deea11364fbf0ULL);
    static_assert(stableActorHash(ActorKey{ActorKind::Player, 1}, 0ULL) == 0xe4d971771b652c20ULL);
    static_assert(stableActorHash(ActorKey{ActorKind::Room, 42}, 0ULL) == 0x5fd30d2fcbef75e3ULL);
    static_assert(stableActorHash(ActorKey{ActorKind::Zone, 7}, 0ULL) == 0xb3466f8a7b81a989ULL);

    static_assert(ownerOf(ActorKey{ActorKind::Player, 1}, 8, 0x1234ULL) == WorkerId{5});
    static_assert(ownerOf(ActorKey{ActorKind::Room, 42}, 8, 0x1234ULL) == WorkerId{2});
    static_assert(ownerOf(ActorKey{ActorKind::Zone, 7}, 8, 0x1234ULL) == WorkerId{0});

    void test_default_ids_are_invalid()
    {
        const ActorIncarnation incarnation{};
        assert(incarnation.value == 0);
        assert(!incarnation.isValid());

        const OperationId operation{};
        assert(operation.value == 0);
        assert(!operation.isValid());

        const ConnectionGeneration generation{};
        assert(generation.value == 0);
        assert(!generation.isValid());
    }

    void test_sequence_source_starts_at_one()
    {
        IncarnationSource incarnation_source;
        assert(incarnation_source.last().value == 0);
        const auto inc1 = incarnation_source.next();
        assert(inc1.value == 1);
        assert(inc1.isValid());
        assert(incarnation_source.last() == inc1);
        const auto inc2 = incarnation_source.next();
        assert(inc2.value == 2);
        assert(incarnation_source.last() == inc2);

        OperationIdSource operation_source;
        assert(operation_source.last().value == 0);
        const auto op1 = operation_source.next();
        assert(op1.value == 1);
        assert(op1.isValid());
        assert(operation_source.last() == op1);
        const auto op2 = operation_source.next();
        assert(op2.value == 2);
        assert(operation_source.last() == op2);

        ConnectionGenerationSource generation_source;
        assert(generation_source.last().value == 0);
        const auto gen1 = generation_source.next();
        assert(gen1.value == 1);
        assert(gen1.isValid());
        assert(generation_source.last() == gen1);
        const auto gen2 = generation_source.next();
        assert(gen2.value == 2);
        assert(generation_source.last() == gen2);
    }

    void test_operation_ids_are_never_reused()
    {
        OperationIdSource source;
        OperationId previous = source.next();
        assert(previous.value == 1);
        for (std::size_t i = 1; i < 1000; ++i)
        {
            const OperationId current = source.next();
            assert(current.isValid());
            assert(current.value == previous.value + 1);
            assert(!(current == previous));
            previous = current;
        }
        assert(source.last().value == 1000);
    }

    void test_actor_key_equality()
    {
        const ActorKey key1{ActorKind::Player, 100};
        const ActorKey key2{ActorKind::Player, 100};
        const ActorKey key_diff_kind{ActorKind::Room, 100};
        const ActorKey key_diff_entity{ActorKind::Player, 101};

        assert(key1 == key2);
        assert(!(key1 == key_diff_kind));
        assert(!(key1 == key_diff_entity));
    }

    void test_actor_key_hash_matches_stable_hash()
    {
        const ActorKey keys[] = {
            {ActorKind::ProvisionalPlayer, 0},
            {ActorKind::Player, 1},
            {ActorKind::Zone, 7},
            {ActorKind::Room, 42},
        };
        const ActorKeyHash hasher;
        for (const auto& key : keys)
        {
            assert(hasher(key) == static_cast<std::size_t>(stableActorHash(key, 0)));
        }
    }

    void test_owner_of_is_deterministic_and_in_range()
    {
        const ActorKey key{ActorKind::Player, 12345};
        constexpr std::uint64_t seed = 0x5678ULL;
        for (std::uint16_t worker_count = 1; worker_count <= 16; ++worker_count)
        {
            const WorkerId owner1 = ownerOf(key, worker_count, seed);
            const WorkerId owner2 = ownerOf(key, worker_count, seed);
            assert(owner1 == owner2);
            assert(owner1.value < worker_count);
        }
    }

    void test_owner_of_with_single_worker_is_always_zero()
    {
        for (std::uint64_t entity = 0; entity < 50; ++entity)
        {
            assert(ownerOf(ActorKey{ActorKind::Player, entity}, 1, 0) == WorkerId{0});
            assert(ownerOf(ActorKey{ActorKind::Room, entity}, 1, 0x1234) == WorkerId{0});
            assert(ownerOf(ActorKey{ActorKind::Zone, entity}, 1, 0x5678) == WorkerId{0});
            assert(ownerOf(ActorKey{ActorKind::ProvisionalPlayer, entity}, 1, 0x9abc) == WorkerId{0});
        }
    }

    void test_owner_of_uses_every_worker()
    {
        std::array<std::size_t, 8> counts{};
        constexpr std::uint64_t seed = 0x1234ULL;
        constexpr std::uint16_t worker_count = 8;
        for (std::uint64_t entity = 0; entity < 256; ++entity)
        {
            const ActorKey key{ActorKind::Player, entity};
            const WorkerId owner = ownerOf(key, worker_count, seed);
            assert(owner.value < worker_count);
            counts[owner.value]++;
        }
        for (const std::size_t count : counts)
        {
            assert(count >= 8 && count <= 64);
        }
    }

    void test_completion_matching_await_key_is_accepted()
    {
        const AwaitKey key{ActorKey{ActorKind::Player, 10}, ActorIncarnation{1}, OperationId{5}};
        assert(acceptsCompletion(key, key));
    }

    void test_completion_from_old_incarnation_is_rejected()
    {
        const AwaitKey arriving{ActorKey{ActorKind::Player, 10}, ActorIncarnation{1}, OperationId{5}};
        const AwaitKey blocked{ActorKey{ActorKind::Player, 10}, ActorIncarnation{2}, OperationId{5}};
        assert(!acceptsCompletion(arriving, blocked));
    }

    void test_completion_with_stale_operation_is_rejected()
    {
        const AwaitKey arriving{ActorKey{ActorKind::Player, 10}, ActorIncarnation{1}, OperationId{4}};
        const AwaitKey blocked{ActorKey{ActorKind::Player, 10}, ActorIncarnation{1}, OperationId{5}};
        assert(!acceptsCompletion(arriving, blocked));
    }

    void test_completion_for_other_actor_is_rejected()
    {
        const AwaitKey arriving{ActorKey{ActorKind::Player, 10}, ActorIncarnation{1}, OperationId{5}};
        const AwaitKey blocked{ActorKey{ActorKind::Player, 20}, ActorIncarnation{1}, OperationId{5}};
        assert(!acceptsCompletion(arriving, blocked));
    }

    void test_completion_for_other_actor_kind_is_rejected()
    {
        const AwaitKey arriving{ActorKey{ActorKind::Player, 10}, ActorIncarnation{1}, OperationId{5}};
        const AwaitKey blocked{ActorKey{ActorKind::Room, 10}, ActorIncarnation{1}, OperationId{5}};
        assert(!acceptsCompletion(arriving, blocked));
    }

    void test_completion_with_invalid_incarnation_is_rejected()
    {
        const AwaitKey key_invalid_inc{ActorKey{ActorKind::Player, 10}, ActorIncarnation{0}, OperationId{5}};
        assert(!acceptsCompletion(key_invalid_inc, key_invalid_inc));
    }

    void test_completion_with_invalid_operation_is_rejected()
    {
        const AwaitKey key_invalid_op{ActorKey{ActorKind::Player, 10}, ActorIncarnation{1}, OperationId{0}};
        assert(!acceptsCompletion(key_invalid_op, key_invalid_op));
    }

    void test_activation_event_from_old_incarnation_is_rejected()
    {
        const ActivationRef old_ref{ActorKey{ActorKind::Player, 1}, ActorIncarnation{1}};
        const ActivationRef cur_ref{ActorKey{ActorKind::Player, 1}, ActorIncarnation{2}};
        const ActorIncarnation current{2};

        assert(!acceptsActivationEvent(old_ref, current));
        assert(acceptsActivationEvent(cur_ref, current));
    }

    void test_activation_event_with_invalid_incarnation_is_rejected()
    {
        const ActivationRef invalid_ref{ActorKey{ActorKind::Player, 1}, ActorIncarnation{0}};
        assert(!acceptsActivationEvent(invalid_ref, ActorIncarnation{0}));
        assert(!acceptsActivationEvent(invalid_ref, ActorIncarnation{1}));
        const ActivationRef valid_ref{ActorKey{ActorKind::Player, 1}, ActorIncarnation{1}};
        assert(!acceptsActivationEvent(valid_ref, ActorIncarnation{0}));
    }

    void test_connection_action_with_old_generation_is_rejected()
    {
        ConnectionGenerationSource gen_source;
        const ConnectionGeneration gen1 = gen_source.next();
        const ConnectionRef conn1{ConnectionId{42}, gen1, WorkerId{0}};

        const ConnectionGeneration gen2 = gen_source.next();
        const ConnectionRef conn2{ConnectionId{42}, gen2, WorkerId{0}};

        assert(!acceptsConnectionAction(conn1, gen2));
        assert(acceptsConnectionAction(conn2, gen2));
    }

    void test_connection_action_with_invalid_target_generation_is_rejected()
    {
        const ConnectionRef invalid_ref{ConnectionId{42}, ConnectionGeneration{0}, WorkerId{0}};
        assert(!acceptsConnectionAction(invalid_ref, ConnectionGeneration{1}));
        assert(!acceptsConnectionAction(invalid_ref, ConnectionGeneration{0}));
    }

    void test_connection_action_with_invalid_current_generation_is_rejected()
    {
        const ConnectionRef valid_ref{ConnectionId{42}, ConnectionGeneration{1}, WorkerId{0}};
        assert(!acceptsConnectionAction(valid_ref, ConnectionGeneration{0}));
    }

    void test_connection_action_ignores_owner()
    {
        const ConnectionRef conn_worker0{ConnectionId{42}, ConnectionGeneration{1}, WorkerId{0}};
        const ConnectionRef conn_worker1{ConnectionId{42}, ConnectionGeneration{1}, WorkerId{1}};
        const ConnectionGeneration current{1};

        assert(acceptsConnectionAction(conn_worker0, current));
        assert(acceptsConnectionAction(conn_worker1, current));
    }
}

void run_worker_identity_tests()
{
    test_default_ids_are_invalid();
    test_sequence_source_starts_at_one();
    test_operation_ids_are_never_reused();
    test_actor_key_equality();
    test_actor_key_hash_matches_stable_hash();
    test_owner_of_is_deterministic_and_in_range();
    test_owner_of_with_single_worker_is_always_zero();
    test_owner_of_uses_every_worker();
    test_completion_matching_await_key_is_accepted();
    test_completion_from_old_incarnation_is_rejected();
    test_completion_with_stale_operation_is_rejected();
    test_completion_for_other_actor_is_rejected();
    test_completion_for_other_actor_kind_is_rejected();
    test_completion_with_invalid_incarnation_is_rejected();
    test_completion_with_invalid_operation_is_rejected();
    test_activation_event_from_old_incarnation_is_rejected();
    test_activation_event_with_invalid_incarnation_is_rejected();
    test_connection_action_with_old_generation_is_rejected();
    test_connection_action_with_invalid_target_generation_is_rejected();
    test_connection_action_with_invalid_current_generation_is_rejected();
    test_connection_action_ignores_owner();
}
