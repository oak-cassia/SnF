#include "snf/worker/timer_queue.hpp"

#include <cassert>
#include <chrono>
#include <vector>

namespace
{
    using namespace snf::worker;

    void test_timer_queue_expires_in_deadline_order()
    {
        TimerQueue queue;
        const auto base_time = std::chrono::steady_clock::now();

        const AwaitKey k1{ActorKey{ActorKind::Player, 1}, ActorIncarnation{1}, OperationId{1}};
        const AwaitKey k2{ActorKey{ActorKind::Player, 2}, ActorIncarnation{1}, OperationId{2}};
        const AwaitKey k3{ActorKey{ActorKind::Player, 3}, ActorIncarnation{1}, OperationId{3}};

        assert(queue.trySchedule(base_time + std::chrono::milliseconds(300), AwaitTimeout{k3}));
        assert(queue.trySchedule(base_time + std::chrono::milliseconds(100), AwaitTimeout{k1}));
        assert(queue.trySchedule(base_time + std::chrono::milliseconds(200), AwaitTimeout{k2}));

        std::vector<std::uint64_t> expired_entities;
        const auto result = queue.expire(
            base_time + std::chrono::milliseconds(500),
            CountTimeBudget{.max_count = 10, .max_duration = std::chrono::seconds(1)},
            [&](TimerPayload&& payload)
            {
                const auto* timeout = std::get_if<AwaitTimeout>(&payload);
                assert(timeout != nullptr);
                expired_entities.push_back(timeout->key.actor.entity);
            }
        );

        assert(result.expired == 3);
        assert(!result.due_items_remain);
        assert(!result.budget_exhausted);
        assert(expired_entities.size() == 3);
        assert(expired_entities[0] == 1);
        assert(expired_entities[1] == 2);
        assert(expired_entities[2] == 3);
    }

    void test_timer_queue_does_not_expire_early()
    {
        TimerQueue queue;
        const auto base_time = std::chrono::steady_clock::now();

        const AwaitKey k1{ActorKey{ActorKind::Player, 1}, ActorIncarnation{1}, OperationId{1}};
        assert(queue.trySchedule(base_time + std::chrono::milliseconds(200), AwaitTimeout{k1}));

        std::size_t expired_count = 0;
        const auto result = queue.expire(
            base_time + std::chrono::milliseconds(100),
            CountTimeBudget{.max_count = 10, .max_duration = std::chrono::seconds(1)},
            [&](TimerPayload&&)
            {
                ++expired_count;
            }
        );

        assert(result.expired == 0);
        assert(expired_count == 0);
        assert(!result.due_items_remain);
        assert(!result.budget_exhausted);
        assert(queue.size() == 1);
    }

    void test_timer_queue_next_deadline_is_minimum()
    {
        TimerQueue queue;
        assert(!queue.nextDeadline().has_value());

        const auto base_time = std::chrono::steady_clock::now();
        const auto t1 = base_time + std::chrono::milliseconds(100);
        const auto t2 = base_time + std::chrono::milliseconds(200);

        assert(queue.trySchedule(t2, AwaitTimeout{}));
        assert(queue.nextDeadline().value() == t2);

        assert(queue.trySchedule(t1, AwaitTimeout{}));
        assert(queue.nextDeadline().value() == t1);
    }

    void test_timer_queue_respects_count_budget()
    {
        TimerQueue queue;
        const auto base_time = std::chrono::steady_clock::now();

        for (std::uint64_t i = 0; i < 20; ++i)
        {
            const AwaitKey key{ActorKey{ActorKind::Player, i}, ActorIncarnation{1}, OperationId{i + 1}};
            assert(queue.trySchedule(base_time + std::chrono::milliseconds(50), AwaitTimeout{key}));
        }

        std::size_t expired_count = 0;
        const auto result = queue.expire(
            base_time + std::chrono::milliseconds(100),
            CountTimeBudget{.max_count = 10, .max_duration = std::chrono::seconds(1)},
            [&](TimerPayload&&)
            {
                ++expired_count;
            }
        );

        assert(result.expired == 10);
        assert(expired_count == 10);
        assert(result.due_items_remain);
        assert(result.budget_exhausted);
        assert(queue.size() == 10);
    }

    void test_timer_queue_rejects_when_full()
    {
        TimerQueue queue;
        const auto base_time = std::chrono::steady_clock::now();

        for (std::size_t i = 0; i < TimerQueue::CAPACITY; ++i)
        {
            assert(queue.trySchedule(base_time + std::chrono::milliseconds(100), AwaitTimeout{}));
        }

        assert(!queue.trySchedule(base_time + std::chrono::milliseconds(100), AwaitTimeout{}));
    }

    void test_timer_queue_reservation_blocks_admission()
    {
        TimerQueue queue;
        const auto base_time = std::chrono::steady_clock::now();

        for (std::size_t i = 0; i < TimerQueue::CAPACITY; ++i)
        {
            assert(queue.tryReserve());
        }

        assert(!queue.tryReserve());
        assert(!queue.trySchedule(base_time + std::chrono::milliseconds(100), AwaitTimeout{}));
    }

    void test_timer_queue_release_restores_admission()
    {
        TimerQueue queue;
        const auto base_time = std::chrono::steady_clock::now();

        for (std::size_t i = 0; i < TimerQueue::CAPACITY; ++i)
        {
            assert(queue.tryReserve());
        }

        queue.releaseReservation();
        assert(queue.trySchedule(base_time + std::chrono::milliseconds(100), AwaitTimeout{}));
        assert(!queue.tryReserve());
    }

    void test_timer_queue_commit_after_reserve_always_succeeds()
    {
        TimerQueue queue;
        const auto base_time = std::chrono::steady_clock::now();

        for (std::size_t i = 0; i < TimerQueue::CAPACITY; ++i)
        {
            assert(queue.tryReserve());
        }

        for (std::size_t i = 0; i < TimerQueue::CAPACITY; ++i)
        {
            queue.commitReserved(base_time + std::chrono::milliseconds(i + 1), AwaitTimeout{});
        }

        assert(queue.size() == TimerQueue::CAPACITY);
    }

    void test_timer_queue_payload_survives_roundtrip()
    {
        TimerQueue queue;
        const auto base_time = std::chrono::steady_clock::now();

        const AwaitKey key{ActorKey{ActorKind::Player, 77}, ActorIncarnation{3}, OperationId{9}};
        assert(queue.trySchedule(base_time + std::chrono::milliseconds(50), AwaitTimeout{key}));

        bool matched = false;
        const auto result = queue.expire(
            base_time + std::chrono::milliseconds(100),
            CountTimeBudget{.max_count = 10, .max_duration = std::chrono::seconds(1)},
            [&](TimerPayload&& payload)
            {
                const auto* timeout = std::get_if<AwaitTimeout>(&payload);
                assert(timeout != nullptr);
                assert(timeout->key == key);
                matched = true;
            }
        );

        assert(result.expired == 1);
        assert(matched);
    }
}

void run_worker_timer_queue_tests()
{
    test_timer_queue_expires_in_deadline_order();
    test_timer_queue_does_not_expire_early();
    test_timer_queue_next_deadline_is_minimum();
    test_timer_queue_respects_count_budget();
    test_timer_queue_rejects_when_full();
    test_timer_queue_reservation_blocks_admission();
    test_timer_queue_release_restores_admission();
    test_timer_queue_commit_after_reserve_always_succeeds();
    test_timer_queue_payload_survives_roundtrip();
}
