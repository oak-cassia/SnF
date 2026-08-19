#include "snf/runtime/bounded_queue.hpp"

#include <cassert>
#include <chrono>
#include <future>
#include <thread>

namespace
{
    using namespace std::chrono_literals;

    void test_preserves_fifo_order_and_enforces_capacity()
    {
        snf::runtime::BoundedQueue<int> queue{2};

        assert(queue.tryPush(10));
        assert(queue.tryPush(20));
        assert(!queue.tryPush(30));
        assert(queue.size() == 2);
        assert(queue.highWaterMark() == 2);

        const auto first = queue.tryPop();
        const auto second = queue.tryPop();
        assert(first && *first == 10);
        assert(second && *second == 20);
        assert(!queue.tryPop());
        assert(queue.highWaterMark() == 2);
    }

    void test_close_rejects_new_items_after_draining_existing_items()
    {
        snf::runtime::BoundedQueue<int> queue{2};
        assert(queue.tryPush(1));
        assert(queue.tryPush(2));

        queue.close();

        assert(!queue.tryPush(3));
        const auto first = queue.pop();
        const auto second = queue.pop();
        assert(first && *first == 1);
        assert(second && *second == 2);
        assert(!queue.pop());
    }

    void test_cancel_releases_blocked_producer_and_consumer()
    {
        {
            snf::runtime::BoundedQueue<int> queue{1};
            assert(queue.tryPush(1));
            std::promise<bool> push_result;
            auto future = push_result.get_future();
            std::thread producer{[&queue, &push_result] { push_result.set_value(queue.push(2)); }};

            assert(future.wait_for(100ms) == std::future_status::timeout);
            queue.cancel();
            assert(!future.get());
            producer.join();
        }

        {
            snf::runtime::BoundedQueue<int> queue{1};
            std::promise<bool> pop_result;
            auto future = pop_result.get_future();
            std::thread consumer{[&queue, &pop_result] { pop_result.set_value(queue.pop().has_value()); }};

            assert(future.wait_for(100ms) == std::future_status::timeout);
            queue.cancel();
            assert(!future.get());
            consumer.join();
        }
    }
}

void run_bounded_queue_tests()
{
    test_preserves_fifo_order_and_enforces_capacity();
    test_close_rejects_new_items_after_draining_existing_items();
    test_cancel_releases_blocked_producer_and_consumer();
}
