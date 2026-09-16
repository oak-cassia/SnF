#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <type_traits>

namespace snf::worker
{
    // SpscRing 불변 조건:
    // - producer는 ProducerState만, consumer는 ConsumerState만 store한다.
    // - 두 상태 구조체는 false sharing을 방지하기 위해 서로 다른 cache line(alignas(64))에 배치된다.
    // - Capacity는 2의 거듭제곱이어야 하며, 인덱스는 position & (Capacity - 1)로 계산한다.
    // - push/pop 경로에서 동적 메모리 할당을 수행하지 않는다.
    // - tail과 head는 monotonic std::uint64_t 카운터이므로 wraparound를 별도로 처리하지 않는다.
    //
    // tryPush 계약:
    // - true: value를 slot으로 이동(move)시켰다.
    // - false: 링이 가득 찼으며, value를 전혀 건드리지 않았다 (호출자가 그대로 재사용 가능).
    template <class T, std::size_t Capacity> class SpscRing
    {
        static_assert(std::has_single_bit(Capacity), "Capacity must be a power of two");
        static_assert(std::is_nothrow_move_constructible_v<T>);
        static_assert(std::is_nothrow_destructible_v<T>);

        struct Slot
        {
            alignas(T) std::byte storage[sizeof(T)];
        };

        struct alignas(64) ProducerState
        {
            std::atomic<std::uint64_t> tail{0};
        };

        struct alignas(64) ConsumerState
        {
            std::atomic<std::uint64_t> head{0};
        };

    public:
        SpscRing() = default;

        ~SpscRing()
        {
            std::uint64_t head = _consumer.head.load(std::memory_order_relaxed);
            const std::uint64_t tail = _producer.tail.load(std::memory_order_relaxed);
            while (head != tail)
            {
                const std::size_t index = static_cast<std::size_t>(head & (Capacity - 1));
                std::destroy_at(reinterpret_cast<T*>(_slots[index].storage));
                ++head;
            }
        }

        SpscRing(const SpscRing&) = delete;
        SpscRing& operator=(const SpscRing&) = delete;
        SpscRing(SpscRing&&) = delete;
        SpscRing& operator=(SpscRing&&) = delete;

        // producer thread 전용
        [[nodiscard]] bool tryPush(T&& value) noexcept
        {
            const std::uint64_t tail = _producer.tail.load(std::memory_order_relaxed);
            const std::uint64_t head = _consumer.head.load(std::memory_order_acquire);
            if (tail - head >= Capacity)
            {
                return false;
            }

            const std::size_t index = static_cast<std::size_t>(tail & (Capacity - 1));
            std::construct_at(reinterpret_cast<T*>(_slots[index].storage), std::move(value));
            _producer.tail.store(tail + 1, std::memory_order_release);
            return true;
        }

        // consumer thread 전용
        [[nodiscard]] bool tryPop(T& output) noexcept
        {
            const std::uint64_t head = _consumer.head.load(std::memory_order_relaxed);
            const std::uint64_t tail = _producer.tail.load(std::memory_order_acquire);
            if (head == tail)
            {
                return false;
            }

            const std::size_t index = static_cast<std::size_t>(head & (Capacity - 1));
            auto* ptr = reinterpret_cast<T*>(_slots[index].storage);
            output = std::move(*ptr);
            std::destroy_at(ptr);
            _consumer.head.store(head + 1, std::memory_order_release);
            return true;
        }

        [[nodiscard]] std::size_t approximateSize() const noexcept
        {
            const std::uint64_t tail = _producer.tail.load(std::memory_order_relaxed);
            const std::uint64_t head = _consumer.head.load(std::memory_order_relaxed);
            return tail >= head ? static_cast<std::size_t>(tail - head) : 0;
        }

    private:
        ProducerState _producer;
        ConsumerState _consumer;
        Slot _slots[Capacity];
    };
}
