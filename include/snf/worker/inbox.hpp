#pragma once

#include "snf/worker/budget.hpp"
#include "snf/worker/identity.hpp"
#include "snf/worker/wakeup.hpp"
#include "snf/worker/worker_event.hpp"

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <type_traits>

namespace snf::worker
{
    struct WorkerEnvelope
    {
        WorkerEvent event;
        std::uint32_t charged_bytes{0};
    };

    static_assert(std::is_nothrow_move_constructible_v<WorkerEnvelope>);
    static_assert(std::is_nothrow_destructible_v<WorkerEnvelope>);

    enum class InboxPushResult : std::uint8_t
    {
        Accepted = 0,
        Full = 1, // event 수 또는 byte 상한 초과
        Closed = 2,
    };

    struct WorkerInboxConfig
    {
        std::uint64_t max_bytes_per_worker{64ull * 1024 * 1024};
        std::uint16_t max_workers{32};
    };

    struct DrainResult
    {
        std::size_t processed{0};
        bool has_more{false};         // 아직 처리할 event가 남았다
        bool budget_exhausted{false}; // count 또는 time budget 때문에 중단했다
    };

    // Lifetime 불변식:
    // 모든 WorkerInboxPort는 대상 WorkerInbox와 WakeupHandle보다 먼저 사용을 중단해야 한다.
    // 권장 shutdown 순서:
    // 1. 모든 Worker에 stop 요청
    // 2. Worker thread가 신규 application 작업 중단
    // 3. accepted inbox event drain
    // 4. 모든 Worker thread join
    // 5. WorkerInboxPort 폐기
    // 6. Worker / WorkerInbox / WakeupHandle 파괴
    //
    // close() 계약:
    // - close 이후 새로 시작한 push는 Closed를 반환한다.
    // - close와 이미 진행 중이던 push는 Accepted가 될 수 있다.
    // - Accepted를 반환한 event는 shutdown drain 대상이며 유실되지 않는다.
    // - Inbox 파괴는 모든 producer thread가 종료된 뒤에만 한다.
    class InboxLane
    {
    public:
        static constexpr std::size_t CAPACITY = 1024;

        explicit InboxLane(std::uint64_t max_queued_bytes = 0) noexcept;
        ~InboxLane();

        InboxLane(const InboxLane&) = delete;
        InboxLane& operator=(const InboxLane&) = delete;
        InboxLane(InboxLane&&) = delete;
        InboxLane& operator=(InboxLane&&) = delete;

        void configure(std::uint64_t max_queued_bytes) noexcept;

        [[nodiscard]] InboxPushResult tryPush(WorkerEnvelope&& envelope) noexcept; // producer 전용
        [[nodiscard]] bool tryPop(WorkerEnvelope& out) noexcept;                   // consumer 전용
        void close() noexcept;
        [[nodiscard]] std::uint64_t approximateQueuedBytes() const noexcept;
        [[nodiscard]] std::uint64_t maxQueuedBytes() const noexcept;
        [[nodiscard]] bool isEmpty() const noexcept;

    private:
        struct Slot
        {
            alignas(WorkerEnvelope) std::byte storage[sizeof(WorkerEnvelope)];
        };

        struct alignas(64) ProducerState
        {
            std::atomic<std::uint64_t> tail{0};
            std::atomic<std::uint64_t> produced_bytes{0};
        };

        struct alignas(64) ConsumerState
        {
            std::atomic<std::uint64_t> head{0};
            std::atomic<std::uint64_t> consumed_bytes{0};
        };

        std::uint64_t _max_queued_bytes{0};
        std::atomic<bool> _closed{false};
        ProducerState _producer;
        ConsumerState _consumer;
        Slot _slots[CAPACITY];
    };

    class WorkerInbox;

    class WorkerInboxPort
    {
    public:
        WorkerInboxPort() = default;

        [[nodiscard]] bool isBound() const noexcept;
        [[nodiscard]] InboxPushResult tryPush(WorkerEnvelope&& envelope) noexcept;

    private:
        friend class WorkerInbox;
        InboxLane* _lane{nullptr};
        WakeupHandle* _wakeup{nullptr};
    };

    class WorkerInbox
    {
    public:
        WorkerInbox(std::uint16_t worker_count, WakeupHandle& wakeup, const WorkerInboxConfig& config);

        WorkerInbox(const WorkerInbox&) = delete;
        WorkerInbox& operator=(const WorkerInbox&) = delete;

        // startup 전용. Worker thread가 하나라도 시작된 뒤에 호출하지 않는다.
        // 같은 source에 두 번 호출하면 debug 빌드에서 assert로 잡는다.
        [[nodiscard]] WorkerInboxPort bindSource(WorkerId source) noexcept;

        template <class Handler> [[nodiscard]] DrainResult drain(const InboxBudget& budget, Handler&& handler)
        {
            DrainResult result{};
            if (_worker_count == 0)
            {
                return result;
            }

            const auto start_time = std::chrono::steady_clock::now();
            std::size_t lanes_empty_consecutively = 0;

            while (result.processed < budget.max_events && lanes_empty_consecutively < _worker_count)
            {
                const std::size_t lane_idx = _next_lane;
                _next_lane = (_next_lane + 1) % _worker_count;

                auto& lane = _lanes[lane_idx];
                std::size_t lane_processed = 0;
                WorkerEnvelope envelope;

                while (lane_processed < budget.max_per_lane && result.processed < budget.max_events && lane.tryPop(envelope))
                {
                    handler(std::move(envelope.event));
                    ++lane_processed;
                    ++result.processed;

                    if ((result.processed & 63U) == 0)
                    {
                        const auto current_time = std::chrono::steady_clock::now();
                        if (current_time - start_time >= budget.max_duration)
                        {
                            result.budget_exhausted = true;
                            break;
                        }
                    }
                }

                if (lane_processed > 0)
                {
                    lanes_empty_consecutively = 0;
                }
                else
                {
                    ++lanes_empty_consecutively;
                }

                if (result.budget_exhausted)
                {
                    break;
                }

                if (result.processed >= budget.max_events)
                {
                    result.budget_exhausted = true;
                    break;
                }

                const auto current_time = std::chrono::steady_clock::now();
                if (current_time - start_time >= budget.max_duration)
                {
                    result.budget_exhausted = true;
                    break;
                }
            }

            // Check if more items remain across any lane
            for (std::size_t i = 0; i < _worker_count; ++i)
            {
                if (!_lanes[i].isEmpty())
                {
                    result.has_more = true;
                    break;
                }
            }

            return result;
        }

        void close() noexcept;

        [[nodiscard]] bool isEmpty() const noexcept;
        [[nodiscard]] std::uint64_t approximateQueuedBytes() const noexcept;
        [[nodiscard]] std::uint64_t maxQueuedBytesTotal() const noexcept;
        [[nodiscard]] std::uint16_t workerCount() const noexcept;
        [[nodiscard]] InboxLane& lane(std::size_t index) noexcept;
        [[nodiscard]] const InboxLane& lane(std::size_t index) const noexcept;

    private:
        std::unique_ptr<InboxLane[]> _lanes;
        std::unique_ptr<bool[]> _bound;
        std::uint16_t _worker_count;
        WakeupHandle* _wakeup;
        std::size_t _next_lane{0};
    };
}
