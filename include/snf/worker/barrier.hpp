#pragma once

#include "snf/worker/identity.hpp"

#include <atomic>
#include <cstdint>

namespace snf::worker
{
    class WorkerQuiescenceBarrier final
    {
    public:
        static constexpr std::uint16_t MAX_WORKERS = 32;
        static constexpr std::uint32_t MAX_EPOCH = (1u << 30) - 1;

        struct Snapshot
        {
            bool armed{false};
            bool all_quiescent{false};
            bool aborted{false};
            std::uint32_t epoch{0}; // 30-bit stored epoch
        };

        explicit WorkerQuiescenceBarrier(std::uint16_t worker_count = MAX_WORKERS) noexcept;

        void arm() noexcept;
        [[nodiscard]] bool armed() const noexcept;

        // Accepted publication 뒤 호출한다. target bit를 clear하고,
        // 이미 active여도 publication epoch를 반드시 증가시킨다.
        void notePublished(WorkerId target) noexcept;

        // Waiting에서 work를 발견한 자기 자신만 호출한다.
        void markActive(WorkerId me) noexcept;

        // empty scan 전에 관측한 epoch가 여전히 같을 때만 bit를 set한다.
        [[nodiscard]] bool tryMarkQuiescent(
            WorkerId me,
            std::uint32_t observed_epoch
        ) noexcept;

        [[nodiscard]] Snapshot snapshot() const noexcept;
        void abort() noexcept;

        [[nodiscard]] std::uint16_t workerCount() const noexcept
        {
            return _worker_count;
        }

    private:
        static constexpr std::uint64_t MASK_BITS = 0xFFFFFFFFULL;
        static constexpr std::uint64_t EPOCH_SHIFT = 32;
        static constexpr std::uint64_t EPOCH_MASK = static_cast<std::uint64_t>(MAX_EPOCH) << EPOCH_SHIFT;
        static constexpr std::uint64_t ARMED_BIT = 1ULL << 62;
        static constexpr std::uint64_t ABORTED_BIT = 1ULL << 63;

        std::atomic<std::uint64_t> _state{0};
        std::uint32_t _full_mask{0};
        std::uint16_t _worker_count{MAX_WORKERS};
    };
}
