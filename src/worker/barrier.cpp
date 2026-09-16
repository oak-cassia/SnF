#include "snf/worker/barrier.hpp"

#include <cassert>
#include <stdexcept>

namespace snf::worker
{
    WorkerQuiescenceBarrier::WorkerQuiescenceBarrier(const std::uint16_t worker_count)
        : _worker_count(worker_count)
    {
        if (worker_count == 0 || worker_count > MAX_WORKERS)
        {
            throw std::invalid_argument{"WorkerQuiescenceBarrier worker count must be between 1 and 32"};
        }
        _full_mask = (worker_count == 32) ? 0xFFFFFFFFU : ((1U << worker_count) - 1U);
        _state.store(0, std::memory_order_relaxed);
    }

    void WorkerQuiescenceBarrier::arm() noexcept
    {
        std::uint64_t curr = _state.load(std::memory_order_relaxed);
        while (true)
        {
            const std::uint64_t next = (curr & (EPOCH_MASK | ABORTED_BIT)) | ARMED_BIT;
            if (_state.compare_exchange_weak(curr, next, std::memory_order_acq_rel, std::memory_order_acquire))
            {
                break;
            }
        }
    }

    bool WorkerQuiescenceBarrier::armed() const noexcept
    {
        return (_state.load(std::memory_order_acquire) & ARMED_BIT) != 0;
    }

    void WorkerQuiescenceBarrier::notePublished(const WorkerId target) noexcept
    {
        assert(target.value < _worker_count);
        std::uint64_t curr = _state.load(std::memory_order_relaxed);
        while (true)
        {
            if ((curr & ARMED_BIT) == 0 || (curr & ABORTED_BIT) != 0)
            {
                return;
            }
            const auto epoch = static_cast<std::uint32_t>((curr >> EPOCH_SHIFT) & MAX_EPOCH);
            std::uint64_t next;
            if (epoch >= MAX_EPOCH)
            {
                next = curr | ABORTED_BIT;
            }
            else
            {
                const std::uint32_t new_epoch = epoch + 1;
                const std::uint64_t target_bit = 1ULL << target.value;
                const std::uint64_t new_mask = (curr & MASK_BITS) & ~target_bit;
                next = new_mask | (static_cast<std::uint64_t>(new_epoch) << EPOCH_SHIFT) | (curr & (ARMED_BIT | ABORTED_BIT));
            }
            if (_state.compare_exchange_weak(curr, next, std::memory_order_acq_rel, std::memory_order_acquire))
            {
                break;
            }
        }
    }

    void WorkerQuiescenceBarrier::markActive(const WorkerId me) noexcept
    {
        assert(me.value < _worker_count);
        std::uint64_t curr = _state.load(std::memory_order_relaxed);
        while (true)
        {
            if ((curr & ARMED_BIT) == 0 || (curr & ABORTED_BIT) != 0)
            {
                return;
            }
            const std::uint64_t my_bit = 1ULL << me.value;
            if ((curr & my_bit) == 0)
            {
                return;
            }
            const std::uint64_t next = curr & ~my_bit;
            if (_state.compare_exchange_weak(curr, next, std::memory_order_acq_rel, std::memory_order_acquire))
            {
                break;
            }
        }
    }

    bool WorkerQuiescenceBarrier::tryMarkQuiescent(const WorkerId me, const std::uint32_t observed_epoch) noexcept
    {
        assert(me.value < _worker_count);
        std::uint64_t curr = _state.load(std::memory_order_relaxed);
        while (true)
        {
            if ((curr & ARMED_BIT) == 0 || (curr & ABORTED_BIT) != 0)
            {
                return false;
            }
            const auto curr_epoch = static_cast<std::uint32_t>((curr >> EPOCH_SHIFT) & MAX_EPOCH);
            if (curr_epoch != observed_epoch)
            {
                return false;
            }
            const std::uint64_t my_bit = 1ULL << me.value;
            const std::uint64_t next = curr | my_bit;
            if (_state.compare_exchange_weak(curr, next, std::memory_order_acq_rel, std::memory_order_acquire))
            {
                return true;
            }
        }
    }

    WorkerQuiescenceBarrier::Snapshot WorkerQuiescenceBarrier::snapshot() const noexcept
    {
        const std::uint64_t curr = _state.load(std::memory_order_acquire);
        const bool armed_val = (curr & ARMED_BIT) != 0;
        const bool aborted_val = (curr & ABORTED_BIT) != 0;
        const auto mask = static_cast<std::uint32_t>(curr & MASK_BITS);
        const auto epoch = static_cast<std::uint32_t>((curr >> EPOCH_SHIFT) & MAX_EPOCH);
        const bool all_quiescent_val = armed_val && !aborted_val && (mask == _full_mask);

        return Snapshot{
            .armed = armed_val,
            .all_quiescent = all_quiescent_val,
            .aborted = aborted_val,
            .epoch = epoch,
        };
    }

    void WorkerQuiescenceBarrier::abort() noexcept
    {
        _state.fetch_or(ABORTED_BIT, std::memory_order_acq_rel);
    }
}
