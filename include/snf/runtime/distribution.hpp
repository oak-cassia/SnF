#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace snf::runtime
{
    struct DistributionSnapshot
    {
        std::uint64_t sample_count{0};
        std::uint64_t p50{0};
        std::uint64_t p95{0};
        std::uint64_t p99{0};
        std::uint64_t max{0};
    };

    class Distribution
    {
        static constexpr std::size_t EXACT_BUCKET_BITS = 4;
        static constexpr std::uint64_t EXACT_BUCKET_COUNT = std::uint64_t{1} << EXACT_BUCKET_BITS;
        static constexpr std::uint64_t SCALED_BUCKET_COUNT_PER_SCALE = EXACT_BUCKET_COUNT / 2;
        static constexpr std::size_t MAX_SCALE = 30;
        static constexpr std::size_t BUCKET_COUNT = static_cast<std::size_t>(EXACT_BUCKET_COUNT + MAX_SCALE * SCALED_BUCKET_COUNT_PER_SCALE);

    public:
        static constexpr std::uint64_t REPRESENTABLE_UPPER_BOUND = (2 * SCALED_BUCKET_COUNT_PER_SCALE << MAX_SCALE) - 1;

        Distribution() = default;

        Distribution(const Distribution&) = delete;
        Distribution& operator=(const Distribution&) = delete;

        void record(std::uint64_t value) noexcept
        {
            std::uint64_t observed = _max.load(std::memory_order_relaxed);
            while (observed < value && !_max.compare_exchange_weak(observed, value, std::memory_order_relaxed, std::memory_order_relaxed))
            {
            }

            _buckets[bucketOf(value)].fetch_add(1, std::memory_order_relaxed);
        }

        void record(const std::chrono::nanoseconds value) noexcept
        {
            record(value.count() <= 0 ? 0 : static_cast<std::uint64_t>(value.count()));
        }

        [[nodiscard]] DistributionSnapshot snapshot() const noexcept
        {
            std::array<std::uint64_t, BUCKET_COUNT> counts{};
            std::uint64_t sample_count = 0;
            for (std::size_t bucket = 0; bucket < BUCKET_COUNT; ++bucket)
            {
                counts[bucket] = _buckets[bucket].load(std::memory_order_relaxed);
                sample_count += counts[bucket];
            }

            DistributionSnapshot snapshot;
            snapshot.sample_count = sample_count;
            snapshot.max = _max.load(std::memory_order_relaxed);
            if (sample_count == 0)
            {
                return snapshot;
            }

            snapshot.p50 = std::min(percentileOf(counts, sample_count, 50), snapshot.max);
            snapshot.p95 = std::min(percentileOf(counts, sample_count, 95), snapshot.max);
            snapshot.p99 = std::min(percentileOf(counts, sample_count, 99), snapshot.max);
            return snapshot;
        }

    private:
        [[nodiscard]] static std::size_t bucketOf(const std::uint64_t value) noexcept
        {
            if (value < EXACT_BUCKET_COUNT)
            {
                return static_cast<std::size_t>(value);
            }

            const auto scale = static_cast<std::size_t>(std::bit_width(value)) - EXACT_BUCKET_BITS;
            if (scale > MAX_SCALE)
            {
                return BUCKET_COUNT - 1;
            }

            const auto offset = static_cast<std::size_t>((value >> scale) - SCALED_BUCKET_COUNT_PER_SCALE);
            return static_cast<std::size_t>(EXACT_BUCKET_COUNT) + (scale - 1) * static_cast<std::size_t>(SCALED_BUCKET_COUNT_PER_SCALE) + offset;
        }

        [[nodiscard]] static std::uint64_t lastValueOf(const std::size_t bucket) noexcept
        {
            if (bucket < EXACT_BUCKET_COUNT)
            {
                return bucket;
            }

            const std::size_t scaled = bucket - static_cast<std::size_t>(EXACT_BUCKET_COUNT);
            const std::size_t scale = scaled / static_cast<std::size_t>(SCALED_BUCKET_COUNT_PER_SCALE) + 1;
            const std::size_t offset = scaled % static_cast<std::size_t>(SCALED_BUCKET_COUNT_PER_SCALE);
            const std::uint64_t first_value = (SCALED_BUCKET_COUNT_PER_SCALE + offset) << scale;
            return first_value + (std::uint64_t{1} << scale) - 1;
        }

        [[nodiscard]] static std::uint64_t percentileOf(const std::array<std::uint64_t, BUCKET_COUNT>& counts, const std::uint64_t sample_count, const std::uint64_t percentile) noexcept
        {
            const std::uint64_t rank = (sample_count * percentile + 99) / 100;
            std::uint64_t cumulative = 0;
            for (std::size_t bucket = 0; bucket < BUCKET_COUNT; ++bucket)
            {
                cumulative += counts[bucket];
                if (cumulative >= rank)
                {
                    return lastValueOf(bucket);
                }
            }

            return lastValueOf(BUCKET_COUNT - 1);
        }

        std::array<std::atomic<std::uint64_t>, BUCKET_COUNT> _buckets{};
        std::atomic<std::uint64_t> _max{0};
    };
}
