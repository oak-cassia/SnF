#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace snf::worker
{
    struct LatencyHistogramSnapshot
    {
        std::uint64_t count{0};
        std::uint64_t sum{0};
        std::uint64_t max{0};
        std::uint64_t p50{0};
        std::uint64_t p95{0};
        std::uint64_t p99{0};
    };

    // Owner-thread-only histogram for worker hot paths. It deliberately mirrors
    // runtime::Distribution's bucket geometry without importing the runtime layer.
    class LatencyHistogram final
    {
        static constexpr std::size_t EXACT_BUCKET_BITS = 4;
        static constexpr std::uint64_t EXACT_BUCKET_COUNT = std::uint64_t{1} << EXACT_BUCKET_BITS;
        static constexpr std::uint64_t SCALED_BUCKET_COUNT_PER_SCALE = EXACT_BUCKET_COUNT / 2;
        static constexpr std::size_t MAX_SCALE = 30;
        static constexpr std::size_t BUCKET_COUNT = static_cast<std::size_t>(EXACT_BUCKET_COUNT + MAX_SCALE * SCALED_BUCKET_COUNT_PER_SCALE);

    public:
        static constexpr std::uint64_t REPRESENTABLE_UPPER_BOUND = (2 * SCALED_BUCKET_COUNT_PER_SCALE << MAX_SCALE) - 1;

        void record(const std::uint64_t value) noexcept
        {
            ++_count;
            _sum += value;
            _max = std::max(_max, value);
            ++_buckets[bucketOf(value)];
        }

        void record(const std::chrono::nanoseconds value) noexcept
        {
            record(value.count() <= 0 ? 0 : static_cast<std::uint64_t>(value.count()));
        }

        [[nodiscard]] std::uint64_t count() const noexcept
        {
            return _count;
        }

        [[nodiscard]] LatencyHistogramSnapshot snapshot() const noexcept
        {
            LatencyHistogramSnapshot result{
                .count = _count,
                .sum = _sum,
                .max = _max,
            };
            if (_count == 0)
            {
                return result;
            }

            result.p50 = std::min(percentileOf(50), _max);
            result.p95 = std::min(percentileOf(95), _max);
            result.p99 = std::min(percentileOf(99), _max);
            return result;
        }

    private:
        [[nodiscard]] static constexpr std::size_t bucketOf(const std::uint64_t value) noexcept
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

        [[nodiscard]] static constexpr std::uint64_t lastValueOf(const std::size_t bucket) noexcept
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

        [[nodiscard]] std::uint64_t percentileOf(const std::uint64_t percentile) const noexcept
        {
            const std::uint64_t rank = (_count / 100) * percentile + ((_count % 100) * percentile + 99) / 100;
            std::uint64_t cumulative = 0;
            for (std::size_t bucket = 0; bucket < BUCKET_COUNT; ++bucket)
            {
                cumulative += _buckets[bucket];
                if (cumulative >= rank)
                {
                    return lastValueOf(bucket);
                }
            }
            return lastValueOf(BUCKET_COUNT - 1);
        }

        std::array<std::uint64_t, BUCKET_COUNT> _buckets{};
        std::uint64_t _count{0};
        std::uint64_t _sum{0};
        std::uint64_t _max{0};
    };

    static_assert(sizeof(LatencyHistogram) <= 2200);
}
