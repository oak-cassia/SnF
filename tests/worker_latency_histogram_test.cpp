#include "snf/worker/latency_histogram.hpp"

#include <algorithm>
#include <bit>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <utility>
#include <vector>

namespace
{
    using namespace std::chrono_literals;
    using snf::worker::LatencyHistogram;

    [[nodiscard]] std::uint64_t referencePercentile(std::vector<std::uint64_t> values, const double ratio)
    {
        std::ranges::sort(values);
        const auto rank = static_cast<std::size_t>(std::ceil(ratio * static_cast<double>(values.size())));
        return values[std::min(rank == 0 ? 0 : rank - 1, values.size() - 1)];
    }

    [[nodiscard]] std::uint64_t bucketUpperBound(const std::uint64_t value)
    {
        if (value < 16)
        {
            return value;
        }
        const auto scale = static_cast<std::size_t>(std::bit_width(value)) - 4;
        if (scale > 30)
        {
            return LatencyHistogram::REPRESENTABLE_UPPER_BOUND;
        }
        const std::uint64_t offset = (value >> scale) - 8;
        const std::uint64_t first = (8 + offset) << scale;
        return first + (std::uint64_t{1} << scale) - 1;
    }

    void test_empty_histogram_is_zero_and_copyable()
    {
        const LatencyHistogram histogram;
        const LatencyHistogram copy = histogram;
        const auto snapshot = copy.snapshot();
        assert(snapshot.count == 0);
        assert(snapshot.sum == 0);
        assert(snapshot.max == 0);
        assert(snapshot.p50 == 0);
        assert(snapshot.p95 == 0);
        assert(snapshot.p99 == 0);
    }

    void test_count_sum_and_max_are_exact_and_percentiles_stay_in_their_bucket()
    {
        LatencyHistogram histogram;
        std::vector<std::uint64_t> values;
        for (std::uint64_t value = 1; value <= 1000; ++value)
        {
            const std::uint64_t sample = value * value;
            histogram.record(sample);
            values.push_back(sample);
        }

        const auto snapshot = histogram.snapshot();
        assert(snapshot.count == values.size());
        assert(snapshot.sum == std::accumulate(values.begin(), values.end(), std::uint64_t{0}));
        assert(snapshot.max == values.back());

        for (const auto& [reported, ratio] : {
                 std::pair{snapshot.p50, 0.50},
                 std::pair{snapshot.p95, 0.95},
                 std::pair{snapshot.p99, 0.99},
             })
        {
            const std::uint64_t exact = referencePercentile(values, ratio);
            assert(reported >= exact);
            assert(reported <= bucketUpperBound(exact));
        }
    }

    void test_duration_recording_clamps_negative_values()
    {
        LatencyHistogram histogram;
        histogram.record(-5ns);
        histogram.record(250ns);

        const auto snapshot = histogram.snapshot();
        assert(snapshot.count == 2);
        assert(snapshot.sum == 250);
        assert(snapshot.max == 250);
        assert(snapshot.p50 == 0);
    }
}

void run_worker_latency_histogram_tests()
{
    test_empty_histogram_is_zero_and_copyable();
    test_count_sum_and_max_are_exact_and_percentiles_stay_in_their_bucket();
    test_duration_recording_clamps_negative_values();
}
