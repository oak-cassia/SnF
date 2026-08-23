#include "snf/runtime/distribution.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <thread>
#include <vector>

namespace
{
    using namespace std::chrono_literals;

    std::uint64_t reference_percentile(std::vector<std::uint64_t> values, const double ratio)
    {
        std::ranges::sort(values);
        const auto rank = static_cast<std::size_t>(std::ceil(ratio * static_cast<double>(values.size())));
        return values[std::min(rank == 0 ? 0 : rank - 1, values.size() - 1)];
    }

    void test_empty_distribution_reports_zero()
    {
        const snf::runtime::Distribution distribution;

        const auto snapshot = distribution.snapshot();
        assert(snapshot.sample_count == 0);
        assert(snapshot.p50 == 0);
        assert(snapshot.p95 == 0);
        assert(snapshot.p99 == 0);
        assert(snapshot.max == 0);
    }

    void test_reports_exact_percentiles_for_single_value_buckets()
    {
        snf::runtime::Distribution distribution;
        std::vector<std::uint64_t> values;
        for (std::uint64_t value = 0; value < 16; ++value)
        {
            distribution.record(value);
            values.push_back(value);
        }

        const auto snapshot = distribution.snapshot();
        assert(snapshot.sample_count == 16);
        assert(snapshot.max == 15);
        assert(snapshot.p50 == reference_percentile(values, 0.50));
        assert(snapshot.p95 == reference_percentile(values, 0.95));
        assert(snapshot.p99 == reference_percentile(values, 0.99));
    }

    void test_a_single_sample_is_reported_exactly()
    {
        for (const std::uint64_t value : {std::uint64_t{0}, std::uint64_t{1}, std::uint64_t{15}, std::uint64_t{16}, std::uint64_t{4095}, std::uint64_t{1'000'000}})
        {
            snf::runtime::Distribution distribution;
            distribution.record(value);

            const auto snapshot = distribution.snapshot();
            assert(snapshot.sample_count == 1);
            assert(snapshot.max == value);
            assert(snapshot.p50 == value);
            assert(snapshot.p99 == value);
        }
    }

    void test_bucket_estimate_never_underreports_a_sample()
    {
        constexpr std::uint64_t UNCLAMPED_UPPER_SAMPLE = 1'000'000'000;

        for (std::uint64_t value = 16; value < 100'000; value += 7)
        {
            snf::runtime::Distribution distribution;
            distribution.record(value);
            distribution.record(UNCLAMPED_UPPER_SAMPLE);

            const auto snapshot = distribution.snapshot();
            assert(snapshot.sample_count == 2);
            assert(snapshot.p50 >= value);
            assert(snapshot.p50 <= value + value / 8 + 1);
            assert(snapshot.p99 == UNCLAMPED_UPPER_SAMPLE);
        }
    }

    void test_estimates_a_skewed_distribution_within_the_bucket_bound()
    {
        snf::runtime::Distribution distribution;
        std::vector<std::uint64_t> values;
        for (std::uint64_t index = 0; index < 990; ++index)
        {
            distribution.record(1'000 + index);
            values.push_back(1'000 + index);
        }
        for (std::uint64_t index = 0; index < 10; ++index)
        {
            distribution.record(5'000'000 + index);
            values.push_back(5'000'000 + index);
        }

        const auto snapshot = distribution.snapshot();
        assert(snapshot.sample_count == 1'000);
        assert(snapshot.max == 5'000'009);

        for (const auto& [reported, ratio] : {std::pair{snapshot.p50, 0.50}, std::pair{snapshot.p95, 0.95}, std::pair{snapshot.p99, 0.99}})
        {
            const std::uint64_t expected = reference_percentile(values, ratio);
            assert(reported >= expected);
            assert(reported <= expected + expected / 8 + 1);
        }
    }

    void test_percentiles_saturate_above_the_representable_bound()
    {
        constexpr std::uint64_t OVERFLOWING_SAMPLE = snf::runtime::Distribution::REPRESENTABLE_UPPER_BOUND + 1'000;

        snf::runtime::Distribution distribution;
        distribution.record(OVERFLOWING_SAMPLE);
        distribution.record(OVERFLOWING_SAMPLE + 1);

        const auto snapshot = distribution.snapshot();
        assert(snapshot.sample_count == 2);
        assert(snapshot.max == OVERFLOWING_SAMPLE + 1);
        assert(snapshot.p50 == snf::runtime::Distribution::REPRESENTABLE_UPPER_BOUND);
        assert(snapshot.p99 == snf::runtime::Distribution::REPRESENTABLE_UPPER_BOUND);
        assert(snapshot.p99 < snapshot.max);
    }

    void test_reports_the_representable_bound_exactly()
    {
        snf::runtime::Distribution distribution;
        distribution.record(snf::runtime::Distribution::REPRESENTABLE_UPPER_BOUND);

        const auto snapshot = distribution.snapshot();
        assert(snapshot.max == snf::runtime::Distribution::REPRESENTABLE_UPPER_BOUND);
        assert(snapshot.p99 == snf::runtime::Distribution::REPRESENTABLE_UPPER_BOUND);
    }

    void test_records_durations_and_clamps_a_negative_duration()
    {
        snf::runtime::Distribution distribution;
        distribution.record(-5ns);
        distribution.record(250ns);

        const auto snapshot = distribution.snapshot();
        assert(snapshot.sample_count == 2);
        assert(snapshot.max == 250);
        assert(snapshot.p50 == 0);
    }

    void test_counts_every_sample_recorded_concurrently()
    {
        constexpr std::size_t THREAD_COUNT = 4;
        constexpr std::uint64_t SAMPLES_PER_THREAD = 10'000;

        snf::runtime::Distribution distribution;
        std::vector<std::thread> recorders;
        recorders.reserve(THREAD_COUNT);

        for (std::size_t thread_index = 0; thread_index < THREAD_COUNT; ++thread_index)
        {
            recorders.emplace_back(
                [&distribution]
                {
                    for (std::uint64_t sample = 1; sample <= SAMPLES_PER_THREAD; ++sample)
                    {
                        distribution.record(sample);
                    }
                });
        }

        for (auto& recorder : recorders)
        {
            recorder.join();
        }

        const auto snapshot = distribution.snapshot();
        assert(snapshot.sample_count == THREAD_COUNT * SAMPLES_PER_THREAD);
        assert(snapshot.max == SAMPLES_PER_THREAD);
        assert(snapshot.p50 >= SAMPLES_PER_THREAD / 2);
        assert(snapshot.p50 <= snapshot.max);
    }
}

void run_distribution_tests()
{
    test_empty_distribution_reports_zero();
    test_reports_exact_percentiles_for_single_value_buckets();
    test_a_single_sample_is_reported_exactly();
    test_bucket_estimate_never_underreports_a_sample();
    test_estimates_a_skewed_distribution_within_the_bucket_bound();
    test_percentiles_saturate_above_the_representable_bound();
    test_reports_the_representable_bound_exactly();
    test_records_durations_and_clamps_a_negative_duration();
    test_counts_every_sample_recorded_concurrently();
}
