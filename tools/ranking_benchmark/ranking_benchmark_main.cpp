#include "snf/runtime/distribution.hpp"
#include "snf/server/mysql_player_repository.hpp"
#include "snf/server/repository_ranking_projector.hpp"

#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>

namespace
{
    using namespace std::chrono_literals;

    struct BenchmarkConfig
    {
        std::size_t players{1000};
        std::size_t awards{10000};
        std::size_t workers{2};
        std::size_t queue_capacity{4096};
        std::size_t max_in_flight{4096};
        std::size_t projector_batch_size{1024};
        std::size_t projector_max_batches{8};
        std::uint64_t checkpoint_every{5000};
        std::chrono::milliseconds poll_interval{20};
        std::uint64_t player_base{1'000'000};
        std::uint64_t award_seed{0};
    };

    struct CompletionState
    {
        std::mutex mutex;
        std::condition_variable changed;
        std::size_t in_flight{0};
        std::size_t completed{0};
        std::size_t committed{0};
        std::size_t replayed{0};
        std::size_t rejected{0};
        snf::runtime::Distribution latency;
    };

    [[nodiscard]] std::optional<std::string> environment(const char* const name)
    {
        const char* const value = std::getenv(name);
        return value == nullptr ? std::nullopt : std::optional<std::string>{value};
    }

    [[nodiscard]] std::optional<std::uint64_t> parse_positive(const std::string_view text)
    {
        std::uint64_t value = 0;
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
        if (error != std::errc{} || end != text.data() + text.size() || value == 0)
        {
            return std::nullopt;
        }
        return value;
    }

    [[nodiscard]] std::size_t checked_size(const std::uint64_t value, const std::string_view field)
    {
        if (value > std::numeric_limits<std::size_t>::max())
        {
            throw std::invalid_argument{std::string{field} + " exceeds size_t"};
        }
        return static_cast<std::size_t>(value);
    }

    [[nodiscard]] std::uint16_t mysql_port()
    {
        const auto value = environment("SNF_MYSQL_PORT");
        if (!value)
        {
            return 3306;
        }
        const auto parsed = parse_positive(*value);
        if (!parsed || *parsed > std::numeric_limits<std::uint16_t>::max())
        {
            throw std::invalid_argument{"SNF_MYSQL_PORT is invalid"};
        }
        return static_cast<std::uint16_t>(*parsed);
    }

    [[nodiscard]] std::string required_environment(const char* const name)
    {
        const auto value = environment(name);
        if (!value || value->empty())
        {
            throw std::invalid_argument{std::string{name} + " is required"};
        }
        return *value;
    }

    void print_usage(const std::string_view program)
    {
        std::cout << "Usage: " << program
                  << " [--players 1000] [--awards 10000] [--workers 2] [--queue-capacity 4096]"
                     " [--max-in-flight 4096] [--projector-batch-size 1024]"
                     " [--projector-max-batches 8] [--checkpoint-every 5000] [--poll-ms 20]"
                     " [--player-base 1000000]"
                     " [--award-seed N]\n"
                     "MySQL environment: SNF_MYSQL_HOST (required), SNF_MYSQL_USER (required),"
                     " SNF_MYSQL_PASSWORD, SNF_MYSQL_DATABASE, SNF_MYSQL_PORT\n";
    }

    [[nodiscard]] BenchmarkConfig parse_arguments(const int count, char* arguments[])
    {
        BenchmarkConfig config;
        config.award_seed =
            static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                           std::chrono::system_clock::now().time_since_epoch())
                                           .count());

        for (int index = 1; index < count; ++index)
        {
            const std::string_view option{arguments[index]};
            if (option == "--help")
            {
                print_usage(arguments[0]);
                std::exit(0);
            }
            if (index + 1 == count)
            {
                throw std::invalid_argument{"Missing value for " + std::string{option}};
            }
            const auto parsed = parse_positive(arguments[++index]);
            if (!parsed)
            {
                throw std::invalid_argument{"Invalid value for " + std::string{option}};
            }

            if (option == "--players")
            {
                config.players = checked_size(*parsed, option);
            }
            else if (option == "--awards")
            {
                config.awards = checked_size(*parsed, option);
            }
            else if (option == "--workers")
            {
                config.workers = checked_size(*parsed, option);
            }
            else if (option == "--queue-capacity")
            {
                config.queue_capacity = checked_size(*parsed, option);
            }
            else if (option == "--max-in-flight")
            {
                config.max_in_flight = checked_size(*parsed, option);
            }
            else if (option == "--projector-batch-size")
            {
                config.projector_batch_size = checked_size(*parsed, option);
            }
            else if (option == "--projector-max-batches")
            {
                config.projector_max_batches = checked_size(*parsed, option);
            }
            else if (option == "--checkpoint-every")
            {
                config.checkpoint_every = *parsed;
            }
            else if (option == "--poll-ms")
            {
                if (*parsed > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
                {
                    throw std::invalid_argument{"Invalid value for " + std::string{option}};
                }
                config.poll_interval =
                    std::chrono::milliseconds{static_cast<std::int64_t>(*parsed)};
            }
            else if (option == "--player-base")
            {
                config.player_base = *parsed;
            }
            else if (option == "--award-seed")
            {
                config.award_seed = *parsed;
            }
            else
            {
                throw std::invalid_argument{"Unknown option: " + std::string{option}};
            }
        }

        if (config.player_base > std::numeric_limits<std::uint64_t>::max() - config.players ||
            config.award_seed > std::numeric_limits<std::uint64_t>::max() - config.awards)
        {
            throw std::invalid_argument{"Benchmark identity range overflows"};
        }
        return config;
    }

    [[nodiscard]] snf::server::MySqlPlayerRepositoryConfig
    repository_config(const BenchmarkConfig& benchmark)
    {
        const std::size_t awards_per_player = (benchmark.awards - 1) / benchmark.players + 1;
        return snf::server::MySqlPlayerRepositoryConfig{
            .host = required_environment("SNF_MYSQL_HOST"),
            .port = mysql_port(),
            .user = required_environment("SNF_MYSQL_USER"),
            .password = environment("SNF_MYSQL_PASSWORD").value_or(""),
            .database = environment("SNF_MYSQL_DATABASE").value_or("snf"),
            .worker_count = benchmark.workers,
            .queue_capacity = benchmark.queue_capacity,
            .max_idempotency_records_per_player = awards_per_player + 16,
            .connect_timeout = 5s,
            .read_timeout = 5s,
            .write_timeout = 5s,
            .purchase_fault_injector = {},
            .ranking_award_fault_injector = {},
            .ranking_checkpoint_fault_injector = {},
        };
    }

    void print_distribution(const snf::runtime::DistributionSnapshot& value)
    {
        std::cout << value.p50 << '/' << value.p95 << '/' << value.p99 << '/' << value.max << " ("
                  << value.sample_count << " samples)";
    }
}

int main(const int argument_count, char* arguments[])
try
{
    const BenchmarkConfig config = parse_arguments(argument_count, arguments);
    snf::server::MySqlPlayerRepository repository{repository_config(config)};
    snf::server::RepositoryRankingProjector projector{
        repository,
        snf::server::RepositoryRankingProjectorConfig{
            .batch_size = config.projector_batch_size,
            .max_batches_per_poll = config.projector_max_batches,
            .checkpoint_every_events = config.checkpoint_every,
            .poll_interval = config.poll_interval,
        }};

    CompletionState state;
    const auto benchmark_started_at = std::chrono::steady_clock::now();
    for (std::size_t index = 0; index < config.awards; ++index)
    {
        {
            std::unique_lock lock{state.mutex};
            state.changed.wait(
                lock, [&state, &config] { return state.in_flight < config.max_in_flight; });
            ++state.in_flight;
        }

        const auto submitted_at = std::chrono::steady_clock::now();
        repository.asyncAwardRankingScore(
            snf::server::RankingAwardRequest{
                .player =
                    snf::server::PlayerId{.value = config.player_base + 1 + index % config.players},
                .award_id = snf::server::RankingAwardId{.value = config.award_seed + index},
                .score_delta = 1,
            },
            [&state, submitted_at](const snf::server::RankingAwardTransactionResult result)
            {
                state.latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - submitted_at));
                {
                    std::lock_guard lock{state.mutex};
                    --state.in_flight;
                    ++state.completed;
                    if (result.replayed)
                    {
                        ++state.replayed;
                    }
                    else if (result.status == snf::server::RankingAwardStatus::Committed)
                    {
                        ++state.committed;
                    }
                    else
                    {
                        ++state.rejected;
                    }
                }
                state.changed.notify_all();
            });
    }

    {
        std::unique_lock lock{state.mutex};
        state.changed.wait(lock, [&state, &config] { return state.completed == config.awards; });
    }
    projector.stop();
    const auto elapsed = std::chrono::steady_clock::now() - benchmark_started_at;

    const snf::server::PlayerRepositoryStats repository_stats = repository.stats();
    const snf::server::RankingPipelineStats projector_stats = projector.stats();
    const double seconds = std::chrono::duration<double>{elapsed}.count();
    const double throughput = seconds == 0.0 ? 0.0 : state.committed / seconds;

    std::cout << "Configuration: " << config.players << " players, " << config.awards << " awards, "
              << config.workers << " workers, queue " << config.queue_capacity << ", max in-flight "
              << config.max_in_flight << ", checkpoint every " << config.checkpoint_every << '\n';
    std::cout << std::fixed << std::setprecision(2) << "Result: " << state.committed
              << " committed, " << state.replayed << " replayed, " << state.rejected
              << " rejected, " << throughput << " committed awards/s\n";
    std::cout << "End-to-end latency ns p50/p95/p99/max: ";
    print_distribution(state.latency.snapshot());
    std::cout << "\nRepository award latency ns p50/p95/p99/max: ";
    print_distribution(repository_stats.ranking_award_latency_nanoseconds);
    std::cout << "\nRepository queue high-water/rejected/failures: "
              << repository_stats.queue_high_water_mark << '/' << repository_stats.rejected << '/'
              << repository_stats.operation_failures;
    std::cout << "\nProjector poll latency ns p50/p95/p99/max: ";
    print_distribution(projector_stats.poll_latency_nanoseconds);
    std::cout << "\nProjector checkpoint latency ns p50/p95/p99/max: ";
    print_distribution(projector_stats.checkpoint_latency_nanoseconds);
    std::cout << "\nProjector offset tail/applied/checkpoint/lag: "
              << projector_stats.committed_tail_offset << '/' << projector_stats.projection_offset
              << '/' << projector_stats.checkpoint_offset << '/' << projector_stats.projection_lag
              << '\n';

    return state.replayed == 0 && state.rejected == 0 && projector_stats.projection_lag == 0 ? 0
                                                                                             : 2;
}
catch (const std::exception& error)
{
    std::cerr << error.what() << '\n';
    return 1;
}
