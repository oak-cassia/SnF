#include "snf/load/load_client.hpp"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace
{
    void print_usage(const std::string_view program_name)
    {
        std::cout << "Usage: " << program_name
                  << " [--host 127.0.0.1] [--port 7777] [--connections 100]"
                     " [--scenario ping|zone] [--players-per-zone 50]"
                     " [--duration 30] [--requests-per-second 1]"
                     " [--connect-timeout-ms 5000] [--request-timeout-ms 3000]\n";
    }

    std::optional<std::uint16_t> parse_port(const std::string_view text)
    {
        std::uint32_t value = 0;
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);

        if (error != std::errc{} || end != text.data() + text.size() || value == 0 || value > 65535)
        {
            return std::nullopt;
        }

        return static_cast<std::uint16_t>(value);
    }

    std::optional<std::size_t> parse_positive_size(const std::string_view text,
                                                   const std::size_t maximum)
    {
        std::size_t value = 0;
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);

        if (error != std::errc{} || end != text.data() + text.size() || value == 0 ||
            value > maximum)
        {
            return std::nullopt;
        }

        return value;
    }

    double percentile(const std::vector<double>& sorted_values, const double ratio)
    {
        if (sorted_values.empty())
        {
            return 0.0;
        }

        const auto index = static_cast<std::size_t>(
            std::ceil(ratio * static_cast<double>(sorted_values.size())) - 1.0);
        return sorted_values[std::min(index, sorted_values.size() - 1)];
    }
}

int main(const int argument_count, char* arguments[])
{
    snf::load::LoadClientConfig config;

    for (int argument_index = 1; argument_index < argument_count; ++argument_index)
    {
        const std::string_view argument{arguments[argument_index]};

        if (argument == "--help")
        {
            print_usage(arguments[0]);
            return 0;
        }

        if (argument != "--host" && argument != "--port" && argument != "--connections" &&
            argument != "--duration" && argument != "--requests-per-second" &&
            argument != "--scenario" && argument != "--players-per-zone" &&
            argument != "--connect-timeout-ms" && argument != "--request-timeout-ms")
        {
            std::cerr << "Unknown option: " << argument << '\n';
            return 1;
        }

        if (argument_index + 1 >= argument_count)
        {
            std::cerr << "Missing value for " << argument << '\n';
            return 1;
        }

        const std::string_view value{arguments[++argument_index]};

        if (argument == "--host")
        {
            config.host = value;
        }
        else if (argument == "--port")
        {
            const auto port = parse_port(value);
            if (!port)
            {
                std::cerr << "Invalid port: " << value << '\n';
                return 1;
            }

            config.port = *port;
        }
        else if (argument == "--connections")
        {
            const auto connections = parse_positive_size(value, 100'000);
            if (!connections)
            {
                std::cerr << "Invalid connection count: " << value << '\n';
                return 1;
            }

            config.connections = *connections;
        }
        else if (argument == "--duration")
        {
            const auto duration_seconds = parse_positive_size(value, 86'400);
            if (!duration_seconds)
            {
                std::cerr << "Invalid duration: " << value << '\n';
                return 1;
            }

            config.duration = std::chrono::seconds{static_cast<std::int64_t>(*duration_seconds)};
        }
        else if (argument == "--requests-per-second")
        {
            const auto requests_per_second = parse_positive_size(value, 1'000'000);
            if (!requests_per_second)
            {
                std::cerr << "Invalid requests per second: " << value << '\n';
                return 1;
            }

            config.requests_per_second = *requests_per_second;
        }
        else if (argument == "--scenario")
        {
            if (value == "ping")
            {
                config.scenario = snf::load::LoadScenario::Ping;
            }
            else if (value == "zone")
            {
                config.scenario = snf::load::LoadScenario::Zone;
            }
            else
            {
                std::cerr << "Invalid scenario: " << value << '\n';
                return 1;
            }
        }
        else if (argument == "--players-per-zone")
        {
            const auto players = parse_positive_size(value, 100'000);
            if (!players)
            {
                std::cerr << "Invalid players per Zone: " << value << '\n';
                return 1;
            }
            config.players_per_zone = *players;
        }
        else if (argument == "--connect-timeout-ms")
        {
            const auto timeout = parse_positive_size(value, 3'600'000);
            if (!timeout)
            {
                std::cerr << "Invalid connect timeout: " << value << '\n';
                return 1;
            }

            config.connect_timeout = std::chrono::milliseconds{static_cast<std::int64_t>(*timeout)};
        }
        else if (argument == "--request-timeout-ms")
        {
            const auto timeout = parse_positive_size(value, 3'600'000);
            if (!timeout)
            {
                std::cerr << "Invalid request timeout: " << value << '\n';
                return 1;
            }

            config.request_timeout = std::chrono::milliseconds{static_cast<std::int64_t>(*timeout)};
        }
    }

    const snf::load::LoadClient client{std::move(config)};
    const auto result = client.run();

    std::vector<double> round_trip_milliseconds;
    round_trip_milliseconds.reserve(result.round_trip_times.size());

    double total_round_trip_milliseconds = 0.0;
    for (const auto round_trip_time : result.round_trip_times)
    {
        const double value = std::chrono::duration<double, std::milli>{round_trip_time}.count();
        round_trip_milliseconds.push_back(value);
        total_round_trip_milliseconds += value;
    }

    std::ranges::sort(round_trip_milliseconds);

    std::vector<double> gameplay_round_trip_milliseconds;
    gameplay_round_trip_milliseconds.reserve(result.gameplay_round_trip_times.size());
    for (const auto round_trip_time : result.gameplay_round_trip_times)
    {
        gameplay_round_trip_milliseconds.push_back(
            std::chrono::duration<double, std::milli>{round_trip_time}.count());
    }
    std::ranges::sort(gameplay_round_trip_milliseconds);

    const double average_round_trip_milliseconds =
        round_trip_milliseconds.empty()
            ? 0.0
            : total_round_trip_milliseconds / round_trip_milliseconds.size();

    const double load_duration_seconds =
        std::chrono::duration<double>{result.load_duration}.count();
    const double throughput =
        load_duration_seconds > 0.0
            ? static_cast<double>(result.received_responses) / load_duration_seconds
            : 0.0;

    std::cout << "Connections: " << result.successful_connections << '/'
              << result.requested_connections << " succeeded, " << result.failed_connections
              << " failed, peak active " << result.maximum_active_connections << '\n';
    std::cout << "Requests: " << result.sent_requests << " sent, " << result.received_responses
              << " received, " << result.request_timeouts << " timeout, "
              << result.invalid_responses << " invalid, " << result.socket_errors
              << " socket error\n";
    std::cout << "Workload: " << result.sent_bootstrap_requests << '/'
              << result.received_bootstrap_responses << " bootstrap sent/received, "
              << result.sent_gameplay_requests << '/' << result.received_gameplay_responses
              << " gameplay sent/received\n";

    std::cout << std::fixed << std::setprecision(3) << "Throughput: " << throughput
              << " responses/s\n"
              << "RTT ms: avg " << average_round_trip_milliseconds << ", p50 "
              << percentile(round_trip_milliseconds, 0.50) << ", p95 "
              << percentile(round_trip_milliseconds, 0.95) << ", p99 "
              << percentile(round_trip_milliseconds, 0.99) << '\n';
    if (!gameplay_round_trip_milliseconds.empty())
    {
        std::cout << "Gameplay RTT ms: p50 " << percentile(gameplay_round_trip_milliseconds, 0.50)
                  << ", p95 " << percentile(gameplay_round_trip_milliseconds, 0.95) << ", p99 "
                  << percentile(gameplay_round_trip_milliseconds, 0.99) << '\n';
    }

    if (!result.success)
    {
        std::cerr << "Load client error: " << result.error << '\n';
        return 1;
    }

    return 0;
}
