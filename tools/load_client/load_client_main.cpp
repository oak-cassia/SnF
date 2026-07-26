#include "snf/load/load_client.hpp"

#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string_view>
#include <utility>

namespace
{
    void print_usage(const std::string_view program_name)
    {
        std::cout << "Usage: " << program_name
                  << " [--host 127.0.0.1] [--port 7777] [--connections 100]\n";
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

    std::optional<std::size_t> parse_connection_count(const std::string_view text)
    {
        std::size_t value = 0;
        const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);

        constexpr std::size_t MAX_CONNECTION_COUNT = 100'000;
        if (error != std::errc{} || end != text.data() + text.size() || value == 0 ||
            value > MAX_CONNECTION_COUNT)
        {
            return std::nullopt;
        }

        return value;
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

        if (argument != "--host" && argument != "--port" && argument != "--connections")
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
            const auto connections = parse_connection_count(value);
            if (!connections)
            {
                std::cerr << "Invalid connection count: " << value << '\n';
                return 1;
            }

            config.connections = *connections;
        }
    }

    const snf::load::LoadClient client{std::move(config)};
    const auto result = client.run();

    std::chrono::steady_clock::duration total_round_trip_time{};
    for (const auto round_trip_time : result.round_trip_times)
    {
        total_round_trip_time += round_trip_time;
    }

    const double average_round_trip_milliseconds =
        result.round_trip_times.empty()
            ? 0.0
            : std::chrono::duration<double, std::milli>{total_round_trip_time}.count() /
                  result.round_trip_times.size();

    std::cout << "Connections: " << result.successful_connections << '/'
              << result.requested_connections << " succeeded, " << result.failed_connections
              << " failed\n";

    if (!result.success)
    {
        std::cerr << "Load client error: " << result.error << '\n';
        return 1;
    }

    std::cout << std::fixed << std::setprecision(3)
              << "Average RTT: " << average_round_trip_milliseconds << " ms\n";
    return 0;
}
