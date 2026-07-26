#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace snf::load
{
    struct LoadClientConfig
    {
        std::string host{"127.0.0.1"};
        std::uint16_t port{7777};
        std::size_t connections{100};
        std::chrono::milliseconds duration{30000};
        std::size_t requests_per_second{1};
        std::chrono::milliseconds connect_timeout{5000};
        std::chrono::milliseconds request_timeout{3000};
    };

    struct LoadClientResult
    {
        bool success{false};
        std::string error;
        std::size_t requested_connections{0};
        std::size_t successful_connections{0};
        std::size_t failed_connections{0};
        std::size_t maximum_active_connections{0};
        std::size_t sent_requests{0};
        std::size_t received_responses{0};
        std::size_t request_timeouts{0};
        std::size_t invalid_responses{0};
        std::size_t socket_errors{0};
        std::chrono::steady_clock::duration load_duration{};
        std::vector<std::chrono::steady_clock::duration> round_trip_times;
    };

    class LoadClient
    {
    public:
        explicit LoadClient(LoadClientConfig config);

        [[nodiscard]] LoadClientResult run() const;

    private:
        LoadClientConfig _config;
    };
}
