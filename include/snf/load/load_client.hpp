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
