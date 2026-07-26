#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace snf::load
{
    struct LoadClientConfig
    {
        std::string host{"127.0.0.1"};
        std::uint16_t port{7777};
        std::chrono::milliseconds connect_timeout{5000};
        std::chrono::milliseconds request_timeout{3000};
    };

    struct LoadClientResult
    {
        bool success{false};
        std::string error;
        std::chrono::steady_clock::duration round_trip_time{};
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
