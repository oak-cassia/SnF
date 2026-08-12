#pragma once

#include <cstdint>

namespace snf::load
{
    enum class LoadScenario
    {
        Ping,
        Zone,
    };

    struct ClientWorkload
    {
        LoadScenario scenario{LoadScenario::Ping};
        std::uint64_t player_id{0};
        std::uint64_t zone_id{0};
    };
}
