#pragma once

#include <cstdint>

namespace snf::load
{
    enum class LoadScenario
    {
        Ping,
        Zone,
        Battle,
    };

    struct ClientWorkload
    {
        LoadScenario scenario{LoadScenario::Ping};
        std::uint64_t player_id{0};
        std::uint64_t zone_id{0};
        std::uint64_t room_id{0};
        bool starts_battle{false};
    };
}
