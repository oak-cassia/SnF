#pragma once

#include "snf/server/player_id.hpp"

#include <cstdint>

namespace snf::server
{
    struct PlayerRecord
    {
        PlayerId player;
        std::uint64_t handled_command_count{0};
    };
}
