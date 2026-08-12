#pragma once

#include "snf/server/player_id.hpp"
#include "snf/server/player_location.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    struct PlayerRecord
    {
        PlayerId player;
        std::uint64_t handled_command_count{0};
        std::optional<PlayerLocation> last_location;
    };
}
