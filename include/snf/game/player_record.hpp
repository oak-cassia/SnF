#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/player_location.hpp"
#include "snf/game/purchase.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    struct PlayerRecord
    {
        PlayerId player;
        std::uint64_t handled_command_count{0};
        std::optional<PlayerLocation> last_location;
        std::uint64_t currency_balance{INITIAL_CURRENCY_BALANCE};
        std::uint64_t purchased_item_count{0};
        std::uint64_t street_experience{0};
    };
}
