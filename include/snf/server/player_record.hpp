#pragma once

#include "snf/server/player_id.hpp"
#include "snf/server/player_location.hpp"
#include "snf/server/purchase.hpp"

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
        std::uint64_t ranking_score{0};
        std::uint64_t last_domain_event_sequence{0};
    };
}
