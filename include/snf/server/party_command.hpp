#pragma once

#include "snf/server/player_id.hpp"

#include <cstdint>
#include <variant>

namespace snf::server
{
    struct JoinPartyCommand
    {
        PlayerId player;
        std::uint64_t membership_epoch{0};
    };

    struct LeavePartyCommand
    {
        PlayerId player;
        std::uint64_t membership_epoch{0};
    };

    using PartyCommand = std::variant<JoinPartyCommand, LeavePartyCommand>;
}
