#pragma once

#include "snf/game/player_id.hpp"

#include <cstdint>

namespace snf::server
{
    struct StreetExperienceGrant
    {
        PlayerId player;
        std::uint64_t experience{0};

        [[nodiscard]] bool operator==(const StreetExperienceGrant&) const noexcept = default;
    };
}
