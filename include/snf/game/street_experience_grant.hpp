#pragma once

#include "snf/game/player_id.hpp"

#include <cstdint>

namespace snf::server
{
    // A reward a cleared Room hands to one of its participants. It names the player
    // rather than an actor key: routing a grant to a mailbox is the binding's job,
    // and the game model has no business knowing that actors exist.
    //
    // The Room's binding also puts this value in the TellPayload the Player's
    // binding takes back out, which is what keeps the runtime's carrier free of
    // either side's command types.
    struct StreetExperienceGrant
    {
        PlayerId player;
        std::uint64_t experience{0};

        [[nodiscard]] bool operator==(const StreetExperienceGrant&) const noexcept = default;
    };
}
