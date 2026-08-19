#pragma once

#include <cstdint>

namespace snf::server
{
    // What a cleared Room tells each participant's PlayerActor. Both bindings name
    // this type and no one else does: the Room's binding wraps it in a TellPayload,
    // and the Player's binding is the only one that can take it back out, which is
    // what keeps actor routing free of either side's command types.
    struct StreetExperienceGrant
    {
        std::uint64_t experience{0};
    };
}
