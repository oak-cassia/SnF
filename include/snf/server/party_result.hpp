#pragma once

#include "snf/server/party_id.hpp"
#include "snf/server/player_id.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace snf::server
{
    enum class PartyCommandStatus : std::uint8_t
    {
        Applied = 0,
        AlreadyMember = 1,
        MemberMissing = 2,
        StaleMembership = 3,
        PartyFull = 4,
    };

    struct PartyResult
    {
        PartyCommandStatus status{PartyCommandStatus::Applied};
        PartyId party;
        PlayerId player;
        std::uint64_t membership_epoch{0};
        std::vector<PlayerId> members;

        [[nodiscard]] std::size_t memberCount() const noexcept
        {
            return members.size();
        }
    };
}
