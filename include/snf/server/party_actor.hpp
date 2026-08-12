#pragma once

#include "snf/server/party_command.hpp"
#include "snf/server/party_id.hpp"
#include "snf/server/party_result.hpp"

#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <vector>

namespace snf::server
{
    struct PartyActorConfig
    {
        std::size_t max_members{8};
    };

    class PartyActor
    {
    public:
        explicit PartyActor(PartyId party, PartyActorConfig config = {});

        [[nodiscard]] PartyId id() const noexcept;
        [[nodiscard]] std::size_t memberCount() const noexcept;
        [[nodiscard]] std::vector<PlayerId> members() const;
        [[nodiscard]] PartyResult handle(const PartyCommand& command);

    private:
        [[nodiscard]] PartyResult handleCommand(const JoinPartyCommand& command);
        [[nodiscard]] PartyResult handleCommand(const LeavePartyCommand& command);
        [[nodiscard]] PartyResult
        result(PartyCommandStatus status, PlayerId player, std::uint64_t membership_epoch) const;

        PartyId _party;
        std::size_t _max_members;
        std::unordered_map<PlayerId, std::uint64_t, PlayerIdHash> _members;
    };
}
