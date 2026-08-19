#include "snf/server/party_actor.hpp"

#include <algorithm>
#include <stdexcept>
#include <variant>

namespace snf::server
{
    PartyActor::PartyActor(const PartyId party, const PartyActorConfig config)
        : _party(party)
        , _max_members(config.max_members)
    {
        if (_party.value == 0)
        {
            throw std::invalid_argument{"PartyId must be non-zero"};
        }
        if (_max_members == 0)
        {
            throw std::invalid_argument{"Party member capacity must be positive"};
        }
    }

    PartyId PartyActor::id() const noexcept
    {
        return _party;
    }

    std::size_t PartyActor::memberCount() const noexcept
    {
        return _members.size();
    }

    std::vector<PlayerId> PartyActor::members() const
    {
        std::vector<PlayerId> result;
        result.reserve(_members.size());
        for (const auto& [player, epoch] : _members)
        {
            static_cast<void>(epoch);
            result.push_back(player);
        }
        std::sort(result.begin(), result.end(), [](const PlayerId left, const PlayerId right) { return left.value < right.value; });
        return result;
    }

    PartyResult PartyActor::handle(const PartyCommand& command)
    {
        return std::visit([this](const auto& value) { return handleCommand(value); }, command);
    }

    PartyResult PartyActor::handleCommand(const JoinPartyCommand& command)
    {
        if (command.player.value == 0 || command.membership_epoch == 0)
        {
            throw std::invalid_argument{"Party membership identity must be non-zero"};
        }

        const auto existing = _members.find(command.player);
        if (existing != _members.end())
        {
            if (command.membership_epoch < existing->second)
            {
                return result(PartyCommandStatus::StaleMembership, command.player, existing->second);
            }
            if (command.membership_epoch == existing->second)
            {
                return result(PartyCommandStatus::AlreadyMember, command.player, existing->second);
            }

            existing->second = command.membership_epoch;
            return result(PartyCommandStatus::Applied, command.player, command.membership_epoch);
        }

        if (_members.size() >= _max_members)
        {
            return result(PartyCommandStatus::PartyFull, command.player, command.membership_epoch);
        }

        _members.emplace(command.player, command.membership_epoch);
        return result(PartyCommandStatus::Applied, command.player, command.membership_epoch);
    }

    PartyResult PartyActor::handleCommand(const LeavePartyCommand& command)
    {
        if (command.player.value == 0 || command.membership_epoch == 0)
        {
            throw std::invalid_argument{"Party membership identity must be non-zero"};
        }

        const auto existing = _members.find(command.player);
        if (existing == _members.end())
        {
            return result(PartyCommandStatus::MemberMissing, command.player, command.membership_epoch);
        }
        if (command.membership_epoch != existing->second)
        {
            return result(PartyCommandStatus::StaleMembership, command.player, existing->second);
        }

        _members.erase(existing);
        return result(PartyCommandStatus::Applied, command.player, command.membership_epoch);
    }

    PartyResult PartyActor::result(const PartyCommandStatus status, const PlayerId player, const std::uint64_t membership_epoch) const
    {
        return PartyResult{
            .status = status,
            .party = _party,
            .player = player,
            .membership_epoch = membership_epoch,
            .members = members(),
        };
    }
}
