#include "snf/game/room.hpp"

#include <algorithm>
#include <stdexcept>
#include <variant>

namespace
{
    // The participant list is kept sorted by identity, so every lookup is a binary
    // search and the insert position falls out of the same call.
    constexpr auto BY_PLAYER_ID = [](const snf::server::PlayerId left, const snf::server::PlayerId right)
    {
        return left.value < right.value;
    };
}

namespace snf::server
{
    Room::Room(const RoomId room, const RoomConfig config)
        : _room(room)
        , _config(config)
    {
        if (_room.value == 0)
        {
            throw std::invalid_argument{"RoomId must be non-zero"};
        }
        if (_config.battle_duration <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Room battle duration must be positive"};
        }
        if (_config.max_participants == 0)
        {
            throw std::invalid_argument{"Room must admit at least one participant"};
        }
    }

    RoomId Room::id() const noexcept
    {
        return _room;
    }

    RoomPhase Room::phase() const noexcept
    {
        return _phase;
    }

    std::size_t Room::participantCount() const noexcept
    {
        return _participants.size();
    }

    std::optional<CombatStats> Room::statsOf(const PlayerId player) const
    {
        const auto position = std::ranges::lower_bound(_participants, player, BY_PLAYER_ID, &Participant::player);
        if (position == _participants.end() || position->player != player)
        {
            return std::nullopt;
        }
        return position->stats;
    }

    RoomResult Room::handle(const RoomCommand& command)
    {
        return std::visit(
            [this](const auto& value)
            {
                return handleCommand(value);
            },
            command
        );
    }

    RoomResult Room::handleCommand(const JoinRoom& command)
    {
        if (_phase != RoomPhase::Waiting)
        {
            return RoomResult{
                .status = RoomCommandStatus::WrongPhase,
                .phase = _phase,
                .player = command.player,
            };
        }

        const auto position = std::ranges::lower_bound(_participants, command.player, BY_PLAYER_ID, &Participant::player);
        if (position != _participants.end() && position->player == command.player)
        {
            return RoomResult{
                .status = RoomCommandStatus::AlreadyJoined,
                .phase = _phase,
                .player = command.player,
            };
        }
        if (_participants.size() >= _config.max_participants)
        {
            return RoomResult{
                .status = RoomCommandStatus::RoomFull,
                .phase = _phase,
                .player = command.player,
            };
        }

        // Sorted insert rather than append. A clear emits its rewards in this order,
        // so the order must not depend on who happened to join first.
        _participants.insert(
            position,
            Participant{
                .player = command.player,
                .stats = command.stats,
            }
        );
        return RoomResult{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
            .player = command.player,
        };
    }

    RoomResult Room::handleCommand(const LeaveRoom& command)
    {
        // Cleared is terminal. The rewards are out and the Room is on its way to
        // passivation, so there is no seat left to hand back.
        if (_phase == RoomPhase::Cleared)
        {
            return RoomResult{
                .status = RoomCommandStatus::WrongPhase,
                .phase = _phase,
                .player = command.player,
            };
        }

        const auto position = std::ranges::lower_bound(_participants, command.player, BY_PLAYER_ID, &Participant::player);
        if (position == _participants.end() || position->player != command.player)
        {
            return RoomResult{
                .status = RoomCommandStatus::NotJoined,
                .phase = _phase,
                .player = command.player,
            };
        }

        // Removal rather than a flag, so the reward path needs to know nothing about
        // who left: a clear pays the participants still held. The last participant
        // leaving a running battle therefore clears with no grants, which is the same
        // outcome an empty room is refused a start for -- but here the timer is
        // already armed and only the binding can see that the Room is now empty.
        _participants.erase(position);
        return RoomResult{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
            .player = command.player,
        };
    }

    RoomResult Room::handleCommand(const StartBattle&)
    {
        // An empty room would arm the timer and then clear with nobody to reward.
        if (_phase != RoomPhase::Waiting || _participants.empty())
        {
            return RoomResult{
                .status = RoomCommandStatus::WrongPhase,
                .phase = _phase,
            };
        }

        _phase = RoomPhase::Running;

        // One shot. Combat is a placeholder, so the battle is this delay and nothing
        // else; there is no periodic tick to rearm.
        return RoomResult{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
            .complete_after = _config.battle_duration,
        };
    }

    RoomResult Room::handleCommand(const BattleCompleted&)
    {
        // A second completion lands here. The phase moves before a single reward is
        // built, so a clear cannot pay out twice however the timer or the mailbox
        // behaves.
        if (_phase != RoomPhase::Running)
        {
            return RoomResult{
                .status = RoomCommandStatus::WrongPhase,
                .phase = _phase,
            };
        }

        _phase = RoomPhase::Cleared;

        std::vector<StreetExperienceGrant> grants;
        grants.reserve(_participants.size());
        for (const Participant& participant : _participants)
        {
            grants.push_back(StreetExperienceGrant{
                .player = participant.player,
                .experience = _config.clear_experience,
            });
        }

        return RoomResult{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
            .grants = std::move(grants),
        };
    }
}
