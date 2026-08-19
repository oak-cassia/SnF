#include "snf/server/room_actor.hpp"

#include <algorithm>
#include <stdexcept>
#include <variant>

namespace snf::server
{
    RoomActor::RoomActor(const RoomId room, const RoomActorConfig config)
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

    RoomId RoomActor::id() const noexcept
    {
        return _room;
    }

    RoomPhase RoomActor::phase() const noexcept
    {
        return _phase;
    }

    std::size_t RoomActor::participantCount() const noexcept
    {
        return _participants.size();
    }

    RoomResult RoomActor::handle(const RoomCommand& command)
    {
        return std::visit([this](const auto& value) { return handleCommand(value); }, command);
    }

    RoomResult RoomActor::handleCommand(const JoinRoom& command)
    {
        if (_phase != RoomPhase::Waiting)
        {
            return RoomResult{
                .status = RoomCommandStatus::WrongPhase,
                .phase = _phase,
                .player = command.player,
            };
        }

        const auto position = std::lower_bound(_participants.begin(), _participants.end(), command.player, [](const PlayerId left, const PlayerId right) { return left.value < right.value; });
        if (position != _participants.end() && *position == command.player)
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
        _participants.insert(position, command.player);
        return RoomResult{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
            .player = command.player,
        };
    }

    RoomResult RoomActor::handleCommand(const StartBattle&)
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

        RoomResult result{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
        };
        // One shot. Combat is a placeholder, so the battle is this delay and nothing
        // else; there is no periodic tick to rearm.
        result.complete_after = _config.battle_duration;
        return result;
    }

    RoomResult RoomActor::handleCommand(const BattleCompleted&)
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

        RoomResult result{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
        };
        result.grants.reserve(_participants.size());
        for (const PlayerId participant : _participants)
        {
            result.grants.push_back(StreetExperienceGrant{
                .player = participant,
                .experience = _config.clear_experience,
            });
        }
        return result;
    }
}
