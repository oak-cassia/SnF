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

    constexpr auto BY_SKILL_ID = [](const snf::server::SkillId left, const snf::server::SkillId right)
    {
        return left.value < right.value;
    };
}

namespace snf::server
{
    Room::Room(const RoomId room, const RoomConfig config)
        : _room(room)
        , _config(config)
        , _boss_health(config.boss_health)
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
        if (_config.boss_health == 0)
        {
            // A boss that starts dead would clear on the first cast, or on none.
            throw std::invalid_argument{"Room boss health must be positive"};
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

    std::uint64_t Room::bossHealth() const noexcept
    {
        return _boss_health;
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

    RoomResult Room::handle(const RoomCommand& command, const std::chrono::steady_clock::time_point observed_at)
    {
        return std::visit(
            [this, observed_at](const auto& value)
            {
                return handleCommand(value, observed_at);
            },
            command
        );
    }

    Room::Participant* Room::findParticipant(const PlayerId player)
    {
        const auto position = std::ranges::lower_bound(_participants, player, BY_PLAYER_ID, &Participant::player);
        if (position == _participants.end() || position->player != player)
        {
            return nullptr;
        }
        return &*position;
    }

    std::vector<PlayerId> Room::audience() const
    {
        std::vector<PlayerId> players;
        players.reserve(_participants.size());
        for (const Participant& participant : _participants)
        {
            players.push_back(participant.player);
        }
        return players;
    }

    RoomResult Room::refuse(const RoomCommandStatus status, const std::optional<PlayerId> player) const
    {
        return RoomResult{
            .status = status,
            .phase = _phase,
            .player = player,
            .boss_health = _boss_health,
        };
    }

    RoomResult Room::handleCommand(const JoinRoom& command, const std::chrono::steady_clock::time_point observed_at)
    {
        static_cast<void>(observed_at);
        if (_phase != RoomPhase::Waiting)
        {
            return refuse(RoomCommandStatus::WrongPhase, command.player);
        }

        const auto position = std::ranges::lower_bound(_participants, command.player, BY_PLAYER_ID, &Participant::player);
        if (position != _participants.end() && position->player == command.player)
        {
            return refuse(RoomCommandStatus::AlreadyJoined, command.player);
        }
        if (_participants.size() >= _config.max_participants)
        {
            return refuse(RoomCommandStatus::RoomFull, command.player);
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
            .boss_health = _boss_health,
        };
    }

    RoomResult Room::handleCommand(const LeaveRoom& command, const std::chrono::steady_clock::time_point observed_at)
    {
        static_cast<void>(observed_at);
        // Both terminal phases are final. The battle is decided and the Room is on
        // its way to passivation, so there is no seat left to hand back.
        if (_phase == RoomPhase::Cleared || _phase == RoomPhase::Failed)
        {
            return refuse(RoomCommandStatus::WrongPhase, command.player);
        }

        const auto position = std::ranges::lower_bound(_participants, command.player, BY_PLAYER_ID, &Participant::player);
        if (position == _participants.end() || position->player != command.player)
        {
            return refuse(RoomCommandStatus::NotJoined, command.player);
        }

        // Removal rather than a flag, so the reward path needs to know nothing about
        // who left: a clear pays the participants still held. The last participant
        // leaving a running battle therefore fails with no grants, which is the same
        // outcome an empty room is refused a start for -- but here the timer is
        // already armed and only the binding can see that the Room is now empty.
        _participants.erase(position);
        return RoomResult{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
            .player = command.player,
            .boss_health = _boss_health,
        };
    }

    RoomResult Room::handleCommand(const StartBattle&, const std::chrono::steady_clock::time_point observed_at)
    {
        static_cast<void>(observed_at);
        // An empty room would arm the timer and then decide a battle nobody fought.
        if (_phase != RoomPhase::Waiting || _participants.empty())
        {
            return refuse(RoomCommandStatus::WrongPhase, std::nullopt);
        }

        _phase = RoomPhase::Running;

        // One shot, and the only timer this battle has. The boss dying ends the
        // battle earlier and leaves this to arrive at a phase that refuses it.
        return RoomResult{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
            .deadline_after = _config.battle_duration,
            .boss_health = _boss_health,
        };
    }

    RoomResult Room::handleCommand(const UseSkill& command, const std::chrono::steady_clock::time_point observed_at)
    {
        if (_phase != RoomPhase::Running)
        {
            return refuse(RoomCommandStatus::WrongPhase, command.player);
        }

        Participant* participant = findParticipant(command.player);
        if (participant == nullptr)
        {
            return refuse(RoomCommandStatus::NotJoined, command.player);
        }

        // Checked before the skill and the cooldown: a resend must be answered the
        // same way however long it took to arrive, and re-deciding it against a
        // cooldown that has since expired would apply the same cast twice.
        if (command.request_sequence <= participant->applied_sequence)
        {
            return refuse(RoomCommandStatus::DuplicateRequest, command.player);
        }

        const auto skill = findSkill(command.skill);
        if (!skill)
        {
            return refuse(RoomCommandStatus::UnknownSkill, command.player);
        }

        const auto cooldown = std::ranges::lower_bound(participant->cooldowns, command.skill, BY_SKILL_ID, &SkillCooldown::skill);
        const bool tracked = cooldown != participant->cooldowns.end() && cooldown->skill == command.skill;
        if (tracked && observed_at < cooldown->ready_at)
        {
            return refuse(RoomCommandStatus::SkillOnCooldown, command.player);
        }

        const std::uint64_t damage = std::min(skillDamage(*skill, participant->stats.attack), _boss_health);
        _boss_health -= damage;
        participant->applied_sequence = command.request_sequence;
        if (tracked)
        {
            cooldown->ready_at = observed_at + skill->cooldown;
        }
        else
        {
            participant->cooldowns.insert(
                cooldown,
                SkillCooldown{
                    .skill = command.skill,
                    .ready_at = observed_at + skill->cooldown,
                }
            );
        }

        RoomResult result{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
            .player = command.player,
            .boss_health = _boss_health,
            .skill =
                SkillOutcome{
                    .actor = command.player,
                    .skill = command.skill,
                    .damage = damage,
                },
            // Everyone in the battle sees every cast, the caster included: the reply
            // that answers their request is the sink's business, not the Room's.
            .audience = audience(),
        };
        if (_boss_health > 0)
        {
            return result;
        }

        // The killing blow is one result: the damage that landed and the clear it
        // caused. Splitting them would let a client observe a boss at zero health in
        // a battle still running.
        _phase = RoomPhase::Cleared;
        result.phase = _phase;
        result.grants.reserve(_participants.size());
        for (const Participant& rewarded : _participants)
        {
            result.grants.push_back(StreetExperienceGrant{
                .player = rewarded.player,
                .experience = _config.clear_experience,
            });
        }
        return result;
    }

    RoomResult Room::handleCommand(const BattleDeadline&, const std::chrono::steady_clock::time_point observed_at)
    {
        static_cast<void>(observed_at);
        // A clear that got there first leaves this timer to land on a terminal
        // phase, and a redelivered timer lands on one too. The phase moves before
        // anything is built, so neither outcome can be declared twice.
        if (_phase != RoomPhase::Running)
        {
            return refuse(RoomCommandStatus::WrongPhase, std::nullopt);
        }

        _phase = RoomPhase::Failed;

        // No grants: the reward is for killing the boss. The audience still carries
        // every participant, because a failed battle has to send them home too.
        return RoomResult{
            .status = RoomCommandStatus::Applied,
            .phase = _phase,
            .boss_health = _boss_health,
            .audience = audience(),
        };
    }
}
