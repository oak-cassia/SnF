#include "snf/game/room.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <variant>

namespace
{
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
            throw std::invalid_argument{"Room boss health must be positive"};
        }
        if (_config.tick_interval <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Room tick interval must be positive"};
        }
        if (_config.wave_interval <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Room wave interval must be positive"};
        }
        if (_config.minion_health == 0)
        {
            throw std::invalid_argument{"Room minion health must be positive"};
        }
        if (_config.boss_spawn_after <= std::chrono::milliseconds::zero() || _config.boss_spawn_after >= _config.battle_duration)
        {
            throw std::invalid_argument{"Room boss spawn time must be positive and before the battle deadline"};
        }
        if (_config.max_spawned_enemies == 0 || _config.max_spawned_enemies > std::numeric_limits<std::uint32_t>::max())
        {
            throw std::invalid_argument{"Room spawned enemy capacity must fit EnemyId"};
        }
        if (_config.digest_flush_threshold == 0)
        {
            throw std::invalid_argument{"Room digest flush threshold must be positive"};
        }
        if ((_config.wave_count == 0) != (_config.minions_per_wave == 0))
        {
            throw std::invalid_argument{"Room wave and minion counts must both be zero or both be positive"};
        }
        if (_config.wave_count != 0 && _config.minions_per_wave > (std::numeric_limits<std::size_t>::max() - 1) / _config.wave_count)
        {
            throw std::invalid_argument{"Room total enemy count exceeds size_t"};
        }
        const std::size_t total_enemies = _config.wave_count * _config.minions_per_wave + 1;
        if (total_enemies > _config.max_spawned_enemies)
        {
            throw std::invalid_argument{"Room total enemy count exceeds its spawn capacity"};
        }
        const auto waves_before_boss = static_cast<std::uint64_t>(_config.boss_spawn_after / _config.wave_interval);
        if (_config.wave_count > 1 && _config.wave_count - 1 > waves_before_boss)
        {
            throw std::invalid_argument{"Room boss must spawn after every configured wave"};
        }

        _enemies.reserve(total_enemies);
        _pending_events.reserve(std::min(_config.digest_flush_threshold, total_enemies));
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

    std::size_t Room::enemyCount() const noexcept
    {
        return _enemies.size();
    }

    bool Room::bossSpawned() const noexcept
    {
        return _boss_spawned;
    }

    const Enemy* Room::boss() const noexcept
    {
        const auto position = std::ranges::find(_enemies, EnemyKind::Boss, &Enemy::kind);
        return position == _enemies.end() ? nullptr : &*position;
    }

    std::uint64_t Room::bossHealth() const noexcept
    {
        const Enemy* current_boss = boss();
        return current_boss == nullptr ? 0 : current_boss->health;
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

    Enemy* Room::firstLivingEnemy()
    {
        const auto position = std::ranges::find_if(
            _enemies,
            [](const Enemy& enemy)
            {
                return enemy.health > 0;
            }
        );
        return position == _enemies.end() ? nullptr : &*position;
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

    RoomResult Room::baseResult(const RoomCommandStatus status, const std::optional<PlayerId> player) const
    {
        return RoomResult{
            .status = status,
            .phase = _phase,
            .player = player,
            .boss_health = bossHealth(),
            .boss_spawned = _boss_spawned,
        };
    }

    std::optional<BattleDigest> Room::takeDigest()
    {
        if (_pending_events.empty())
        {
            return std::nullopt;
        }

        BattleDigest digest{
            .sequence = ++_digest_sequence,
            .events = std::move(_pending_events),
        };
        _pending_events.clear();
        return digest;
    }

    RoomResult Room::failBattle(const RoomCommandStatus status, const std::optional<PlayerId> player)
    {
        _phase = RoomPhase::Failed;
        RoomResult result = baseResult(status, player);
        result.digest = takeDigest();
        result.outcome = BattleOutcome::Failed;
        result.audience = audience();
        return result;
    }

    void Room::spawnWave()
    {
        if (_spawned_wave_count >= _config.wave_count)
        {
            return;
        }

        for (std::size_t index = 0; index < _config.minions_per_wave; ++index)
        {
            const Enemy enemy{
                .id = EnemyId{.value = _next_enemy_id++},
                .kind = EnemyKind::Minion,
                .health = _config.minion_health,
            };
            _enemies.push_back(enemy);
            _pending_events.push_back(EnemySpawned{.id = enemy.id, .kind = enemy.kind, .health = enemy.health});
        }
        ++_spawned_wave_count;
    }

    void Room::spawnBoss()
    {
        if (_boss_spawned)
        {
            return;
        }

        const Enemy enemy{
            .id = EnemyId{.value = _next_enemy_id++},
            .kind = EnemyKind::Boss,
            .health = _config.boss_health,
        };
        _enemies.push_back(enemy);
        _pending_events.push_back(EnemySpawned{.id = enemy.id, .kind = enemy.kind, .health = enemy.health});
        _boss_spawned = true;
    }

    void Room::rewardClear(RoomResult& result) const
    {
        result.grants.reserve(_participants.size());
        for (const Participant& participant : _participants)
        {
            result.grants.push_back(StreetExperienceGrant{
                .player = participant.player,
                .experience = _config.clear_experience,
            });
        }
    }

    RoomResult Room::handleCommand(const JoinRoom& command, const std::chrono::steady_clock::time_point observed_at)
    {
        static_cast<void>(observed_at);
        if (_phase != RoomPhase::Waiting)
        {
            return baseResult(RoomCommandStatus::WrongPhase, command.player);
        }

        const auto position = std::ranges::lower_bound(_participants, command.player, BY_PLAYER_ID, &Participant::player);
        if (position != _participants.end() && position->player == command.player)
        {
            return baseResult(RoomCommandStatus::AlreadyJoined, command.player);
        }
        if (_participants.size() >= _config.max_participants)
        {
            return baseResult(RoomCommandStatus::RoomFull, command.player);
        }

        _participants.insert(position, Participant{.player = command.player, .stats = command.stats, .cooldowns = {}});
        return baseResult(RoomCommandStatus::Applied, command.player);
    }

    RoomResult Room::handleCommand(const LeaveRoom& command, const std::chrono::steady_clock::time_point observed_at)
    {
        static_cast<void>(observed_at);
        if (_phase == RoomPhase::Cleared || _phase == RoomPhase::Failed)
        {
            return baseResult(RoomCommandStatus::WrongPhase, command.player);
        }

        const auto position = std::ranges::lower_bound(_participants, command.player, BY_PLAYER_ID, &Participant::player);
        if (position == _participants.end() || position->player != command.player)
        {
            return baseResult(RoomCommandStatus::NotJoined, command.player);
        }

        _participants.erase(position);
        return baseResult(RoomCommandStatus::Applied, command.player);
    }

    RoomResult Room::handleCommand(const StartBattle&, const std::chrono::steady_clock::time_point observed_at)
    {
        if (_phase != RoomPhase::Waiting || _participants.empty())
        {
            return baseResult(RoomCommandStatus::WrongPhase, std::nullopt);
        }

        _phase = RoomPhase::Running;
        _battle_deadline_at = observed_at + _config.battle_duration;
        _next_wave_at = observed_at + _config.wave_interval;
        _boss_spawn_at = observed_at + _config.boss_spawn_after;
        spawnWave();

        RoomResult result = baseResult(RoomCommandStatus::Applied, std::nullopt);
        result.deadline_after = _config.battle_duration;
        result.tick_after = _config.tick_interval;
        result.digest = takeDigest();
        if (result.digest)
        {
            result.audience = audience();
        }
        return result;
    }

    RoomResult Room::handleCommand(const UseSkill& command, const std::chrono::steady_clock::time_point observed_at)
    {
        if (_phase != RoomPhase::Running)
        {
            return baseResult(RoomCommandStatus::WrongPhase, command.player);
        }
        if (observed_at >= _battle_deadline_at)
        {
            return failBattle(RoomCommandStatus::BattleExpired, command.player);
        }

        Participant* participant = findParticipant(command.player);
        if (participant == nullptr)
        {
            return baseResult(RoomCommandStatus::NotJoined, command.player);
        }
        if (command.request_sequence <= participant->applied_sequence)
        {
            return baseResult(RoomCommandStatus::DuplicateRequest, command.player);
        }

        const auto skill = findSkill(command.skill);
        if (!skill)
        {
            return baseResult(RoomCommandStatus::UnknownSkill, command.player);
        }

        const auto cooldown = std::ranges::lower_bound(participant->cooldowns, command.skill, BY_SKILL_ID, &SkillCooldown::skill);
        const bool tracked = cooldown != participant->cooldowns.end() && cooldown->skill == command.skill;
        if (tracked && observed_at < cooldown->ready_at)
        {
            return baseResult(RoomCommandStatus::SkillOnCooldown, command.player);
        }

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

        Enemy* target = firstLivingEnemy();
        if (target == nullptr)
        {
            _pending_events.push_back(SkillWhiffed{.actor = command.player, .skill = command.skill});
        }
        else
        {
            const std::uint64_t damage = std::min(skillDamage(*skill, participant->stats.attack), target->health);
            target->health -= damage;
            _pending_events.push_back(EnemyDamaged{
                .target = target->id,
                .actor = command.player,
                .skill = command.skill,
                .amount = damage,
                .health = target->health,
            });
            if (target->health == 0)
            {
                _pending_events.push_back(EnemyDied{.id = target->id});
            }

            if (target->kind == EnemyKind::Boss && target->health == 0)
            {
                _phase = RoomPhase::Cleared;
                RoomResult result = baseResult(RoomCommandStatus::Applied, command.player);
                result.digest = takeDigest();
                result.outcome = BattleOutcome::Cleared;
                result.audience = audience();
                rewardClear(result);
                return result;
            }
        }

        RoomResult result = baseResult(RoomCommandStatus::Applied, command.player);
        if (_pending_events.size() >= _config.digest_flush_threshold)
        {
            result.digest = takeDigest();
            result.audience = audience();
        }
        return result;
    }

    RoomResult Room::handleCommand(const BattleDeadline&, const std::chrono::steady_clock::time_point observed_at)
    {
        if (_phase != RoomPhase::Running || observed_at < _battle_deadline_at)
        {
            return baseResult(RoomCommandStatus::WrongPhase, std::nullopt);
        }
        return failBattle(RoomCommandStatus::Applied, std::nullopt);
    }

    RoomResult Room::handleCommand(const RoomSimulationTick&, const std::chrono::steady_clock::time_point observed_at)
    {
        if (_phase != RoomPhase::Running)
        {
            return baseResult(RoomCommandStatus::WrongPhase, std::nullopt);
        }
        if (observed_at >= _battle_deadline_at)
        {
            return failBattle(RoomCommandStatus::Applied, std::nullopt);
        }

        while (_spawned_wave_count < _config.wave_count && observed_at >= _next_wave_at)
        {
            spawnWave();
            _next_wave_at += _config.wave_interval;
        }
        if (!_boss_spawned && observed_at >= _boss_spawn_at)
        {
            spawnBoss();
        }

        std::erase_if(
            _enemies,
            [](const Enemy& enemy)
            {
                return enemy.health == 0;
            }
        );

        RoomResult result = baseResult(RoomCommandStatus::Applied, std::nullopt);
        result.digest = takeDigest();
        if (result.digest)
        {
            result.audience = audience();
        }
        result.tick_after = _config.tick_interval;
        return result;
    }
}
