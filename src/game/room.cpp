#include "snf/game/room.hpp"

import snf.game.skill_catalog;

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <variant>

namespace
{
    template <typename... Visitors> struct Overloaded : Visitors...
    {
        using Visitors::operator()...;
    };

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
        if (_config.max_active_projectiles == 0 || _config.max_active_projectiles > std::numeric_limits<std::uint32_t>::max())
        {
            throw std::invalid_argument{"Room projectile capacity must fit ProjectileId"};
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

        constexpr std::uint32_t MAX_ARENA_EXTENT = static_cast<std::uint32_t>(std::numeric_limits<std::int32_t>::max());
        if (_config.arena_width == 0 || _config.arena_height == 0 || _config.arena_width > MAX_ARENA_EXTENT ||
            _config.arena_height > MAX_ARENA_EXTENT)
        {
            throw std::invalid_argument{"Room arena extents must be positive and fit signed distance intermediates"};
        }
        if (_config.player_move_speed == 0 || _config.minion_move_speed == 0 || _config.boss_move_speed == 0)
        {
            throw std::invalid_argument{"Room arena movement speeds must be positive"};
        }
        if (_config.participant_spawn_spacing == 0 || _config.minion_spawn_radius == 0)
        {
            throw std::invalid_argument{"Room arena spawn spacing and radius must be positive"};
        }
        if (_config.minion_attack_damage == 0 || _config.boss_attack_damage == 0 || _config.minion_attack_range == 0 ||
            _config.boss_attack_range == 0)
        {
            throw std::invalid_argument{"Room enemy attacks must have positive damage and range"};
        }
        if (_config.minion_attack_cooldown <= std::chrono::milliseconds::zero() || _config.boss_attack_cooldown <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Room enemy attack cooldowns must be positive"};
        }

        const std::uint64_t center_x = _config.arena_width / 2;
        const std::uint64_t center_y = _config.arena_height / 2;
        const std::uint64_t right_space = static_cast<std::uint64_t>(_config.arena_width - 1) - center_x;
        const std::uint64_t bottom_space = static_cast<std::uint64_t>(_config.arena_height - 1) - center_y;
        const std::uint64_t radius = _config.minion_spawn_radius;
        if (radius > center_x || radius > center_y || radius > right_space || radius > bottom_space)
        {
            throw std::invalid_argument{"Room minion spawn radius must fit around the arena center"};
        }

        const std::size_t formation_members = _config.max_participants - 1;
        if (formation_members > std::numeric_limits<std::uint64_t>::max() / _config.participant_spawn_spacing)
        {
            throw std::invalid_argument{"Room participant formation exceeds uint64"};
        }
        const std::uint64_t formation_span = static_cast<std::uint64_t>(_config.participant_spawn_spacing) * formation_members;
        if (formation_span / 2 > center_x || center_x - formation_span / 2 + formation_span >= _config.arena_width)
        {
            throw std::invalid_argument{"Room participant formation must fit around the arena center"};
        }

        const std::size_t max_size = std::numeric_limits<std::size_t>::max();
        if (_config.max_participants > max_size - _config.digest_flush_threshold)
        {
            throw std::invalid_argument{"Room event reserve exceeds size_t"};
        }
        const std::size_t reserve_base = _config.digest_flush_threshold + _config.max_participants;
        if (_config.max_spawned_enemies > (max_size - reserve_base) / 3)
        {
            throw std::invalid_argument{"Room event reserve exceeds size_t"};
        }
        const std::size_t event_reserve = reserve_base + 3 * _config.max_spawned_enemies;

        _participants.reserve(_config.max_participants);
        _enemies.reserve(total_enemies);
        _pending_events.reserve(event_reserve);
        _projectiles.reserve(_config.max_active_projectiles);
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

    std::size_t Room::projectileCount() const noexcept
    {
        return _projectiles.size();
    }

    bool Room::canStartBattle() const noexcept
    {
        return _phase == RoomPhase::Waiting && !_participants.empty();
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
        const auto position = std::ranges::lower_bound(_participants, player, BY_PLAYER_ID, &Participant::player_id);
        if (position == _participants.end() || position->player_id != player)
        {
            return std::nullopt;
        }
        return position->stats;
    }

    std::optional<std::uint64_t> Room::healthOf(const PlayerId player) const
    {
        const auto position = std::ranges::lower_bound(_participants, player, BY_PLAYER_ID, &Participant::player_id);
        if (position == _participants.end() || position->player_id != player)
        {
            return std::nullopt;
        }
        return position->current_health;
    }

    std::optional<ArenaPosition> Room::positionOf(const PlayerId player) const
    {
        const auto position = std::ranges::lower_bound(_participants, player, BY_PLAYER_ID, &Participant::player_id);
        if (position == _participants.end() || position->player_id != player)
        {
            return std::nullopt;
        }
        return position->position;
    }

    std::optional<Projectile> Room::projectileById(const ProjectileId id) const
    {
        const auto position = std::ranges::find_if(
            _projectiles,
            [id](const Projectile& projectile)
            {
                return projectile.id == id;
            }
        );
        if (position == _projectiles.end())
        {
            return std::nullopt;
        }
        return *position;
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
        const auto position = std::ranges::lower_bound(_participants, player, BY_PLAYER_ID, &Participant::player_id);
        if (position == _participants.end() || position->player_id != player)
        {
            return nullptr;
        }
        return &*position;
    }

    Room::Participant* Room::nearestLivingParticipant(const ArenaPosition origin)
    {
        Participant* nearest = nullptr;
        std::uint64_t nearest_distance = std::numeric_limits<std::uint64_t>::max();
        for (Participant& participant : _participants)
        {
            if (participant.current_health == 0)
            {
                continue;
            }
            const std::uint64_t distance = squaredDistance(origin, participant.position);
            if (nearest == nullptr || distance < nearest_distance ||
                (distance == nearest_distance && participant.player_id.value < nearest->player_id.value))
            {
                nearest = &participant;
                nearest_distance = distance;
            }
        }
        return nearest;
    }

    const Enemy* Room::findTargetEnemy(const ArenaPosition origin, const std::uint32_t acquisition_range) const noexcept
    {
        const Enemy* nearest = nullptr;
        std::uint64_t nearest_distance = std::numeric_limits<std::uint64_t>::max();
        for (const Enemy& enemy : _enemies)
        {
            if (enemy.health == 0 || !isWithinRange(origin, enemy.position, acquisition_range))
            {
                continue;
            }
            const std::uint64_t distance = squaredDistance(origin, enemy.position);
            if (nearest == nullptr || distance < nearest_distance || (distance == nearest_distance && enemy.id.value < nearest->id.value))
            {
                nearest = &enemy;
                nearest_distance = distance;
            }
        }
        return nearest;
    }

    bool Room::allParticipantsDead() const noexcept
    {
        return !_participants.empty() && std::ranges::none_of(
                                             _participants,
                                             [](const Participant& participant)
                                             {
                                                 return participant.current_health > 0;
                                             }
                                         );
    }

    std::uint32_t Room::enemyMoveSpeed(const EnemyKind kind) const noexcept
    {
        return kind == EnemyKind::Minion ? _config.minion_move_speed : _config.boss_move_speed;
    }

    std::uint32_t Room::enemyAttackRange(const EnemyKind kind) const noexcept
    {
        return kind == EnemyKind::Minion ? _config.minion_attack_range : _config.boss_attack_range;
    }

    std::uint64_t Room::enemyAttackDamage(const EnemyKind kind) const noexcept
    {
        return kind == EnemyKind::Minion ? _config.minion_attack_damage : _config.boss_attack_damage;
    }

    std::chrono::milliseconds Room::enemyAttackCooldown(const EnemyKind kind) const noexcept
    {
        return kind == EnemyKind::Minion ? _config.minion_attack_cooldown : _config.boss_attack_cooldown;
    }

    std::vector<PlayerId> Room::audience() const
    {
        std::vector<PlayerId> players;
        players.reserve(_participants.size());
        for (const Participant& participant : _participants)
        {
            players.push_back(participant.player_id);
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

    RoomResult Room::failBattle(const RoomCommandStatus status, const std::optional<PlayerId> player, const BattleFailureReason reason)
    {
        _phase = RoomPhase::Failed;
        _projectiles.clear();
        RoomResult result = baseResult(status, player);
        result.digest = takeDigest();
        result.outcome = BattleOutcome::Failed;
        result.failure_reason = reason;
        result.audience = audience();
        return result;
    }

    void Room::initializeArena()
    {
        _pending_events.push_back(ArenaStarted{.width = _config.arena_width, .height = _config.arena_height});
        for (std::size_t index = 0; index < _participants.size(); ++index)
        {
            Participant& participant = _participants[index];
            participant.current_health = participant.stats.health;
            participant.position = centeredParticipantPosition(
                index, _participants.size(), _config.arena_width, _config.arena_height, _config.participant_spawn_spacing
            );
            participant.move_intent = MoveDirection::Stop;
            _pending_events.push_back(
                ParticipantSpawned{
                    .player = participant.player_id,
                    .position = participant.position,
                    .health = participant.current_health,
                }
            );
        }
    }

    void Room::moveParticipants()
    {
        for (Participant& participant : _participants)
        {
            if (participant.current_health == 0)
            {
                continue;
            }
            const ArenaPosition moved =
                moveInDirection(participant.position, participant.move_intent, _config.player_move_speed, _config.arena_width, _config.arena_height);
            if (moved == participant.position)
            {
                continue;
            }
            participant.position = moved;
            _pending_events.push_back(ParticipantMoved{.player = participant.player_id, .position = moved});
        }
    }

    bool Room::actEnemies(const std::chrono::steady_clock::time_point observed_at)
    {
        for (Enemy& enemy : _enemies)
        {
            if (enemy.health == 0)
            {
                continue;
            }

            Participant* target = nearestLivingParticipant(enemy.position);
            if (target == nullptr)
            {
                return !allParticipantsDead();
            }

            const std::uint32_t range = enemyAttackRange(enemy.kind);
            if (!isWithinRange(enemy.position, target->position, range))
            {
                const ArenaPosition moved =
                    moveToward(enemy.position, target->position, enemyMoveSpeed(enemy.kind), _config.arena_width, _config.arena_height);
                if (moved != enemy.position)
                {
                    enemy.position = moved;
                    _pending_events.push_back(EnemyPositioned{.enemy = enemy.id, .position = moved});
                }
            }

            if (observed_at < enemy.attack_ready_at || !isWithinRange(enemy.position, target->position, range))
            {
                continue;
            }

            const std::uint64_t damage = std::min(enemyAttackDamage(enemy.kind), target->current_health);
            target->current_health -= damage;
            enemy.attack_ready_at = observed_at + enemyAttackCooldown(enemy.kind);
            _pending_events.push_back(
                ParticipantDamaged{
                    .target = target->player_id,
                    .attacker = enemy.id,
                    .amount = damage,
                    .health = target->current_health,
                }
            );
            if (target->current_health == 0)
            {
                target->move_intent = MoveDirection::Stop;
                _pending_events.push_back(ParticipantDied{.player = target->player_id});
                if (allParticipantsDead())
                {
                    return false;
                }
            }
        }
        return true;
    }

    void Room::spawnWave(const std::chrono::steady_clock::time_point observed_at)
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
                .position =
                    squarePerimeterPosition(index, _config.minions_per_wave, _config.arena_width, _config.arena_height, _config.minion_spawn_radius),
                .attack_ready_at = observed_at + _config.minion_attack_cooldown,
            };
            _enemies.push_back(enemy);
            _pending_events.push_back(EnemySpawned{.id = enemy.id, .kind = enemy.kind, .health = enemy.health});
            _pending_events.push_back(EnemyPositioned{.enemy = enemy.id, .position = enemy.position});
        }
        ++_spawned_wave_count;
    }

    void Room::spawnBoss(const std::chrono::steady_clock::time_point observed_at)
    {
        if (_boss_spawned)
        {
            return;
        }

        const Enemy enemy{
            .id = EnemyId{.value = _next_enemy_id++},
            .kind = EnemyKind::Boss,
            .health = _config.boss_health,
            .position = bossSpawnPosition(_config.arena_width),
            .attack_ready_at = observed_at + _config.boss_attack_cooldown,
        };
        _enemies.push_back(enemy);
        _pending_events.push_back(EnemySpawned{.id = enemy.id, .kind = enemy.kind, .health = enemy.health});
        _pending_events.push_back(EnemyPositioned{.enemy = enemy.id, .position = enemy.position});
        _boss_spawned = true;
    }

    void Room::rewardClear(RoomResult& result) const
    {
        result.grants.reserve(_participants.size());
        for (const Participant& participant : _participants)
        {
            result.grants.push_back(
                StreetExperienceGrant{
                    .player = participant.player_id,
                    .experience = _config.clear_experience,
                }
            );
        }
    }

    RoomResult Room::handleCommand(const JoinRoom& command, const std::chrono::steady_clock::time_point observed_at)
    {
        static_cast<void>(observed_at);
        if (_phase != RoomPhase::Waiting)
        {
            return baseResult(RoomCommandStatus::WrongPhase, command.player);
        }

        const auto position = std::ranges::lower_bound(_participants, command.player, BY_PLAYER_ID, &Participant::player_id);
        if (position != _participants.end() && position->player_id == command.player)
        {
            return baseResult(RoomCommandStatus::AlreadyJoined, command.player);
        }
        if (_participants.size() >= _config.max_participants)
        {
            return baseResult(RoomCommandStatus::RoomFull, command.player);
        }

        _participants.insert(
            position,
            Participant{
                .player_id = command.player,
                .stats = command.stats,
                .current_health = command.stats.health,
                .cooldowns = {},
            }
        );
        return baseResult(RoomCommandStatus::Applied, command.player);
    }

    RoomResult Room::handleCommand(const LeaveRoom& command, const std::chrono::steady_clock::time_point observed_at)
    {
        static_cast<void>(observed_at);
        if (_phase == RoomPhase::Cleared || _phase == RoomPhase::Failed)
        {
            return baseResult(RoomCommandStatus::WrongPhase, command.player);
        }

        const auto position = std::ranges::lower_bound(_participants, command.player, BY_PLAYER_ID, &Participant::player_id);
        if (position == _participants.end() || position->player_id != command.player)
        {
            return baseResult(RoomCommandStatus::NotJoined, command.player);
        }

        const bool running = _phase == RoomPhase::Running;
        _participants.erase(position);
        if (!running)
        {
            return baseResult(RoomCommandStatus::Applied, command.player);
        }
        if (_participants.empty())
        {
            _pending_events.clear();
            _projectiles.clear();
            return baseResult(RoomCommandStatus::Applied, command.player);
        }

        _pending_events.push_back(ParticipantLeft{.player = command.player});
        if (allParticipantsDead())
        {
            return failBattle(RoomCommandStatus::Applied, command.player, BattleFailureReason::ParticipantsDefeated);
        }

        RoomResult result = baseResult(RoomCommandStatus::Applied, command.player);
        result.digest = takeDigest();
        result.audience = audience();
        return result;
    }

    RoomResult Room::handleCommand(const StartBattle&, const std::chrono::steady_clock::time_point observed_at)
    {
        if (!canStartBattle())
        {
            return baseResult(RoomCommandStatus::WrongPhase, std::nullopt);
        }

        _phase = RoomPhase::Running;
        _battle_deadline_at = observed_at + _config.battle_duration;
        _next_wave_at = observed_at + _config.wave_interval;
        _boss_spawn_at = observed_at + _config.boss_spawn_after;
        initializeArena();
        spawnWave(observed_at);

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

    void Room::commitSkillUse(const SkillCastContext& context)
    {
        Participant& participant = context.participant;
        const SkillId context_skill_id = context.command.skill_id;
        const auto cooldown = std::ranges::lower_bound(participant.cooldowns, context_skill_id, BY_SKILL_ID, &SkillCooldown::skill_id);
        if (cooldown != participant.cooldowns.end() && cooldown->skill_id == context_skill_id)
        {
            cooldown->ready_at = context.observed_at + context.cooldown;
        }
        else
        {
            participant.cooldowns.insert(
                cooldown,
                SkillCooldown{
                    .skill_id = context_skill_id,
                    .ready_at = context.observed_at + context.cooldown,
                }
            );
        }
        participant.applied_skill_sequence = context.command.request_sequence;
    }

    RoomResult Room::finishSkillUse(const PlayerId player)
    {
        RoomResult result = baseResult(RoomCommandStatus::Applied, player);
        if (_pending_events.size() >= _config.digest_flush_threshold)
        {
            result.digest = takeDigest();
            result.audience = audience();
        }
        return result;
    }

    RoomResult Room::applyAreaAttack(SkillCastContext& context, const std::uint32_t range)
    {
        commitSkillUse(context);

        bool any_enemy_hit = false;
        bool boss_killed = false;
        for (Enemy& target : _enemies)
        {
            if (target.health == 0 || !isWithinRange(context.participant.position, target.position, range))
            {
                continue;
            }

            any_enemy_hit = true;
            const std::uint64_t damage = std::min(context.damage, target.health);
            target.health -= damage;
            _pending_events.push_back(
                EnemyDamaged{
                    .target = target.id,
                    .actor = context.command.player,
                    .skill = context.command.skill_id,
                    .amount = damage,
                    .health = target.health,
                }
            );
            if (target.health == 0)
            {
                _pending_events.push_back(EnemyDied{.id = target.id});
                boss_killed = boss_killed || target.kind == EnemyKind::Boss;
            }
        }

        if (!any_enemy_hit)
        {
            _pending_events.push_back(SkillWhiffed{.actor = context.command.player, .skill = context.command.skill_id});
        }
        else if (boss_killed)
        {
            _phase = RoomPhase::Cleared;
            _projectiles.clear();
            RoomResult result = baseResult(RoomCommandStatus::Applied, context.command.player);
            result.digest = takeDigest();
            result.outcome = BattleOutcome::Cleared;
            result.audience = audience();
            rewardClear(result);
            return result;
        }

        return finishSkillUse(context.command.player);
    }

    RoomResult Room::spawnHomingProjectile(SkillCastContext& context, const std::uint32_t acquisition_range, const std::chrono::milliseconds lifetime)
    {
        const Enemy* target = findTargetEnemy(context.participant.position, acquisition_range);
        if (target == nullptr)
        {
            commitSkillUse(context);
            _pending_events.push_back(SkillWhiffed{.actor = context.command.player, .skill = context.command.skill_id});
            return finishSkillUse(context.command.player);
        }

        if (_projectiles.size() >= _config.max_active_projectiles || _next_projectile_id == 0)
        {
            return baseResult(RoomCommandStatus::ProjectileCapacityExceeded, context.command.player);
        }

        commitSkillUse(context);

        const ProjectileId projectile_id{.value = _next_projectile_id};
        if (_next_projectile_id == std::numeric_limits<std::uint32_t>::max())
        {
            _next_projectile_id = 0;
        }
        else
        {
            ++_next_projectile_id;
        }

        _projectiles.push_back(
            Projectile{
                .id = projectile_id,
                .owner = context.command.player,
                .skill = context.command.skill_id,
                .target = target->id,
                .position = context.participant.position,
                .damage = context.damage,
                .expires_at = context.observed_at + lifetime,
            }
        );

        return finishSkillUse(context.command.player);
    }

    RoomResult Room::handleCommand(const UseSkill& command, const std::chrono::steady_clock::time_point observed_at)
    {
        if (_phase != RoomPhase::Running)
        {
            return baseResult(RoomCommandStatus::WrongPhase, command.player);
        }
        if (observed_at >= _battle_deadline_at)
        {
            return failBattle(RoomCommandStatus::BattleExpired, command.player, BattleFailureReason::Deadline);
        }

        Participant* participant = findParticipant(command.player);
        if (participant == nullptr)
        {
            return baseResult(RoomCommandStatus::NotJoined, command.player);
        }
        if (command.request_sequence <= participant->applied_skill_sequence)
        {
            return baseResult(RoomCommandStatus::DuplicateRequest, command.player);
        }
        if (participant->current_health == 0)
        {
            return baseResult(RoomCommandStatus::ParticipantDead, command.player);
        }

        const auto skill_definition = findSkill(command.skill_id);
        if (!skill_definition)
        {
            return baseResult(RoomCommandStatus::UnknownSkill, command.player);
        }

        const auto cooldown = std::ranges::lower_bound(participant->cooldowns, command.skill_id, BY_SKILL_ID, &SkillCooldown::skill_id);
        const bool tracked = cooldown != participant->cooldowns.end() && cooldown->skill_id == command.skill_id;
        if (tracked && observed_at < cooldown->ready_at)
        {
            return baseResult(RoomCommandStatus::SkillOnCooldown, command.player);
        }

        SkillCastContext context{
            .participant = *participant,
            .command = command,
            .observed_at = observed_at,
            .cooldown = skill_definition->cooldown,
            .damage = calculateSkillDamage(*skill_definition, participant->stats.attack),
        };

        return std::visit(
            Overloaded{
                [this, &context](const AreaAttackBehavior& behavior)
                {
                    return applyAreaAttack(context, behavior.range);
                },
                [this, &context](const HomingProjectileAttackBehavior& behavior)
                {
                    return spawnHomingProjectile(context, behavior.acquisition_range, behavior.lifetime);
                },
            },
            skill_definition->behavior
        );
    }

    RoomResult Room::handleCommand(const SetMoveIntent& command, const std::chrono::steady_clock::time_point observed_at)
    {
        if (_phase != RoomPhase::Running)
        {
            return baseResult(RoomCommandStatus::WrongPhase, command.player);
        }
        if (observed_at >= _battle_deadline_at)
        {
            return failBattle(RoomCommandStatus::BattleExpired, command.player, BattleFailureReason::Deadline);
        }

        Participant* participant = findParticipant(command.player);
        if (participant == nullptr)
        {
            return baseResult(RoomCommandStatus::NotJoined, command.player);
        }
        if (command.request_sequence <= participant->applied_movement_sequence)
        {
            return baseResult(RoomCommandStatus::DuplicateRequest, command.player);
        }
        if (participant->current_health == 0)
        {
            return baseResult(RoomCommandStatus::ParticipantDead, command.player);
        }

        participant->applied_movement_sequence = command.request_sequence;
        participant->move_intent = command.direction;
        return baseResult(RoomCommandStatus::Applied, command.player);
    }

    RoomResult Room::handleCommand(const BattleDeadline&, const std::chrono::steady_clock::time_point observed_at)
    {
        if (_phase != RoomPhase::Running || observed_at < _battle_deadline_at)
        {
            return baseResult(RoomCommandStatus::WrongPhase, std::nullopt);
        }
        return failBattle(RoomCommandStatus::Applied, std::nullopt, BattleFailureReason::Deadline);
    }

    RoomResult Room::handleCommand(const RoomSimulationTick&, const std::chrono::steady_clock::time_point observed_at)
    {
        if (_phase != RoomPhase::Running)
        {
            return baseResult(RoomCommandStatus::WrongPhase, std::nullopt);
        }
        if (observed_at >= _battle_deadline_at)
        {
            return failBattle(RoomCommandStatus::Applied, std::nullopt, BattleFailureReason::Deadline);
        }

        std::erase_if(
            _enemies,
            [](const Enemy& enemy)
            {
                return enemy.health == 0;
            }
        );

        moveParticipants();
        if (!actEnemies(observed_at))
        {
            return failBattle(RoomCommandStatus::Applied, std::nullopt, BattleFailureReason::ParticipantsDefeated);
        }

        while (_spawned_wave_count < _config.wave_count && observed_at >= _next_wave_at)
        {
            spawnWave(observed_at);
            _next_wave_at += _config.wave_interval;
        }
        if (!_boss_spawned && observed_at >= _boss_spawn_at)
        {
            spawnBoss(observed_at);
        }

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
