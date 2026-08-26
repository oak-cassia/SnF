#pragma once

#include "snf/game/arena.hpp"
#include "snf/game/enemy.hpp"
#include "snf/game/player_id.hpp"
#include "snf/game/skill_id.hpp"

#include <chrono>
#include <cstdint>

namespace snf::server
{
    struct ProjectileId
    {
        std::uint32_t value{0};

        [[nodiscard]] bool operator==(const ProjectileId&) const noexcept = default;
    };

    struct Projectile
    {
        ProjectileId id{};
        PlayerId owner{};
        SkillId skill{};
        EnemyId target{};
        ArenaPosition position{};
        std::uint32_t speed_per_tick{0};
        std::uint32_t hit_range{0};
        std::uint64_t damage{0};
        std::chrono::steady_clock::time_point expires_at{};

        [[nodiscard]] bool operator==(const Projectile&) const noexcept = default;
    };
}
