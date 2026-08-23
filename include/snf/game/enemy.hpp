#pragma once

#include "snf/game/arena.hpp"

#include <chrono>
#include <cstdint>

namespace snf::server
{
    struct EnemyId
    {
        std::uint32_t value{0};

        [[nodiscard]] bool operator==(const EnemyId&) const noexcept = default;
    };

    enum class EnemyKind : std::uint8_t
    {
        Minion = 0,
        Boss = 1,
    };

    struct Enemy
    {
        EnemyId id{};
        EnemyKind kind{EnemyKind::Minion};
        std::uint64_t health{0};
        ArenaPosition position{};
        std::chrono::steady_clock::time_point attack_ready_at{};

        [[nodiscard]] bool operator==(const Enemy&) const noexcept = default;
    };
}
