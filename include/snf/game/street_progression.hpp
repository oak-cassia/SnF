#pragma once

#include <cstdint>

namespace snf::server
{

    inline constexpr std::uint64_t EXPERIENCE_PER_STREET_LEVEL = 1000;
    inline constexpr std::uint64_t MAX_STREET_LEVEL = 30;
    inline constexpr std::uint64_t BASE_ATTACK = 10;
    inline constexpr std::uint64_t BASE_HEALTH = 100;
    inline constexpr std::uint64_t STAT_GROWTH_NUMERATOR = 1;
    inline constexpr std::uint64_t STAT_GROWTH_DENOMINATOR = 10;

    struct CombatStats
    {
        std::uint64_t attack{0};
        std::uint64_t health{0};

        [[nodiscard]] bool operator==(const CombatStats&) const noexcept = default;
    };

    [[nodiscard]] std::uint64_t streetLevel(std::uint64_t experience) noexcept;

    [[nodiscard]] CombatStats combatStats(std::uint64_t level) noexcept;
}
