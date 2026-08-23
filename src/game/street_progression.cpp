#include "snf/game/street_progression.hpp"

#include <algorithm>

namespace snf::server
{
    namespace
    {
        [[nodiscard]] std::uint64_t grown(const std::uint64_t base, const std::uint64_t levels_gained) noexcept
        {
            return base + ((levels_gained * base * STAT_GROWTH_NUMERATOR) / STAT_GROWTH_DENOMINATOR);
        }
    }

    std::uint64_t streetLevel(const std::uint64_t experience) noexcept
    {
        return std::min(experience / EXPERIENCE_PER_STREET_LEVEL + 1, MAX_STREET_LEVEL);
    }

    CombatStats combatStats(const std::uint64_t level) noexcept
    {
        const std::uint64_t levels_gained = std::clamp(level, std::uint64_t{1}, MAX_STREET_LEVEL) - 1;

        return CombatStats{
            .attack = grown(BASE_ATTACK, levels_gained),
            .health = grown(BASE_HEALTH, levels_gained),
        };
    }
}
