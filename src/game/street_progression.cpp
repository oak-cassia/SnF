#include "snf/game/street_progression.hpp"

#include <algorithm>

namespace snf::server
{
    namespace
    {
        // The multiply comes before the divide, which is what keeps the growth
        // exact when a base is not a multiple of the denominator.
        [[nodiscard]] std::uint64_t grown(const std::uint64_t base, const std::uint64_t levels_gained) noexcept
        {
            return base + ((levels_gained * base * STAT_GROWTH_NUMERATOR) / STAT_GROWTH_DENOMINATOR);
        }
    }

    std::uint64_t streetLevel(const std::uint64_t experience) noexcept
    {
        // The division runs first, so the increment cannot overflow even at the
        // largest representable experience.
        return std::min(experience / EXPERIENCE_PER_STREET_LEVEL + 1, MAX_STREET_LEVEL);
    }

    CombatStats combatStats(const std::uint64_t level) noexcept
    {
        // Clamped rather than rejected: this is a derived view, so a level from a
        // corrupt record should read as the nearest valid one instead of wrapping
        // the subtraction below into an enormous stat.
        const std::uint64_t levels_gained = std::clamp(level, std::uint64_t{1}, MAX_STREET_LEVEL) - 1;

        return CombatStats{
            .attack = grown(BASE_ATTACK, levels_gained),
            .health = grown(BASE_HEALTH, levels_gained),
        };
    }
}
