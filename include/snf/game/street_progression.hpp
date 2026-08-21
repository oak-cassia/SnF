#pragma once

#include <cstdint>

namespace snf::server
{
    // Street progression stores one number -- total accumulated experience -- and
    // derives everything else from it. A stored level could disagree with the
    // stored experience after a partial save, and retuning the constants below
    // would become a data migration instead of a formula change.
    //
    // Keep the arithmetic integral. These values reach PlayerRecord and the
    // repository, so a float would make snapshots and tests depend on rounding.
    //
    // Growth is linear, not compounding: every level adds 10% of the base value,
    // so level N is base * (9 + N) / 10. Compounding would need a cap to stay
    // sane, and this form stays exact in integers as long as the multiply comes
    // before the divide.
    //
    // Clamp the level, never the experience: raw experience keeps accumulating in
    // the record, so raising MAX_STREET_LEVEL later grants the levels already
    // earned instead of needing a backfill. Both functions below clamp, which is
    // what bounds the multiply for any input they are given.

    inline constexpr std::uint64_t EXPERIENCE_PER_STREET_LEVEL = 1000;
    inline constexpr std::uint64_t MAX_STREET_LEVEL = 30;
    inline constexpr std::uint64_t BASE_ATTACK = 10;
    inline constexpr std::uint64_t BASE_HEALTH = 100;
    // 10% per level, kept as a ratio so the growth never needs a float.
    inline constexpr std::uint64_t STAT_GROWTH_NUMERATOR = 1;
    inline constexpr std::uint64_t STAT_GROWTH_DENOMINATOR = 10;

    struct CombatStats
    {
        std::uint64_t attack{0};
        std::uint64_t health{0};

        [[nodiscard]] bool operator==(const CombatStats&) const noexcept = default;
    };

    // Levels start at 1, so zero experience is level 1 rather than level 0, and
    // stop at MAX_STREET_LEVEL however much experience is stored.
    [[nodiscard]] std::uint64_t streetLevel(std::uint64_t experience) noexcept;

    // Pure in the level: nothing here reads actor state. Combat has no consumer
    // yet, which is why these values are derived on demand and never persisted.
    [[nodiscard]] CombatStats combatStats(std::uint64_t level) noexcept;
}
