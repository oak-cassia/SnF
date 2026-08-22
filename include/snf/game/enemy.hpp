#pragma once

#include <cstdint>

namespace snf::server
{
    struct EnemyId
    {
        std::uint32_t value{0};

        [[nodiscard]] bool operator==(const EnemyId&) const noexcept = default;
    };

    // Crosses the wire as one byte. Add new kinds only at the end.
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

        [[nodiscard]] bool operator==(const Enemy&) const noexcept = default;
    };
}
