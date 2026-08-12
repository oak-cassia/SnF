#pragma once

#include "snf/server/player_id.hpp"

#include <cstdint>
#include <variant>

namespace snf::server
{
    struct PlayerScoreChanged
    {
        PlayerId player;
        std::uint64_t sequence{0};
        std::uint64_t score{0};

        [[nodiscard]] bool operator==(const PlayerScoreChanged&) const noexcept = default;
    };

    using PlayerDomainEvent = std::variant<PlayerScoreChanged>;

    struct PlayerEventRecord
    {
        std::uint64_t offset{0};
        PlayerDomainEvent event;

        [[nodiscard]] bool operator==(const PlayerEventRecord&) const noexcept = default;
    };

    [[nodiscard]] inline PlayerId eventPlayer(const PlayerDomainEvent& event) noexcept
    {
        return std::visit([](const auto& value) { return value.player; }, event);
    }

    [[nodiscard]] inline std::uint64_t eventSequence(const PlayerDomainEvent& event) noexcept
    {
        return std::visit([](const auto& value) { return value.sequence; }, event);
    }
}
