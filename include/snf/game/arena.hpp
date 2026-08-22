#pragma once

#include <cstddef>
#include <cstdint>

namespace snf::server
{
    struct ArenaPosition
    {
        std::uint32_t x{0};
        std::uint32_t y{0};

        [[nodiscard]] bool operator==(const ArenaPosition&) const noexcept = default;
    };

    // Crosses the wire as one byte. Zero is deliberately a valid Stop command;
    // append any future directions after NorthWest.
    enum class MoveDirection : std::uint8_t
    {
        Stop = 0,
        North = 1,
        NorthEast = 2,
        East = 3,
        SouthEast = 4,
        South = 5,
        SouthWest = 6,
        West = 7,
        NorthWest = 8,
    };

    [[nodiscard]] bool isValidMoveDirection(std::uint8_t value) noexcept;
    [[nodiscard]] ArenaPosition
    moveInDirection(ArenaPosition position, MoveDirection direction, std::uint32_t speed, std::uint32_t width, std::uint32_t height) noexcept;
    [[nodiscard]] ArenaPosition
    moveToward(ArenaPosition position, ArenaPosition target, std::uint32_t speed, std::uint32_t width, std::uint32_t height) noexcept;
    [[nodiscard]] std::uint64_t squaredDistance(ArenaPosition left, ArenaPosition right) noexcept;
    [[nodiscard]] bool isWithinRange(ArenaPosition left, ArenaPosition right, std::uint32_t range) noexcept;

    // The caller validates that the requested formation fits. Keeping these pure
    // makes spawn geometry independently testable without a Room or a clock.
    [[nodiscard]] ArenaPosition
    centeredParticipantPosition(std::size_t index, std::size_t count, std::uint32_t width, std::uint32_t height, std::uint32_t spacing) noexcept;
    [[nodiscard]] ArenaPosition
    squarePerimeterPosition(std::size_t index, std::size_t count, std::uint32_t width, std::uint32_t height, std::uint32_t radius) noexcept;
    [[nodiscard]] ArenaPosition bossSpawnPosition(std::uint32_t width) noexcept;
}
