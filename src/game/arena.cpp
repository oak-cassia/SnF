#include "snf/game/arena.hpp"

#include <algorithm>
#include <limits>

namespace
{
    [[nodiscard]] constexpr std::uint32_t decrease(const std::uint32_t value, const std::uint32_t amount) noexcept
    {
        return value - std::min(value, amount);
    }

    [[nodiscard]] constexpr std::uint32_t increase(const std::uint32_t value, const std::uint32_t amount, const std::uint32_t maximum) noexcept
    {
        return value + std::min(amount, maximum - value);
    }

    [[nodiscard]] constexpr std::uint32_t stepToward(const std::uint32_t value, const std::uint32_t target, const std::uint32_t amount) noexcept
    {
        if (target < value)
        {
            return value - std::min(amount, value - target);
        }
        return value + std::min(amount, target - value);
    }

    [[nodiscard]] constexpr std::uint64_t
    productQuotient(const std::uint64_t value, const std::uint64_t multiplier, const std::uint64_t divisor) noexcept
    {
        return value / divisor * multiplier + value % divisor * multiplier / divisor;
    }
}

namespace snf::server
{
    bool isValidMoveDirection(const std::uint8_t value) noexcept
    {
        return value <= static_cast<std::uint8_t>(MoveDirection::NorthWest);
    }

    ArenaPosition moveInDirection(
        ArenaPosition position, const MoveDirection direction, const std::uint32_t speed, const std::uint32_t width, const std::uint32_t height
    ) noexcept
    {
        const std::uint32_t max_x = width - 1;
        const std::uint32_t max_y = height - 1;
        switch (direction)
        {
        case MoveDirection::Stop:
            break;
        case MoveDirection::North:
            position.y = decrease(position.y, speed);
            break;
        case MoveDirection::NorthEast:
            position.x = increase(position.x, speed, max_x);
            position.y = decrease(position.y, speed);
            break;
        case MoveDirection::East:
            position.x = increase(position.x, speed, max_x);
            break;
        case MoveDirection::SouthEast:
            position.x = increase(position.x, speed, max_x);
            position.y = increase(position.y, speed, max_y);
            break;
        case MoveDirection::South:
            position.y = increase(position.y, speed, max_y);
            break;
        case MoveDirection::SouthWest:
            position.x = decrease(position.x, speed);
            position.y = increase(position.y, speed, max_y);
            break;
        case MoveDirection::West:
            position.x = decrease(position.x, speed);
            break;
        case MoveDirection::NorthWest:
            position.x = decrease(position.x, speed);
            position.y = decrease(position.y, speed);
            break;
        }
        return position;
    }

    ArenaPosition moveToward(
        ArenaPosition position, const ArenaPosition target, const std::uint32_t speed, const std::uint32_t width, const std::uint32_t height
    ) noexcept
    {
        position.x = std::min(stepToward(position.x, target.x, speed), width - 1);
        position.y = std::min(stepToward(position.y, target.y, speed), height - 1);
        return position;
    }

    std::uint64_t squaredDistance(const ArenaPosition left, const ArenaPosition right) noexcept
    {
        const std::uint64_t delta_x = left.x < right.x ? static_cast<std::uint64_t>(right.x) - left.x : static_cast<std::uint64_t>(left.x) - right.x;
        const std::uint64_t delta_y = left.y < right.y ? static_cast<std::uint64_t>(right.y) - left.y : static_cast<std::uint64_t>(left.y) - right.y;
        const std::uint64_t x_squared = delta_x * delta_x;
        const std::uint64_t y_squared = delta_y * delta_y;
        if (y_squared > std::numeric_limits<std::uint64_t>::max() - x_squared)
        {
            return std::numeric_limits<std::uint64_t>::max();
        }
        return x_squared + y_squared;
    }

    bool isWithinRange(const ArenaPosition left, const ArenaPosition right, const std::uint32_t range) noexcept
    {
        const std::uint64_t widened_range = range;
        return squaredDistance(left, right) <= widened_range * widened_range;
    }

    ArenaPosition centeredParticipantPosition(
        const std::size_t index, const std::size_t count, const std::uint32_t width, const std::uint32_t height, const std::uint32_t spacing
    ) noexcept
    {
        const std::uint64_t span = static_cast<std::uint64_t>(spacing) * (count - 1);
        const std::uint64_t start_x = static_cast<std::uint64_t>(width / 2) - span / 2;
        return ArenaPosition{
            .x = static_cast<std::uint32_t>(start_x + static_cast<std::uint64_t>(spacing) * index),
            .y = height / 2,
        };
    }

    ArenaPosition squarePerimeterPosition(
        const std::size_t index, const std::size_t count, const std::uint32_t width, const std::uint32_t height, const std::uint32_t radius
    ) noexcept
    {
        const std::uint64_t center_x = width / 2;
        const std::uint64_t center_y = height / 2;
        const std::uint64_t perimeter = static_cast<std::uint64_t>(radius) * 8;
        const std::uint64_t offset = productQuotient(perimeter, index, count);
        const std::uint64_t top = center_y - radius;
        const std::uint64_t right = center_x + radius;
        const std::uint64_t bottom = center_y + radius;
        const std::uint64_t left = center_x - radius;

        if (offset < radius)
        {
            return ArenaPosition{.x = static_cast<std::uint32_t>(center_x + offset), .y = static_cast<std::uint32_t>(top)};
        }
        if (offset < static_cast<std::uint64_t>(radius) * 3)
        {
            return ArenaPosition{.x = static_cast<std::uint32_t>(right), .y = static_cast<std::uint32_t>(top + offset - radius)};
        }
        if (offset < static_cast<std::uint64_t>(radius) * 5)
        {
            return ArenaPosition{
                .x = static_cast<std::uint32_t>(right - (offset - static_cast<std::uint64_t>(radius) * 3)), .y = static_cast<std::uint32_t>(bottom)
            };
        }
        if (offset < static_cast<std::uint64_t>(radius) * 7)
        {
            return ArenaPosition{
                .x = static_cast<std::uint32_t>(left), .y = static_cast<std::uint32_t>(bottom - (offset - static_cast<std::uint64_t>(radius) * 5))
            };
        }
        return ArenaPosition{
            .x = static_cast<std::uint32_t>(left + offset - static_cast<std::uint64_t>(radius) * 7), .y = static_cast<std::uint32_t>(top)
        };
    }

    ArenaPosition bossSpawnPosition(const std::uint32_t width) noexcept
    {
        return ArenaPosition{.x = width / 2, .y = 0};
    }
}
