#pragma once

#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace snf::server
{
    struct PingCommand
    {
        std::uint32_t request_id{0};
        std::vector<std::byte> payload;
    };

    using PlayerCommand = std::variant<PingCommand>;

    [[nodiscard]] inline std::uint32_t requestId(const PlayerCommand& command) noexcept
    {
        return std::visit([](const auto& value) { return value.request_id; }, command);
    }
}
