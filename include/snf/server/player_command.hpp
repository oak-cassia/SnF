#pragma once

#include "snf/server/player_id.hpp"
#include "snf/server/purchase.hpp"

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

    struct AuthenticateCommand
    {
        std::uint32_t request_id{0};
        PlayerId player;
    };

    struct PurchaseCommand
    {
        std::uint32_t request_id{0};
        PurchaseIdempotencyKey idempotency_key;
        ProductId product;
    };

    using PlayerCommand = std::variant<PingCommand, AuthenticateCommand, PurchaseCommand>;

    [[nodiscard]] inline std::uint32_t requestId(const PlayerCommand& command) noexcept
    {
        return std::visit([](const auto& value) { return value.request_id; }, command);
    }
}
