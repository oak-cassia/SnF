#pragma once

#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace snf::server
{
    struct PongResponse
    {
        std::uint32_t request_id{0};
        std::vector<std::byte> payload;
    };

    using PlayerResponse = std::variant<PongResponse>;

    struct SendResponse
    {
        PlayerResponse response;
    };

    using PlayerEffect = std::variant<SendResponse>;

    struct PlayerResult
    {
        std::vector<PlayerEffect> effects;
    };
}
