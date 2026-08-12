#pragma once

#include "snf/server/player_id.hpp"

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

    struct AuthenticatedResponse
    {
        std::uint32_t request_id{0};
        PlayerId player;
    };

    using PlayerResponse = std::variant<PongResponse, AuthenticatedResponse>;

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
