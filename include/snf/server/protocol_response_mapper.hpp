#pragma once

#include "snf/game/player_result.hpp"
#include "snf/protocol/frame.hpp"

#include <cstdint>

namespace snf::server
{
    class ProtocolResponseMapper
    {
    public:
        [[nodiscard]] snf::protocol::Frame map(const PlayerResponse& response, std::uint32_t request_id) const;
    };
}
