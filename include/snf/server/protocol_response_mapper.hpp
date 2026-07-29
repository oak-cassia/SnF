#pragma once

#include "snf/protocol/frame.hpp"
#include "snf/server/player_result.hpp"

namespace snf::server
{
    // The outbound protocol boundary. PlayerActor returns domain responses and
    // this stateless mapper turns them into wire frames.
    class ProtocolResponseMapper
    {
    public:
        [[nodiscard]] snf::protocol::Frame map(const PlayerResponse& response) const;
    };
}
