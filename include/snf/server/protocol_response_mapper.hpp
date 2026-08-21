#pragma once

#include "snf/game/player_result.hpp"
#include "snf/protocol/frame.hpp"

#include <cstdint>

namespace snf::server
{
    // The outbound protocol boundary. Player returns domain responses and
    // this stateless mapper turns them into wire frames.
    class ProtocolResponseMapper
    {
    public:
        // The request id comes from the envelope of the command being answered, not
        // from the response, which is why it is a separate argument.
        [[nodiscard]] snf::protocol::Frame map(const PlayerResponse& response, std::uint32_t request_id) const;
    };
}
