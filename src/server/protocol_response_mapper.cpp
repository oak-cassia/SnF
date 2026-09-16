#include "snf/server/protocol_response_mapper.hpp"

#include "snf/adapter/protocol_encoder.hpp"

namespace snf::server
{
    snf::protocol::Frame ProtocolResponseMapper::map(const PlayerResponse& response, const std::uint32_t request_id) const
    {
        return snf::adapter::encodePlayerResponse(response, request_id);
    }
}
