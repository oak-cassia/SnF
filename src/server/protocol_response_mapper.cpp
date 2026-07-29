#include "snf/server/protocol_response_mapper.hpp"

#include <type_traits>
#include <variant>

namespace
{
    template <typename> inline constexpr bool always_false_v = false;
}

namespace snf::server
{
    snf::protocol::Frame ProtocolResponseMapper::map(const PlayerResponse& response) const
    {
        return std::visit(
            [](const auto& value) -> snf::protocol::Frame
            {
                using Response = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<Response, PongResponse>)
                {
                    return snf::protocol::Frame{
                        .type = snf::protocol::MessageType::Pong,
                        .request_id = value.request_id,
                        .payload = value.payload,
                    };
                }
                else
                {
                    static_assert(always_false_v<Response>, "Unhandled PlayerResponse alternative");
                }
            },
            response);
    }
}
