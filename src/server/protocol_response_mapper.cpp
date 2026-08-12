#include "snf/server/protocol_response_mapper.hpp"

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>
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
                else if constexpr (std::is_same_v<Response, AuthenticatedResponse>)
                {
                    constexpr std::size_t PLAYER_ID_WIRE_SIZE = 8;
                    std::vector<std::byte> payload(PLAYER_ID_WIRE_SIZE);
                    std::uint64_t remaining = value.player.value;
                    for (std::size_t index = PLAYER_ID_WIRE_SIZE; index > 0; --index)
                    {
                        payload[index - 1] = static_cast<std::byte>(remaining & 0xFFU);
                        remaining >>= 8U;
                    }

                    return snf::protocol::Frame{
                        .type = snf::protocol::MessageType::Authenticated,
                        .request_id = value.request_id,
                        .payload = std::move(payload),
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
