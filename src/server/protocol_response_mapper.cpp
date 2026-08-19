#include "snf/server/protocol_response_mapper.hpp"

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <utility>
#include <variant>

namespace
{
    template <typename> inline constexpr bool always_false_v = false;

    constexpr std::uint64_t BYTE_MASK = 0xFFU;

    template <typename Integer> void append_big_endian(std::vector<std::byte>& payload, Integer value)
    {
        for (std::size_t remaining = sizeof(Integer); remaining > 0; --remaining)
        {
            const std::size_t shift = (remaining - 1) * 8;
            payload.push_back(static_cast<std::byte>((value >> shift) & BYTE_MASK));
        }
    }
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
                else if constexpr (std::is_same_v<Response, PurchaseResponse>)
                {
                    constexpr std::size_t PURCHASE_RESULT_PAYLOAD_SIZE = 30;
                    std::vector<std::byte> payload;
                    payload.reserve(PURCHASE_RESULT_PAYLOAD_SIZE);
                    payload.push_back(static_cast<std::byte>(value.result.status));
                    payload.push_back(static_cast<std::byte>(value.result.replayed ? 1 : 0));
                    append_big_endian(payload, value.result.idempotency_key.value);
                    append_big_endian(payload, value.result.product.value);
                    append_big_endian(payload, value.result.currency_balance);
                    append_big_endian(payload, value.result.purchased_item_count);

                    return snf::protocol::Frame{
                        .type = snf::protocol::MessageType::PurchaseResult,
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
