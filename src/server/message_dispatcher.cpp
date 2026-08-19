#include "snf/server/message_dispatcher.hpp"

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>

namespace snf::server
{
    MessageDispatcher::MessageDispatcher()
    {
        const bool ping_registered = registerHandler(snf::protocol::MessageType::Ping,
                                                     [](snf::protocol::Frame request) -> std::optional<PlayerCommand>
                                                     {
                                                         return PlayerCommand{PingCommand{
                                                             .request_id = request.request_id,
                                                             .payload = std::move(request.payload),
                                                         }};
                                                     });

        const bool authenticate_registered = registerHandler(snf::protocol::MessageType::Authenticate,
                                                             [](snf::protocol::Frame request) -> std::optional<PlayerCommand>
                                                             {
                                                                 constexpr std::size_t PLAYER_ID_WIRE_SIZE = 8;
                                                                 if (request.payload.size() != PLAYER_ID_WIRE_SIZE)
                                                                 {
                                                                     return std::nullopt;
                                                                 }

                                                                 std::uint64_t player_value = 0;
                                                                 for (const std::byte byte : request.payload)
                                                                 {
                                                                     player_value = (player_value << 8U) | std::to_integer<std::uint64_t>(byte);
                                                                 }
                                                                 if (player_value == 0)
                                                                 {
                                                                     return std::nullopt;
                                                                 }

                                                                 return PlayerCommand{AuthenticateCommand{
                                                                     .request_id = request.request_id,
                                                                     .player = PlayerId{.value = player_value},
                                                                 }};
                                                             });

        const bool purchase_registered = registerHandler(snf::protocol::MessageType::Purchase,
                                                         [](snf::protocol::Frame request) -> std::optional<PlayerCommand>
                                                         {
                                                             constexpr std::size_t PURCHASE_PAYLOAD_SIZE = 12;
                                                             if (request.payload.size() != PURCHASE_PAYLOAD_SIZE)
                                                             {
                                                                 return std::nullopt;
                                                             }

                                                             std::uint64_t key = 0;
                                                             for (std::size_t index = 0; index < 8; ++index)
                                                             {
                                                                 key = (key << 8U) | std::to_integer<std::uint64_t>(request.payload[index]);
                                                             }

                                                             std::uint32_t product = 0;
                                                             for (std::size_t index = 8; index < PURCHASE_PAYLOAD_SIZE; ++index)
                                                             {
                                                                 product = (product << 8U) | std::to_integer<std::uint32_t>(request.payload[index]);
                                                             }
                                                             if (key == 0 || product == 0)
                                                             {
                                                                 return std::nullopt;
                                                             }

                                                             return PlayerCommand{PurchaseCommand{
                                                                 .request_id = request.request_id,
                                                                 .idempotency_key = PurchaseIdempotencyKey{.value = key},
                                                                 .product = ProductId{.value = product},
                                                             }};
                                                         });

        if (!ping_registered || !authenticate_registered || !purchase_registered)
        {
            throw std::logic_error{"A built-in message handler is already registered"};
        }
    }

    bool MessageDispatcher::registerHandler(const snf::protocol::MessageType type, Handler handler)
    {
        if (!handler)
        {
            throw std::invalid_argument{"A message handler must be callable"};
        }

        return _handlers.emplace(type, std::move(handler)).second;
    }

    DispatchResult MessageDispatcher::dispatch(snf::protocol::Frame request) const
    {
        const auto handler_iterator = _handlers.find(request.type);
        if (handler_iterator == _handlers.end())
        {
            return {
                .status = DispatchStatus::HandlerNotFound,
                .command = std::nullopt,
            };
        }

        auto command = handler_iterator->second(std::move(request));
        return command
                   ? DispatchResult{
                       .status = DispatchStatus::Handled,
                       .command = std::move(command),
                   }
                   : DispatchResult{
                       .status = DispatchStatus::InvalidPayload,
                       .command = std::nullopt,
                   };
    }
}
