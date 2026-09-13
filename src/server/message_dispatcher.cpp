#include "snf/server/message_dispatcher.hpp"

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>

namespace snf::server
{
    MessageDispatcher::MessageDispatcher()
    {
        // 생성 시 Ping 타입과 변환 함수를 연결해 둔다. 여기서는 등록만 하고 요청 도착 시 호출한다.
        // 이 핸들러는 payload를 PingCommand로 이동할 뿐, Pong 응답을 직접 보내지는 않는다.
        const bool ping_registered = registerHandler(snf::protocol::MessageType::Ping,
                                                     [](snf::protocol::Frame request) -> std::optional<PlayerCommand>
                                                     {
                                                         return PlayerCommand{PingCommand{
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
        // 프레임의 메시지 종류로 변환 함수를 찾는다. 등록된 함수가 없으면 HandlerNotFound다.
        const auto handler_iterator = _handlers.find(request.type);
        if (handler_iterator == _handlers.end())
        {
            return {
                .status = DispatchStatus::HandlerNotFound,
                .command = std::nullopt,
            };
        }

        // 맵의 second에 저장된 함수를 현재 호출 흐름에서 실행한다. 별도 스레드로 보내는 코드는 아니다.
        // optional에 명령이 있으면 Handled, 비어 있으면 InvalidPayload로 반환한다.
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
