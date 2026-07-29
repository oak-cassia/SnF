#include "snf/server/message_dispatcher.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    MessageDispatcher::MessageDispatcher()
    {
        const bool registered =
            registerHandler(snf::protocol::MessageType::Ping,
                            [](snf::protocol::Frame request) -> std::optional<PlayerCommand>
                            {
                                return PlayerCommand{PingCommand{
                                    .request_id = request.request_id,
                                    .payload = std::move(request.payload),
                                }};
                            });

        if (!registered)
        {
            throw std::logic_error{"The PING handler is already registered"};
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
