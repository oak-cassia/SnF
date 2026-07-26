#include "snf/server/message_dispatcher.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    MessageDispatcher::MessageDispatcher()
    {
        const bool registered =
            registerHandler(snf::protocol::MessageType::Ping,
                            [](const snf::protocol::Frame& request)
                            {
                                return std::vector<snf::protocol::Frame>{
                                    snf::protocol::Frame{
                                        .type = snf::protocol::MessageType::Pong,
                                        .request_id = request.request_id,
                                        .payload = request.payload,
                                    },
                                };
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

    DispatchResult MessageDispatcher::dispatch(const snf::protocol::Frame& request) const
    {
        const auto handler_iterator = _handlers.find(request.type);
        if (handler_iterator == _handlers.end())
        {
            return {
                .status = DispatchStatus::HandlerNotFound,
                .responses = {},
            };
        }

        return {
            .status = DispatchStatus::Handled,
            .responses = handler_iterator->second(request),
        };
    }
}
