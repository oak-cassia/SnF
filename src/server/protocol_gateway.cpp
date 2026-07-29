#include "snf/server/protocol_gateway.hpp"

#include <utility>

namespace snf::server
{
    ProtocolGateway::ProtocolGateway(RoutedCommandIngress& commands)
        : ProtocolGateway(MessageDispatcher{}, commands)
    {
    }

    ProtocolGateway::ProtocolGateway(MessageDispatcher dispatcher, RoutedCommandIngress& commands)
        : _dispatcher(std::move(dispatcher))
        , _commands(commands)
    {
    }

    FramePostResult ProtocolGateway::tryPost(FrameEnvelope envelope)
    {
        DispatchResult dispatch_result = _dispatcher.dispatch(std::move(envelope.frame));
        if (!dispatch_result.handled())
        {
            return dispatch_result.status == DispatchStatus::HandlerNotFound
                       ? FramePostResult::UnsupportedMessage
                       : FramePostResult::InvalidPayload;
        }

        const PostResult post_result = _commands.tryPost(RoutedCommand{
            .connection = envelope.connection,
            .route =
                PlayerCommandRoute{
                    .actor = provisionalActorIdFor(envelope.connection),
                    .command = std::move(*dispatch_result.command),
                },
        });

        switch (post_result)
        {
        case PostResult::Accepted:
            return FramePostResult::Accepted;
        case PostResult::Full:
            return FramePostResult::Full;
        case PostResult::Closed:
            return FramePostResult::Closed;
        }

        return FramePostResult::Closed;
    }

    PostResult ProtocolGateway::tryPostConnectionClosed(ConnectionClosed closed)
    {
        return _commands.tryPost(RoutedCommand{
            .connection = closed.connection,
            .route =
                ConnectionClosedRoute{
                    .actor = provisionalActorIdFor(closed.connection),
                    .cause = closed.cause,
                },
        });
    }

    void ProtocolGateway::close() noexcept
    {
        _commands.close();
    }

    void ProtocolGateway::cancel() noexcept
    {
        _commands.cancel();
    }
}
