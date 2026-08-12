#include "snf/server/protocol_gateway.hpp"

#include <utility>

namespace snf::server
{
    ProtocolGateway::ProtocolGateway(RoutedCommandIngress& commands)
        : ProtocolGateway(MessageDispatcher{}, commands)
    {
    }

    ProtocolGateway::ProtocolGateway(RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions)
        : ProtocolGateway(MessageDispatcher{}, commands, sessions)
    {
    }

    ProtocolGateway::ProtocolGateway(MessageDispatcher dispatcher, RoutedCommandIngress& commands)
        : _dispatcher(std::move(dispatcher))
        , _commands(commands)
        , _owned_sessions()
        , _sessions(_owned_sessions)
    {
    }

    ProtocolGateway::ProtocolGateway(MessageDispatcher dispatcher,
                                     RoutedCommandIngress& commands,
                                     PlayerSessionDirectory& sessions)
        : _dispatcher(std::move(dispatcher))
        , _commands(commands)
        , _owned_sessions()
        , _sessions(sessions)
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

        PlayerActorId actor = provisionalActorIdFor(envelope.connection);
        std::optional<PlayerId> new_attachment;
        if (const auto* authenticate = std::get_if<AuthenticateCommand>(&*dispatch_result.command))
        {
            const PlayerAttachResult attach_result =
                _sessions.tryAttach(envelope.connection, authenticate->player);
            if (attach_result == PlayerAttachResult::Attached)
            {
                new_attachment = authenticate->player;
            }
            else if (attach_result != PlayerAttachResult::AlreadyAttached)
            {
                return FramePostResult::InvalidPayload;
            }
            actor = authenticate->player;
        }
        else if (const auto player = _sessions.playerFor(envelope.connection))
        {
            actor = *player;
        }

        PostResult post_result;
        try
        {
            post_result = _commands.tryPost(RoutedCommand{
                .connection = envelope.connection,
                .route =
                    PlayerCommandRoute{
                        .actor = actor,
                        .command = std::move(*dispatch_result.command),
                    },
            });
        }
        catch (...)
        {
            if (new_attachment)
            {
                _sessions.rollbackAttach(envelope.connection, *new_attachment);
            }
            throw;
        }

        if (post_result == PostResult::Accepted)
        {
            if (actor.kind() == snf::runtime::ActorKind::ProvisionalPlayer)
            {
                static_cast<void>(_sessions.noteProvisionalActivity(envelope.connection));
            }
        }
        else if (new_attachment)
        {
            _sessions.rollbackAttach(envelope.connection, *new_attachment);
        }

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
        const std::optional<PlayerId> player = _sessions.playerFor(closed.connection);
        const PlayerActorId actor = player
                                        ? PlayerActorId{*player}
                                        : PlayerActorId{provisionalActorIdFor(closed.connection)};
        const bool began_persistent_close = player && _sessions.beginClose(closed.connection);

        PostResult result;
        try
        {
            result = _commands.tryPost(RoutedCommand{
                .connection = closed.connection,
                .route =
                    ConnectionClosedRoute{
                        .actor = actor,
                        .cause = closed.cause,
                    },
            });
        }
        catch (...)
        {
            if (began_persistent_close)
            {
                _sessions.rollbackClose(closed.connection);
            }
            throw;
        }

        if (result == PostResult::Full)
        {
            if (began_persistent_close)
            {
                _sessions.rollbackClose(closed.connection);
            }
            return result;
        }

        if (result == PostResult::Closed)
        {
            _sessions.abandon(closed.connection);
        }
        else if (!player)
        {
            _sessions.clearProvisionalActivity(closed.connection);
        }

        return result;
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
