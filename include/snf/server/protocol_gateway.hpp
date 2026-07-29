#pragma once

#include "snf/server/frame_ingress.hpp"
#include "snf/server/message_dispatcher.hpp"
#include "snf/server/routed_command_ingress.hpp"

namespace snf::server
{
    class ProtocolGateway final : public FrameIngress
    {
    public:
        explicit ProtocolGateway(RoutedCommandIngress& commands);
        ProtocolGateway(MessageDispatcher dispatcher, RoutedCommandIngress& commands);

        [[nodiscard]] FramePostResult tryPost(FrameEnvelope envelope) override;
        [[nodiscard]] PostResult tryPostConnectionClosed(ConnectionClosed closed) override;
        void close() noexcept override;
        void cancel() noexcept override;

    private:
        MessageDispatcher _dispatcher;
        RoutedCommandIngress& _commands;
    };
}
