#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/protocol/frame.hpp"
#include "snf/server/connection_lifecycle.hpp"
#include "snf/server/post_result.hpp"

#include <utility>

namespace snf::server
{
    struct FrameEnvelope
    {
        snf::net::ConnectionId connection;
        snf::protocol::Frame frame;
    };

    enum class FramePostResult
    {
        Accepted,
        UnsupportedMessage,
        InvalidPayload,
        Full,
        Closed,
    };

    class FrameIngress
    {
    public:
        virtual ~FrameIngress() = default;

        [[nodiscard]] virtual FramePostResult tryPost(FrameEnvelope envelope) = 0;
        [[nodiscard]] virtual PostResult tryPostConnectionClosed(ConnectionClosed closed) = 0;
        virtual void close() noexcept = 0;
        virtual void cancel() noexcept = 0;
    };
}
