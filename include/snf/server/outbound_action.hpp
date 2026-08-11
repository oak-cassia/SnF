#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/protocol/frame.hpp"
#include "snf/runtime/bounded_queue.hpp"

#include <chrono>
#include <string_view>
#include <variant>

namespace snf::server
{
    struct SendFrame
    {
        snf::net::ConnectionId connection;
        snf::protocol::Frame frame;
    };

    enum class CloseReason
    {
        ProtocolError,
    };

    struct CloseConnection
    {
        snf::net::ConnectionId connection;
        CloseReason reason;
    };

    using OutboundAction = std::variant<SendFrame, CloseConnection>;

    // The queue element carries its publication instant so the network backend can
    // measure hand-off wait. The publishing runtime stays unaware of the metric:
    // the sink stamps the value, and only the consumer reads it.
    struct PostedOutboundAction
    {
        OutboundAction action;
        std::chrono::steady_clock::time_point posted_at{};
    };

    using OutboundActionQueue = snf::runtime::BoundedQueue<PostedOutboundAction>;

    [[nodiscard]] constexpr std::string_view to_string(const CloseReason reason) noexcept
    {
        switch (reason)
        {
        case CloseReason::ProtocolError:
            return "ProtocolError";
        }

        return "Unknown";
    }
}
