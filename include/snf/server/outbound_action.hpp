#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/protocol/frame.hpp"

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

    // The queue element carries its commit instant so the network backend can
    // measure hand-off wait. The committing runtime stays unaware of the metric: the
    // channel stamps the value, and only the consumer reads it.
    //
    // The instant is the commit, not the request for capacity. Waiting for capacity
    // suspends the actor instead of blocking the Worker, and that wait is measured as
    // the actor's suspension.
    struct PostedOutboundAction
    {
        OutboundAction action;
        std::chrono::steady_clock::time_point posted_at{};
    };

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
