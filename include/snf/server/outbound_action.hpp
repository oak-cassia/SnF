#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/protocol/frame.hpp"

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
