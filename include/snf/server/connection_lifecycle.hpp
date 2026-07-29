#pragma once

#include "snf/net/connection_id.hpp"

#include <string_view>

namespace snf::server
{
    enum class ConnectionCloseCause
    {
        PeerClosed,
        ProtocolError,
        Overflow,
        ServerShutdown,
    };

    struct ConnectionClosed
    {
        snf::net::ConnectionId connection;
        ConnectionCloseCause cause;
    };

    [[nodiscard]] constexpr std::string_view to_string(const ConnectionCloseCause cause) noexcept
    {
        switch (cause)
        {
        case ConnectionCloseCause::PeerClosed:
            return "PeerClosed";
        case ConnectionCloseCause::ProtocolError:
            return "ProtocolError";
        case ConnectionCloseCause::Overflow:
            return "Overflow";
        case ConnectionCloseCause::ServerShutdown:
            return "ServerShutdown";
        }

        return "Unknown";
    }
}
