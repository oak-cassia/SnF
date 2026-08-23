#pragma once

#include "snf/game/player_location.hpp"
#include "snf/net/connection_id.hpp"

#include <optional>
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
        bool has_location_snapshot{false};
        std::optional<PlayerLocation> last_location;
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
