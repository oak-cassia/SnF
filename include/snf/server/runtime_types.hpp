#pragma once

#include "snf/protocol/frame.hpp"

#include <cstdint>
#include <string_view>
#include <variant>

namespace snf::server
{
    struct ConnectionId
    {
        int descriptor{-1};
        std::uint64_t generation{0};

        [[nodiscard]] bool operator==(const ConnectionId&) const noexcept = default;
    };

    struct InboundCommand
    {
        ConnectionId connection;
        snf::protocol::Frame frame;
    };

    struct SendFrame
    {
        ConnectionId connection;
        snf::protocol::Frame frame;
    };

    enum class CloseReason
    {
        ProtocolError,
    };

    struct CloseConnection
    {
        ConnectionId connection;
        CloseReason reason;
    };

    struct GameRuntimeDrained
    {
    };

    using NetworkAction = std::variant<SendFrame, CloseConnection, GameRuntimeDrained>;

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
