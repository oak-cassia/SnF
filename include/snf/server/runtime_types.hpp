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

    // Connection generations are monotonic for the lifetime of the reactor, so they
    // provide a stable temporary routing key until authenticated user IDs exist.
    struct ActorId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const ActorId&) const noexcept = default;
    };

    struct InboundCommand
    {
        ActorId actor;
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

    struct ActorRuntimeFailed
    {
    };

    using NetworkAction =
        std::variant<SendFrame, CloseConnection, GameRuntimeDrained, ActorRuntimeFailed>;

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
