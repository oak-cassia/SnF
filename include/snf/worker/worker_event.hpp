#pragma once

#include "snf/protocol/frame.hpp"
#include "snf/worker/identity.hpp"

#include <cstdint>
#include <variant>

namespace snf::worker
{
    enum class CloseReason : std::uint8_t
    {
        Shutdown = 0,
        ProtocolViolation = 1,
        SlowConsumer = 2,
        PeerClosed = 3,
        Timeout = 4,
        IoError = 5,
        Overload = 6,
        Application = 7,
    };

    struct RemoteConnectionSend
    {
        ConnectionRef connection;
        snf::protocol::Frame frame;
        bool critical{false};

        [[nodiscard]] bool operator==(const RemoteConnectionSend&) const noexcept = default;
    };

    struct RemoteConnectionClose
    {
        ConnectionRef connection;
        CloseReason reason;
        bool graceful{true};

        [[nodiscard]] bool operator==(const RemoteConnectionClose&) const noexcept = default;
    };

    // RemoteActorMessage와 BlockingJobCompleted는 actor/adapter 단계에서
    // concrete event가 필요할 때 추가한다. Connection events는 지금
    // owner Worker가 직접 소비한다.
    using WorkerEvent = std::variant<RemoteConnectionSend, RemoteConnectionClose>;
}
