#pragma once

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
    };

    struct RemoteConnectionClose
    {
        ConnectionRef connection;
        CloseReason reason;

        [[nodiscard]] bool operator==(const RemoteConnectionClose&) const noexcept = default;
    };

    // 6단계에서 RemoteActorMessage, RemoteConnectionSend를 추가한다.
    // 9단계에서 BlockingJobCompleted를 추가한다(adapter를 쓸 때만).
    using WorkerEvent = std::variant<RemoteConnectionClose>;
}
