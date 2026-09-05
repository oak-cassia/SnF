#pragma once

#include "snf/protocol/frame.hpp"
#include "snf/worker/actor_envelope.hpp"
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

    enum class ActorConnectionClosedResult : std::uint8_t
    {
        MailboxAccepted = 0,
        ActorAbsent = 1,
    };
    using ActorConnectionClosedReceiptResult = ActorConnectionClosedResult;

    struct RemoteActorMessage
    {
        ActorKey target;
        ActorEnvelope message;
    };

    struct RemoteActorConnectionClosed
    {
        ActorKey target;
        ConnectionRef connection;
        ActorEnvelope message;
    };

    struct RemoteActorConnectionClosedReceipt
    {
        ActorKey target;
        ConnectionRef connection;
        ActorConnectionClosedResult result;

        [[nodiscard]] bool operator==(const RemoteActorConnectionClosedReceipt&) const noexcept = default;
    };

    using WorkerEvent = std::
        variant<RemoteConnectionSend, RemoteConnectionClose, RemoteActorMessage, RemoteActorConnectionClosed, RemoteActorConnectionClosedReceipt>;
}
