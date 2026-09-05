#pragma once

#include "snf/protocol/frame.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/worker_event.hpp"

#include <cstdint>

namespace snf::worker
{
    enum class RequestPostResult : std::uint8_t
    {
        Accepted = 0,
        Invalid = 1,
        Rejected = 2,
    };

    // A request is consumed exactly once. Implementations must not retain a
    // reference to the Frame after tryPost returns. Rejected is terminal for
    // the connection; Worker never retries the moved request.
    //
    // Ownership: one instance per Worker. Every call runs on that Worker's owner
    // thread, so an implementation may keep plain, non-atomic per-connection
    // state. WorkerGroup guarantees this by calling its factory once per worker
    // index, but a caller that constructs Workers directly must not pass the same
    // sink to two of them: mutable state inside it would then be shared across
    // threads without synchronisation. Implementations that keep such state
    // should pin the owner thread in a debug assertion.
    class RequestSink
    {
    public:
        virtual ~RequestSink() = default;

        [[nodiscard]] virtual RequestPostResult tryPost(ConnectionRef connection, snf::protocol::Frame&& frame) = 0;

        // Optional lifecycle observation. It runs on the owning Worker thread
        // and is intentionally not a retry or acknowledgement mechanism.
        virtual void onConnectionClosed(ConnectionRef, CloseReason)
        {
        }

        // Connection-closed notification receipt. Called only on the connection
        // owner Worker thread. Local receipts may be invoked synchronously.
        // This acknowledges mailbox admission or actor absence, never execution
        // or domain cleanup completion. Duplicate and late receipts are possible.
        virtual void onActorConnectionClosedReceipt(ActorKey, ConnectionRef, ActorConnectionClosedResult)
        {
        }
    };

    class NullRequestSink final : public RequestSink
    {
    public:
        [[nodiscard]] RequestPostResult tryPost(ConnectionRef, snf::protocol::Frame&&) override
        {
            return RequestPostResult::Accepted;
        }
    };
}
