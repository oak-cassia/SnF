#pragma once

#include "snf/protocol/frame.hpp"
#include "snf/worker/budget.hpp"
#include "snf/worker/connection.hpp"
#include "snf/worker/worker_event.hpp"

#include <chrono>
#include <cstdint>
#include <optional>

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

        // Owner Worker thread only.
        // Deadline lookup is an O(1), state-preserving, non-blocking check.
        // The actual pending store and retry policy are owned by the Sink.
        [[nodiscard]] virtual std::optional<std::chrono::steady_clock::time_point> nextActorConnectionCloseRetryDeadline() const noexcept
        {
            return std::nullopt;
        }

        // Owner Worker thread only.
        // Retry must observe the supplied count and time budget.
        // Local receipt callbacks may be invoked synchronously during retry.
        // This hook does not inline-execute or await Actor handlers.
        // The actual pending store and retry policy are owned by the Sink.
        virtual void retryActorConnectionClosed(std::chrono::steady_clock::time_point /*now*/, const CountTimeBudget& /*budget*/)
        {
        }

        // Shutdown phase D, after every connection has closed. Cancel retained
        // notifications explicitly; this is not a mailbox or cleanup receipt.
        virtual void cancelConnectionCloseRetries() noexcept
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
