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
