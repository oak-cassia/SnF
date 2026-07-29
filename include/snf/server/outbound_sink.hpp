#pragma once

#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/outbound_action.hpp"

#include <stop_token>

namespace snf::server
{
    // Actor runtimes publish data-plane actions without knowing which network
    // backend owns the queue or how that backend is awakened.
    class OutboundSink
    {
    public:
        virtual ~OutboundSink() = default;

        // Blocking backpressure must remain interruptible by the publishing
        // runtime. A stopped token rejects the action without cancelling a sink
        // that may be shared by other runtimes.
        [[nodiscard]] virtual bool publish(OutboundAction action, std::stop_token stop_token) = 0;
    };

    // Current epoll adapter. A future network backend can provide another sink
    // without exposing its wake-up primitive to the game runtime.
    class EventFdOutboundSink final : public OutboundSink
    {
    public:
        EventFdOutboundSink(snf::runtime::BoundedQueue<OutboundAction>& actions,
                            int wake_descriptor);

        [[nodiscard]] bool publish(OutboundAction action, std::stop_token stop_token) override;

    private:
        void signal() const noexcept;

        snf::runtime::BoundedQueue<OutboundAction>& _actions;
        int _wake_descriptor;
    };
}
