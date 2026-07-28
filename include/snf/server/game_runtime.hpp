#pragma once

#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/message_dispatcher.hpp"
#include "snf/server/runtime_types.hpp"

namespace snf::server
{
    class GameRuntime
    {
    public:
        GameRuntime(snf::runtime::BoundedQueue<InboundCommand>& inbound_commands,
                    snf::runtime::BoundedQueue<NetworkAction>& network_actions,
                    int outbound_event_descriptor) noexcept;

        GameRuntime(const GameRuntime&) = delete;
        GameRuntime& operator=(const GameRuntime&) = delete;

        void run();

    private:
        [[nodiscard]] bool publish(NetworkAction action) const;
        void signalNetwork() const noexcept;

        snf::runtime::BoundedQueue<InboundCommand>& _inbound_commands;
        snf::runtime::BoundedQueue<NetworkAction>& _network_actions;
        int _outbound_event_descriptor;
        MessageDispatcher _message_dispatcher;
    };
}
