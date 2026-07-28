#include "snf/server/game_runtime.hpp"

#include <cerrno>
#include <cstdint>
#include <sys/eventfd.h>
#include <unistd.h>
#include <utility>

namespace snf::server
{
    GameRuntime::GameRuntime(
        snf::runtime::BoundedQueue<InboundCommand>& inbound_commands,
        snf::runtime::BoundedQueue<NetworkAction>& network_actions,
        const int outbound_event_descriptor) noexcept
        : _inbound_commands(inbound_commands)
        , _network_actions(network_actions)
        , _outbound_event_descriptor(outbound_event_descriptor)
    {
    }

    void GameRuntime::run()
    {
        while (const auto command = _inbound_commands.pop())
        {
            const auto dispatch_result = _message_dispatcher.dispatch(command->frame);
            if (!dispatch_result.handled())
            {
                if (!publish(CloseConnection{
                        .connection = command->connection,
                        .reason = CloseReason::ProtocolError,
                    }))
                {
                    return;
                }

                continue;
            }

            for (const auto& response : dispatch_result.responses)
            {
                if (!publish(SendFrame{
                        .connection = command->connection,
                        .frame = response,
                    }))
                {
                    return;
                }
            }
        }

        static_cast<void>(publish(GameRuntimeDrained{}));
    }

    bool GameRuntime::publish(NetworkAction action) const
    {
        if (!_network_actions.push(std::move(action)))
        {
            return false;
        }

        signalNetwork();
        return true;
    }

    void GameRuntime::signalNetwork() const noexcept
    {
        constexpr std::uint64_t wakeup_value = 1;

        while (::write(_outbound_event_descriptor, &wakeup_value, sizeof(wakeup_value)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            // EAGAIN means the eventfd counter is already non-zero. The reactor will
            // observe that pending wake-up, so no additional write is necessary.
            return;
        }
    }
}
