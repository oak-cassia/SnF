#include "snf/server/outbound_sink.hpp"

#include <cerrno>
#include <stdexcept>
#include <sys/eventfd.h>
#include <unistd.h>
#include <utility>

namespace snf::server
{
    EventFdOutboundSink::EventFdOutboundSink(snf::runtime::BoundedQueue<OutboundAction>& actions,
                                             const int wake_descriptor)
        : _actions(actions)
        , _wake_descriptor(wake_descriptor)
    {
        if (_wake_descriptor < 0)
        {
            throw std::invalid_argument{"Invalid outbound wake descriptor"};
        }
    }

    bool EventFdOutboundSink::publish(OutboundAction action, const std::stop_token stop_token)
    {
        if (!_actions.push(std::move(action), stop_token))
        {
            return false;
        }

        signal();
        return true;
    }

    void EventFdOutboundSink::signal() const noexcept
    {
        constexpr std::uint64_t wakeup_value = 1;

        while (::write(_wake_descriptor, &wakeup_value, sizeof(wakeup_value)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return;
        }
    }
}
