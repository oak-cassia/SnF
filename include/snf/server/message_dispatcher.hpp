#pragma once

#include "snf/protocol/frame.hpp"

#include <functional>
#include <unordered_map>
#include <vector>

namespace snf::server
{
    enum class DispatchStatus
    {
        Handled,
        HandlerNotFound,
    };

    struct DispatchResult
    {
        DispatchStatus status;
        std::vector<snf::protocol::Frame> responses;

        [[nodiscard]] bool handled() const noexcept
        {
            return status == DispatchStatus::Handled;
        }
    };

    class MessageDispatcher
    {
    public:
        using Handler =
            std::function<std::vector<snf::protocol::Frame>(const snf::protocol::Frame&)>;

        MessageDispatcher();

        [[nodiscard]] bool registerHandler(snf::protocol::MessageType type, Handler handler);
        [[nodiscard]] DispatchResult dispatch(const snf::protocol::Frame& request) const;

    private:
        std::unordered_map<snf::protocol::MessageType, Handler> _handlers;
    };
}
