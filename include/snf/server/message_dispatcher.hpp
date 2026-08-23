#pragma once

#include "snf/game/player_command.hpp"
#include "snf/protocol/frame.hpp"

#include <functional>
#include <optional>
#include <unordered_map>

namespace snf::server
{
    enum class DispatchStatus
    {
        Handled,
        HandlerNotFound,
        InvalidPayload,
    };

    struct DispatchResult
    {
        DispatchStatus status;
        std::optional<PlayerCommand> command;

        [[nodiscard]] bool handled() const noexcept
        {
            return status == DispatchStatus::Handled;
        }
    };

    class MessageDispatcher
    {
    public:
        using Handler = std::function<std::optional<PlayerCommand>(snf::protocol::Frame)>;

        MessageDispatcher();

        [[nodiscard]] bool registerHandler(snf::protocol::MessageType type, Handler handler);
        [[nodiscard]] DispatchResult dispatch(snf::protocol::Frame request) const;

    private:
        std::unordered_map<snf::protocol::MessageType, Handler> _handlers;
    };
}
