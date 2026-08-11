#include "snf/server/player_actor.hpp"

#include <variant>

namespace snf::server
{
    std::uint64_t PlayerState::handledCommandCount() const noexcept
    {
        return _handled_command_count;
    }

    const PlayerState& PlayerActor::state() const noexcept
    {
        return _state;
    }

    snf::runtime::ActorTask<PlayerResult> PlayerActor::handle(const PlayerCommand& command)
    {
        PlayerResult result =
            std::visit([this](const auto& value) { return handleCommand(value); }, command);
        ++_state._handled_command_count;
        co_return result;
    }

    PlayerResult PlayerActor::handleCommand(const PingCommand& command)
    {
        return PlayerResult{
            .effects =
                {
                    SendResponse{
                        .response =
                            PongResponse{
                                .request_id = command.request_id,
                                .payload = command.payload,
                            },
                    },
                },
        };
    }
}
