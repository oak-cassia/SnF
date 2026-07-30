#pragma once

#include "snf/server/player_command.hpp"
#include "snf/server/player_result.hpp"

#include <cstdint>

namespace snf::server
{
    // The state is intentionally only mutable by PlayerActor. More player data is
    // added here as persistent-player work is introduced in a later milestone.
    class PlayerState
    {
    public:
        [[nodiscard]] std::uint64_t handledCommandCount() const noexcept;

    private:
        friend class PlayerActor;

        std::uint64_t _handled_command_count{0};
    };

    class PlayerActor
    {
    public:
        PlayerActor() = default;

        PlayerActor(const PlayerActor&) = delete;
        PlayerActor& operator=(const PlayerActor&) = delete;
        PlayerActor(PlayerActor&&) noexcept = default;
        PlayerActor& operator=(PlayerActor&&) noexcept = default;

        // Only a const view escapes the actor, but it is safe to read only on the
        // owning Worker. Cross-thread queries must use an immutable snapshot or a
        // command; const does not provide synchronization.
        [[nodiscard]] const PlayerState& state() const noexcept;
        [[nodiscard]] PlayerResult handle(const PlayerCommand& command);

    private:
        [[nodiscard]] PlayerResult handleCommand(const PingCommand& command);

        PlayerState _state;
    };
}
