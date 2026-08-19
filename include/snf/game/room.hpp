#pragma once

#include "snf/game/room_command.hpp"
#include "snf/game/room_id.hpp"
#include "snf/game/room_result.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace snf::server
{
    struct RoomConfig
    {
        // Combat is a placeholder: the battle is one delay, not a simulation, so
        // there is no periodic tick and nothing here measures a tick budget.
        std::chrono::milliseconds battle_duration{5000};
        std::size_t max_participants{4};
        std::uint64_t clear_experience{300};
    };

    // The game model, not the execution unit: being an actor is how a Room is run,
    // which is RoomActorBinding's business. This is a pure state machine with no
    // clock, no sockets and no runtime types, which is what keeps its tests
    // deterministic.
    //
    //   Waiting --StartBattle--> Running --BattleCompleted--> Cleared
    //
    class Room
    {
    public:
        explicit Room(RoomId room, RoomConfig config = {});

        [[nodiscard]] RoomId id() const noexcept;
        [[nodiscard]] RoomPhase phase() const noexcept;
        [[nodiscard]] std::size_t participantCount() const noexcept;

        [[nodiscard]] RoomResult handle(const RoomCommand& command);

    private:
        [[nodiscard]] RoomResult handleCommand(const JoinRoom& command);
        [[nodiscard]] RoomResult handleCommand(const StartBattle& command);
        [[nodiscard]] RoomResult handleCommand(const BattleCompleted& command);

        RoomId _room;
        RoomConfig _config;
        RoomPhase _phase{RoomPhase::Waiting};
        // Ascending PlayerId, so a clear emits its rewards in a deterministic order.
        std::vector<PlayerId> _participants;
    };
}
