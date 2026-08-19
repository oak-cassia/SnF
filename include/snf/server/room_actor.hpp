#pragma once

#include "snf/server/room_command.hpp"
#include "snf/server/room_id.hpp"
#include "snf/server/room_result.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace snf::server
{
    struct RoomActorConfig
    {
        // Combat is a placeholder: the battle is one delay, not a simulation, so
        // there is no periodic tick and nothing here measures a tick budget.
        std::chrono::milliseconds battle_duration{5000};
        std::size_t max_participants{4};
        std::uint64_t clear_experience{300};
    };

    // A pure state machine, like ZoneActor: no clock, no sockets, no runtime types
    // beyond the follow-ups it asks for. That is what keeps its tests deterministic.
    //
    //   Waiting --StartBattle--> Running --BattleCompleted--> Cleared
    //
    class RoomActor
    {
    public:
        explicit RoomActor(RoomId room, RoomActorConfig config = {});

        [[nodiscard]] RoomId id() const noexcept;
        [[nodiscard]] RoomPhase phase() const noexcept;
        [[nodiscard]] std::size_t participantCount() const noexcept;

        [[nodiscard]] RoomResult handle(const RoomCommand& command);

    private:
        [[nodiscard]] RoomResult handleCommand(const JoinRoom& command);
        [[nodiscard]] RoomResult handleCommand(const StartBattle& command);
        [[nodiscard]] RoomResult handleCommand(const BattleCompleted& command);

        RoomId _room;
        RoomActorConfig _config;
        RoomPhase _phase{RoomPhase::Waiting};
        // Ascending PlayerId, so a clear emits its rewards in a deterministic order.
        std::vector<PlayerId> _participants;
    };
}
