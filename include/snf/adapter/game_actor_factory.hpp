#pragma once

#include "snf/game/room.hpp"
#include "snf/worker/actor.hpp"
#include "snf/worker/timer_queue.hpp"

#include <chrono>
#include <memory>

namespace snf::adapter
{
    class GameActorFactory final : public snf::worker::ActorFactory
    {
    public:
        GameActorFactory() = default;
        explicit GameActorFactory(snf::worker::TimerAdmission& timer_admission) noexcept
            : _timer_admission(&timer_admission)
        {
        }

        void setTimerAdmission(snf::worker::TimerAdmission& timer_admission) noexcept
        {
            _timer_admission = &timer_admission;
        }

        // Players only get their state from the database when a worker has one.
        // Without it construct() keeps building them empty, which is what the
        // pre-Stage-8 tests and the ping slice rely on.
        void setPlayerLoadEnabled(const bool enabled) noexcept
        {
            _player_load_enabled = enabled;
        }

        void setPlayerSaveInterval(const std::chrono::milliseconds interval) noexcept
        {
            _player_save_interval = interval;
        }

        void setRoomConfig(const snf::server::RoomConfig& config) noexcept
        {
            _room_config = config;
        }

        [[nodiscard]] snf::worker::ActorConstructionResult construct(snf::worker::ActorKey key) override;

        [[nodiscard]] snf::worker::ActorConstructionResult constructLoaded(snf::worker::ActorKey key, const snf::worker::LoadPlayerResult& loaded)
            override;

    private:
        snf::worker::TimerAdmission* _timer_admission{nullptr};
        bool _player_load_enabled{false};
        std::chrono::milliseconds _player_save_interval{std::chrono::seconds{5}};
        snf::server::RoomConfig _room_config{};
    };
}
