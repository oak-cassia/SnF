#pragma once

#include "snf/worker/actor.hpp"
#include "snf/worker/timer_queue.hpp"

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

        [[nodiscard]] snf::worker::ActorConstructionResult construct(snf::worker::ActorKey key) override;

        [[nodiscard]] snf::worker::ActorConstructionResult constructLoaded(snf::worker::ActorKey key, const snf::worker::LoadPlayerResult& loaded)
            override;

    private:
        snf::worker::TimerAdmission* _timer_admission{nullptr};
        bool _player_load_enabled{false};
    };
}
