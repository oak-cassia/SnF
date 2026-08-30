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

        [[nodiscard]] snf::worker::ActorConstructionResult construct(snf::worker::ActorKey key) override;

    private:
        snf::worker::TimerAdmission* _timer_admission{nullptr};
    };
}
