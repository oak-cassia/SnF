#pragma once

#include "snf/game/room.hpp"
#include "snf/worker/actor.hpp"
#include "snf/worker/timer_queue.hpp"

#include <optional>
#include <unordered_map>

namespace snf::adapter
{
    class RoomActorAdapter final : public snf::worker::ActorInstance
    {
    public:
        explicit RoomActorAdapter(
            snf::server::RoomId room_id,
            snf::worker::TimerAdmission* timer_admission = nullptr,
            snf::server::RoomConfig config = {}
        )
            : _room(room_id, config)
            , _timer_admission(timer_admission)
        {
        }

        [[nodiscard]] snf::server::Room& room() noexcept
        {
            return _room;
        }

        [[nodiscard]] const snf::server::Room& room() const noexcept
        {
            return _room;
        }

        void setTimerAdmission(snf::worker::TimerAdmission* timer_admission) noexcept
        {
            _timer_admission = timer_admission;
        }

        [[nodiscard]] snf::worker::TurnResult dispatch(snf::worker::ActorEnvelope&& envelope, const snf::worker::ActorTurnContext& context) override;

    private:
        snf::server::Room _room;
        snf::worker::TimerAdmission* _timer_admission{nullptr};
        std::unordered_map<snf::server::PlayerId, snf::worker::ConnectionRef, snf::server::PlayerIdHash> _player_connections{};
    };
}
