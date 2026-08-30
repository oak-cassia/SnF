#pragma once

#include "snf/game/player.hpp"
#include "snf/worker/actor.hpp"

namespace snf::adapter
{
    class PlayerActorAdapter final : public snf::worker::ActorInstance
    {
    public:
        explicit PlayerActorAdapter(std::optional<snf::server::PlayerId> player_id = std::nullopt)
            : _player(player_id)
        {
        }

        [[nodiscard]] snf::server::Player& player() noexcept
        {
            return _player;
        }

        [[nodiscard]] const snf::server::Player& player() const noexcept
        {
            return _player;
        }

        [[nodiscard]] snf::worker::TurnResult dispatch(
            snf::worker::ActorEnvelope&& envelope,
            const snf::worker::ActorTurnContext& context
        ) override;

    private:
        snf::server::Player _player;
    };
}
