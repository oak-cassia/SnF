#pragma once

#include "snf/game/player.hpp"
#include "snf/game/player_record.hpp"
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

        // Built from persisted state after an activation load.
        PlayerActorAdapter(const snf::server::PlayerId player_id, const snf::server::PlayerRecord& record)
            : _player(player_id)
        {
            _player.restore(record);
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
