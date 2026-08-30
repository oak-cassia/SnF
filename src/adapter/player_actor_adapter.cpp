#include "snf/adapter/player_actor_adapter.hpp"

#include "snf/adapter/game_payloads.hpp"
#include "snf/adapter/to_effects.hpp"

namespace snf::adapter
{
    snf::worker::TurnResult PlayerActorAdapter::dispatch(
        snf::worker::ActorEnvelope&& envelope,
        const snf::worker::ActorTurnContext& context
    )
    {
        if (envelope.is<PlayerCommandMessage>())
        {
            auto msg = envelope.take<PlayerCommandMessage>();
            const auto result = _player.handle(msg.command);
            const PlayerTurnContext turn_ctx{
                .connection = msg.connection,
                .request_id = msg.request_id,
                .player_id = _player.state().identity(),
                .now = context.now,
            };
            auto effects = toEffects(turn_ctx, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<PingMessage>())
        {
            auto msg = envelope.take<PingMessage>();
            const auto result = _player.handle(snf::server::PingCommand{
                .payload = std::move(msg.payload),
            });
            const PlayerTurnContext turn_ctx{
                .connection = msg.connection,
                .request_id = msg.request_id,
                .player_id = _player.state().identity(),
                .now = context.now,
            };
            auto effects = toEffects(turn_ctx, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<ExperienceGrantMessage>())
        {
            auto msg = envelope.take<ExperienceGrantMessage>();
            _player.grantStreetExperience(msg.grant.experience);
            return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
        }

        return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
    }
}
