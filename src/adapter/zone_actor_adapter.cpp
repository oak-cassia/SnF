#include "snf/adapter/zone_actor_adapter.hpp"

#include "snf/adapter/game_payloads.hpp"
#include "snf/adapter/to_effects.hpp"

namespace snf::adapter
{
    namespace
    {
        [[nodiscard]] std::optional<ZoneReplyFrameKind> replyKind(const snf::server::ZoneCommand& command) noexcept
        {
            if (std::holds_alternative<snf::server::EnterZoneCommand>(command))
            {
                return ZoneReplyFrameKind::Entered;
            }
            if (std::holds_alternative<snf::server::MoveInZoneCommand>(command))
            {
                return ZoneReplyFrameKind::Moved;
            }
            if (std::holds_alternative<snf::server::LeaveZoneCommand>(command))
            {
                return ZoneReplyFrameKind::Left;
            }
            return std::nullopt;
        }
    }

    snf::worker::TurnResult ZoneActorAdapter::dispatch(snf::worker::ActorEnvelope&& envelope, const snf::worker::ActorTurnContext& context)
    {
        if (envelope.is<ZoneCommandMessage>())
        {
            auto msg = envelope.take<ZoneCommandMessage>();
            const auto result = _zone.handle(msg.command);
            const ZoneTurnContext turn_ctx{
                .connection = msg.connection,
                .request_id = msg.request_id,
                .zone = _zone.id(),
                .reply_kind = replyKind(msg.command),
                .now = context.now,
                .zone_empty = (_zone.playerCount() == 0),
                .reply_to = msg.reply_to,
            };
            auto effects = toEffects(turn_ctx, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<ZoneTickMessage>())
        {
            auto msg = envelope.take<ZoneTickMessage>();
            static_cast<void>(msg);
            const auto result = _zone.handle(snf::server::ZoneSimulationTick{});
            const ZoneTurnContext turn_ctx{
                .connection = std::nullopt,
                .request_id = 0,
                .zone = _zone.id(),
                .reply_kind = std::nullopt,
                .now = context.now,
                .zone_empty = (_zone.playerCount() == 0),
            };
            auto effects = toEffects(turn_ctx, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
    }
}
