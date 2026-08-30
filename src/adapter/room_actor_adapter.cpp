#include "snf/adapter/room_actor_adapter.hpp"

#include "snf/adapter/game_payloads.hpp"
#include "snf/adapter/to_effects.hpp"

namespace snf::adapter
{
    snf::worker::TurnResult RoomActorAdapter::dispatch(
        snf::worker::ActorEnvelope&& envelope,
        const snf::worker::ActorTurnContext& context
    )
    {
        if (envelope.is<RoomCommandMessage>())
        {
            auto msg = envelope.take<RoomCommandMessage>();

            if (std::holds_alternative<snf::server::JoinRoom>(msg.command))
            {
                const auto& join_cmd = std::get<snf::server::JoinRoom>(msg.command);
                if (msg.connection.has_value())
                {
                    _player_connections[join_cmd.player] = *msg.connection;
                }
            }

            std::vector<RoomAudienceRoute> audience_routes;
            for (const auto& [player_id, conn] : _player_connections)
            {
                audience_routes.push_back(RoomAudienceRoute{
                    .player = player_id,
                    .connection = conn,
                });
            }

            RoomTurnContext turn_ctx{
                .connection = msg.connection,
                .request_id = msg.request_id,
                .now = context.now,
                .audience_routes = std::move(audience_routes),
            };

            if (std::holds_alternative<snf::server::StartBattle>(msg.command))
            {
                if (_room.canStartBattle())
                {
                    auto deadline_msg = GameActorPayloadRegistry::create(RoomDeadlineMessage{
                        .room = _room.id(),
                        .sequence = 1,
                    });
                    const std::uint64_t charge = deadline_msg.chargedBytes();

                    std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                    if (_timer_admission != nullptr)
                    {
                        reservation = _timer_admission->tryReserve(charge, 0);
                    }

                    if (!reservation.has_value())
                    {
                        snf::server::RoomResult overloaded_result{
                            .status = snf::server::RoomCommandStatus::RuntimeOverloaded,
                            .phase = _room.phase(),
                        };
                        auto effects = toEffects(turn_ctx, overloaded_result);
                        return snf::worker::CompletedTurn{.effects = std::move(effects)};
                    }

                    const auto result = _room.handle(msg.command, context.now);
                    PreparedRoomTimer prepared_timer{
                        .deadline = context.now + result.deadline_after.value_or(std::chrono::milliseconds{90000}),
                        .message = std::move(deadline_msg),
                        .reservation = std::move(*reservation),
                    };

                    auto effects = toEffects(turn_ctx, result, std::move(prepared_timer));
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }
            }

            const auto result = _room.handle(msg.command, context.now);
            auto effects = toEffects(turn_ctx, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<RoomDeadlineMessage>())
        {
            std::vector<RoomAudienceRoute> audience_routes;
            for (const auto& [player_id, conn] : _player_connections)
            {
                audience_routes.push_back(RoomAudienceRoute{
                    .player = player_id,
                    .connection = conn,
                });
            }

            RoomTurnContext turn_ctx{
                .connection = std::nullopt,
                .request_id = 0,
                .now = context.now,
                .audience_routes = std::move(audience_routes),
            };

            const auto result = _room.handle(snf::server::BattleDeadline{}, context.now);
            auto effects = toEffects(turn_ctx, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<RoomTickMessage>())
        {
            auto msg = envelope.take<RoomTickMessage>();
            static_cast<void>(msg);

            std::vector<RoomAudienceRoute> audience_routes;
            for (const auto& [player_id, conn] : _player_connections)
            {
                audience_routes.push_back(RoomAudienceRoute{
                    .player = player_id,
                    .connection = conn,
                });
            }

            RoomTurnContext turn_ctx{
                .connection = std::nullopt,
                .request_id = 0,
                .now = context.now,
                .audience_routes = std::move(audience_routes),
            };

            const auto result = _room.handle(snf::server::RoomSimulationTick{}, context.now);
            auto effects = toEffects(turn_ctx, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
    }
}
