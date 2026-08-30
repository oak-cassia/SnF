#include "snf/adapter/to_effects.hpp"

#include "snf/adapter/protocol_encoder.hpp"

#include <utility>

namespace snf::adapter
{
    namespace
    {
        [[nodiscard]] std::optional<snf::worker::ConnectionRef> connectionFor(
            const RoomTurnContext& context,
            const snf::server::PlayerId player
        ) noexcept
        {
            for (const auto& route : context.audience_routes)
            {
                if (route.player == player)
                {
                    return route.connection;
                }
            }
            return std::nullopt;
        }
    }

    snf::worker::EffectBatch toEffects(const PlayerTurnContext& context, const snf::server::PlayerResult& result)
    {
        snf::worker::EffectBatch batch;

        if (result.room_join.has_value())
        {
            const snf::worker::ActorKey room_key{
                .kind = snf::worker::ActorKind::Room,
                .entity = result.room_join->room.value,
            };
            batch.push(snf::worker::TellActorEffect{
                .target = room_key,
                .message = GameActorPayloadRegistry::create(RoomCommandMessage{
                    .connection = context.connection,
                    .request_id = context.request_id,
                    .command =
                        snf::server::JoinRoom{
                            .player = context.player_id.value_or(snf::server::PlayerId{}),
                            .stats = result.room_join->stats,
                            .equipped_skill_id = result.room_join->equipped_skill_id,
                        },
                }),
            });
        }

        if (context.connection.has_value())
        {
            for (const auto& resp : result.responses)
            {
                batch.push(snf::worker::SendFrameEffect{
                    .connection = *context.connection,
                    .frame = encodePlayerResponse(resp.response, context.request_id),
                    .critical = false,
                });
            }
        }

        return batch;
    }

    snf::worker::EffectBatch toEffects(const ZoneTurnContext& context, const snf::server::ZoneResult& result)
    {
        snf::worker::EffectBatch batch;

        if (context.connection.has_value() && context.reply_kind.has_value())
        {
            batch.push(snf::worker::SendFrameEffect{
                .connection = *context.connection,
                .frame = encodeZoneReply(*context.reply_kind, context.zone, result, context.request_id),
                .critical = false,
            });
        }

        if (context.zone_empty)
        {
            batch.push(snf::worker::StopActorEffect{});
        }
        else if (result.tick_after.has_value())
        {
            batch.push(snf::worker::ScheduleTimerEffect{
                .deadline = context.now + *result.tick_after,
                .message = GameActorPayloadRegistry::create(ZoneTickMessage{
                    .tick = result.tick + 1,
                }),
                .reservation = std::nullopt,
            });
        }

        return batch;
    }

    snf::worker::EffectBatch toEffects(
        const RoomTurnContext& context,
        const snf::server::RoomResult& result,
        std::optional<PreparedRoomTimer> prepared_timer
    )
    {
        snf::worker::EffectBatch batch;

        if (prepared_timer.has_value())
        {
            batch.push(snf::worker::ScheduleTimerEffect{
                .deadline = prepared_timer->deadline,
                .message = std::move(prepared_timer->message),
                .reservation = std::move(prepared_timer->reservation),
            });
        }

        if (context.connection.has_value() && context.reply_kind.has_value())
        {
            batch.push(snf::worker::SendFrameEffect{
                .connection = *context.connection,
                .frame = encodeRoomReply(*context.reply_kind, context.room, result, context.request_id),
                .critical = false,
            });
        }

        if (result.digest.has_value())
        {
            const auto digest_frame = encodeBattleDigest(result, snf::protocol::UNSOLICITED_REQUEST_ID);
            if (!digest_frame.has_value())
            {
                for (const auto player : result.audience)
                {
                    if (const auto connection = connectionFor(context, player))
                    {
                        batch.push(snf::worker::CloseConnectionEffect{
                            .connection = *connection,
                            .reason = snf::worker::CloseReason::SlowConsumer,
                            .graceful = false,
                        });
                    }
                }
            }
            else
            {
                for (const auto player : result.audience)
                {
                    if (const auto connection = connectionFor(context, player))
                    {
                        batch.push(snf::worker::SendFrameEffect{
                            .connection = *connection,
                            .frame = *digest_frame,
                            .critical = false,
                        });
                    }
                }
            }
        }

        if (result.outcome.has_value())
        {
            if (*result.outcome == snf::server::BattleOutcome::Cleared)
            {
                for (const auto& grant : result.grants)
                {
                    if (const auto connection = connectionFor(context, grant.player))
                    {
                        batch.push(snf::worker::SendFrameEffect{
                            .connection = *connection,
                            .frame = encodeBattleCleared(grant.experience, snf::protocol::UNSOLICITED_REQUEST_ID),
                            .critical = false,
                        });
                    }
                }
            }
            else
            {
                const auto outcome_frame = encodeBattleFailure(result, snf::protocol::UNSOLICITED_REQUEST_ID);
                for (const auto player : result.audience)
                {
                    if (const auto connection = connectionFor(context, player))
                    {
                        batch.push(snf::worker::SendFrameEffect{
                            .connection = *connection,
                            .frame = outcome_frame,
                            .critical = false,
                        });
                    }
                }
            }

            for (const auto& grant : result.grants)
            {
                const snf::worker::ActorKey player_key{
                    .kind = snf::worker::ActorKind::Player,
                    .entity = grant.player.value,
                };
                batch.push(snf::worker::TellActorEffect{
                    .target = player_key,
                    .message = GameActorPayloadRegistry::create(ExperienceGrantMessage{
                        .grant = grant,
                    }),
                });
            }

            batch.push(snf::worker::StopActorEffect{});
        }
        else if (result.tick_after.has_value())
        {
            batch.push(snf::worker::ScheduleTimerEffect{
                .deadline = context.now + *result.tick_after,
                .message = GameActorPayloadRegistry::create(RoomTickMessage{
                    .room = context.room,
                    .tick = 1,
                }),
                .reservation = std::nullopt,
            });
        }

        return batch;
    }
}
