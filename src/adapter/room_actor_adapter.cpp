#include "snf/adapter/room_actor_adapter.hpp"

#include "snf/adapter/game_payloads.hpp"
#include "snf/adapter/to_effects.hpp"

#include <stdexcept>
#include <utility>

namespace snf::adapter
{
    namespace
    {
        using PlayerConnections = std::unordered_map<snf::server::PlayerId, snf::worker::ConnectionRef, snf::server::PlayerIdHash>;
        using Memberships = std::unordered_map<snf::server::PlayerId, RoomMembership, snf::server::PlayerIdHash>;

        [[nodiscard]] snf::server::RoomConfig validatedConfig(snf::server::RoomConfig config)
        {
            if (config.max_participants > MAX_ROOM_PARTICIPANTS)
            {
                throw std::invalid_argument{"RoomActorAdapter supports at most four participants"};
            }
            return config;
        }

        [[nodiscard]] std::optional<RoomReplyFrameKind> replyKind(const snf::server::RoomCommand& command) noexcept
        {
            if (std::holds_alternative<snf::server::JoinRoom>(command))
            {
                return RoomReplyFrameKind::Joined;
            }
            if (std::holds_alternative<snf::server::StartBattle>(command))
            {
                return RoomReplyFrameKind::BattleStarted;
            }
            if (std::holds_alternative<snf::server::UseSkill>(command))
            {
                return RoomReplyFrameKind::SkillAcknowledged;
            }
            if (std::holds_alternative<snf::server::SetMoveIntent>(command))
            {
                return RoomReplyFrameKind::MoveAcknowledged;
            }
            return std::nullopt;
        }

        [[nodiscard]] std::vector<RoomAudienceRoute> routesOf(const PlayerConnections& connections, const Memberships& memberships)
        {
            std::vector<RoomAudienceRoute> routes;
            routes.reserve(connections.size());
            for (const auto& [player, connection] : connections)
            {
                routes.push_back(RoomAudienceRoute{
                    .player = player,
                    .connection = connection,
                    .membership = memberships.contains(player) ? std::optional{memberships.at(player)} : std::nullopt,
                });
            }
            for (const auto& [player, membership] : memberships)
                if (!connections.contains(player))
                    routes.push_back(RoomAudienceRoute{.player = player, .membership = membership});
            return routes;
        }
    }

    RoomActorAdapter::RoomActorAdapter(
        const snf::server::RoomId room_id,
        snf::worker::TimerAdmission* timer_admission,
        snf::server::RoomConfig config
    )
        : _room(room_id, validatedConfig(std::move(config)))
        , _timer_admission(timer_admission)
    {
        _player_connections.reserve(MAX_ROOM_PARTICIPANTS);
        _memberships.reserve(MAX_ROOM_PARTICIPANTS);
    }

    snf::worker::TurnResult RoomActorAdapter::dispatch(snf::worker::ActorEnvelope&& envelope, const snf::worker::ActorTurnContext& context)
    {
        if (envelope.is<RoomCommandMessage>())
        {
            auto msg = envelope.take<RoomCommandMessage>();
            const auto command_reply_kind = replyKind(msg.command);
            if (const auto* leave = std::get_if<snf::server::LeaveRoom>(&msg.command); leave != nullptr)
            {
                const auto found = _memberships.find(leave->player);
                if ((msg.membership && (found == _memberships.end() || found->second != *msg.membership)) ||
                    (!msg.membership && found != _memberships.end()))
                {
                    // The old membership is absent. A delayed cleanup must not
                    // remove the same player's newer seat.
                    return snf::worker::CompletedTurn{
                        .effects = toEffects(
                            RoomTurnContext{
                                .request_id = msg.request_id,
                                .room = _room.id(),
                                .now = context.now,
                                .reply_to = msg.reply_to,
                                .membership = msg.membership
                            },
                            snf::server::RoomResult{
                                .status = snf::server::RoomCommandStatus::NotJoined, .phase = _room.phase(), .player = leave->player
                            }
                        )
                    };
                }
            }

            if (std::holds_alternative<snf::server::StartBattle>(msg.command) && _room.canStartBattle())
            {
                auto deadline_msg = GameActorPayloadRegistry::create(RoomDeadlineMessage{
                    .room = _room.id(),
                    .sequence = 1,
                });
                const std::uint64_t charge = deadline_msg.chargedBytes();

                std::optional<snf::worker::TimerReservation> reservation = std::nullopt;
                if (_timer_admission != nullptr)
                {
                    reservation = _timer_admission->tryReserve(charge, context.turn_id);
                }

                RoomTurnContext turn_context{
                    .connection = msg.connection,
                    .request_id = msg.request_id,
                    .room = _room.id(),
                    .reply_kind = command_reply_kind,
                    .now = context.now,
                    .audience_routes = routesOf(_player_connections, _memberships),
                    .reply_to = msg.reply_to,
                    .membership = msg.membership,
                };

                if (!reservation.has_value())
                {
                    const snf::server::RoomResult overloaded_result{
                        .status = snf::server::RoomCommandStatus::RuntimeOverloaded,
                        .phase = _room.phase(),
                    };
                    auto effects = toEffects(turn_context, overloaded_result);
                    return snf::worker::CompletedTurn{.effects = std::move(effects)};
                }

                const auto result = _room.handle(msg.command, context.now);
                if (!result.deadline_after.has_value())
                {
                    throw std::logic_error{"Started Room battle did not provide its critical deadline"};
                }

                PreparedRoomTimer prepared_timer{
                    .deadline = context.now + *result.deadline_after,
                    .message = std::move(deadline_msg),
                    .reservation = std::move(*reservation),
                };

                auto effects = toEffects(turn_context, result, std::move(prepared_timer));
                return snf::worker::CompletedTurn{.effects = std::move(effects)};
            }

            const auto result = _room.handle(msg.command, context.now);
            if (std::holds_alternative<snf::server::JoinRoom>(msg.command))
            {
                const auto& join = std::get<snf::server::JoinRoom>(msg.command);
                if (msg.membership && result.status == snf::server::RoomCommandStatus::Applied)
                    _memberships.insert_or_assign(join.player, *msg.membership);
                if (msg.connection.has_value() &&
                    (result.status == snf::server::RoomCommandStatus::Applied || result.status == snf::server::RoomCommandStatus::AlreadyJoined))
                {
                    _player_connections.insert_or_assign(join.player, *msg.connection);
                }
            }
            else if (std::holds_alternative<snf::server::LeaveRoom>(msg.command) && result.status == snf::server::RoomCommandStatus::Applied)
            {
                _player_connections.erase(std::get<snf::server::LeaveRoom>(msg.command).player);
                _memberships.erase(std::get<snf::server::LeaveRoom>(msg.command).player);
            }

            const RoomTurnContext turn_context{
                .connection = msg.connection,
                .request_id = msg.request_id,
                .room = _room.id(),
                .reply_kind = command_reply_kind,
                .now = context.now,
                .audience_routes = routesOf(_player_connections, _memberships),
                .reply_to = msg.reply_to,
                .membership = msg.membership,
            };
            auto effects = toEffects(turn_context, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<RoomDeadlineMessage>())
        {
            const auto msg = envelope.take<RoomDeadlineMessage>();
            if (msg.room != _room.id())
            {
                throw std::logic_error{"Room deadline payload does not match its Actor"};
            }

            const RoomTurnContext turn_context{
                .connection = std::nullopt,
                .request_id = 0,
                .room = _room.id(),
                .reply_kind = std::nullopt,
                .now = context.now,
                .audience_routes = routesOf(_player_connections, _memberships),
            };
            const auto result = _room.handle(snf::server::BattleDeadline{}, context.now);
            auto effects = toEffects(turn_context, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        if (envelope.is<RoomTickMessage>())
        {
            const auto msg = envelope.take<RoomTickMessage>();
            if (msg.room != _room.id())
            {
                throw std::logic_error{"Room tick payload does not match its Actor"};
            }

            const RoomTurnContext turn_context{
                .connection = std::nullopt,
                .request_id = 0,
                .room = _room.id(),
                .reply_kind = std::nullopt,
                .now = context.now,
                .audience_routes = routesOf(_player_connections, _memberships),
            };
            const auto result = _room.handle(snf::server::RoomSimulationTick{}, context.now);
            auto effects = toEffects(turn_context, result);
            return snf::worker::CompletedTurn{.effects = std::move(effects)};
        }

        return snf::worker::CompletedTurn{.effects = snf::worker::EffectBatch{}};
    }
}
