#include "snf/adapter/game_actor_factory.hpp"
#include "snf/adapter/game_payloads.hpp"
#include "snf/adapter/game_request_sink.hpp"
#include "snf/adapter/player_actor_adapter.hpp"
#include "snf/adapter/protocol_encoder.hpp"
#include "snf/adapter/room_actor_adapter.hpp"
#include "snf/adapter/to_effects.hpp"
#include "snf/adapter/zone_actor_adapter.hpp"
#include "snf/game/player.hpp"
#include "snf/game/room.hpp"
#include "snf/game/zone.hpp"
#include "snf/net/tcp_listener.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/actor.hpp"
#include "snf/worker/timer_queue.hpp"
#include "snf/worker/worker.hpp"

#include "socket_test_support.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

namespace
{
    using snf::test::connectClient;
    using snf::test::portOf;
    using snf::test::receiveExact;
    using snf::test::sendAll;

    class MockTimerAdmission final : public snf::worker::TimerAdmission
    {
    public:
        explicit MockTimerAdmission(const std::uint64_t capacity = 1000)
            : _capacity(capacity)
        {
        }

        [[nodiscard]] std::optional<snf::worker::TimerReservation> tryReserve(const std::uint64_t charged_bytes, const std::uint64_t turn_id) noexcept
            override
        {
            assert(turn_id != 0);
            ++try_reserve_calls;
            if (should_fail || _reserved_bytes > _capacity || charged_bytes > _capacity - _reserved_bytes)
            {
                return std::nullopt;
            }
            _reserved_bytes += charged_bytes;
            return snf::worker::TimerReservation(this, charged_bytes, turn_id);
        }

        void releaseReservation(const std::uint64_t charged_bytes) noexcept override
        {
            assert(_reserved_bytes >= charged_bytes);
            _reserved_bytes -= charged_bytes;
        }

        bool should_fail{false};
        std::uint64_t try_reserve_calls{0};
        std::uint64_t _capacity{1000};
        std::uint64_t _reserved_bytes{0};
    };

    class MockRequestSink final : public snf::worker::RequestSink
    {
    public:
        snf::worker::RequestPostResult tryPost(snf::worker::ConnectionRef, snf::protocol::Frame&&) override
        {
            return snf::worker::RequestPostResult::Accepted;
        }
    };

    [[nodiscard]] snf::worker::CompletedTurn& completedTurn(snf::worker::TurnResult& result)
    {
        assert(std::holds_alternative<snf::worker::CompletedTurn>(result));
        return std::get<snf::worker::CompletedTurn>(result);
    }

    [[nodiscard]] snf::worker::ActorEnvelope takeTellMessage(snf::worker::TurnResult& result, const std::size_t index = 0)
    {
        auto effects = completedTurn(result).effects.mutableEffects();
        assert(index < effects.size());
        assert(std::holds_alternative<snf::worker::TellActorEffect>(effects[index]));
        return std::move(std::get<snf::worker::TellActorEffect>(effects[index]).message);
    }

    void authenticateAndEnterZone(
        snf::adapter::PlayerActorAdapter& player_actor,
        snf::adapter::ZoneActorAdapter& zone_actor,
        const snf::worker::ConnectionRef connection,
        const snf::server::PlayerId player,
        const snf::server::ZonePosition position,
        const snf::worker::ActorTurnContext& context
    )
    {
        auto auth = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = connection,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = player},
        });
        auto auth_result = player_actor.dispatch(std::move(auth), context);
        static_cast<void>(completedTurn(auth_result));

        auto enter = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = connection,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{.zone = zone_actor.zone().id(), .position = position},
        });
        auto enter_result = player_actor.dispatch(std::move(enter), context);
        auto zone_result = zone_actor.dispatch(takeTellMessage(enter_result), context);
        auto player_result = player_actor.dispatch(takeTellMessage(zone_result), context);
        static_cast<void>(completedTurn(player_result));

        assert(player_actor.currentZone() == zone_actor.zone().id());
        assert(player_actor.routeEpoch() == 1);
        assert(zone_actor.zone().playerCount() == 1);
    }

    void test_actor_envelope_and_registry_contracts()
    {
        static_assert(std::is_nothrow_move_constructible_v<snf::worker::ActorEnvelope>);
        static_assert(!std::is_copy_constructible_v<snf::worker::ActorEnvelope>);
        static_assert(!std::is_copy_assignable_v<snf::worker::ActorEnvelope>);

        static_assert(snf::adapter::GameActorPayloadRegistry::isRegistered<snf::adapter::PlayerCommandMessage>());
        static_assert(snf::adapter::GameActorPayloadRegistry::isRegistered<snf::adapter::ExperienceGrantMessage>());
        static_assert(snf::adapter::GameActorPayloadRegistry::isRegistered<snf::adapter::ZoneCommandMessage>());
        static_assert(snf::adapter::GameActorPayloadRegistry::isRegistered<snf::adapter::ZoneTickMessage>());
        static_assert(snf::adapter::GameActorPayloadRegistry::isRegistered<snf::adapter::RoomCommandMessage>());
        static_assert(snf::adapter::GameActorPayloadRegistry::isRegistered<snf::adapter::RoomDeadlineMessage>());
        static_assert(snf::adapter::GameActorPayloadRegistry::isRegistered<snf::adapter::RoomTickMessage>());
        static_assert(snf::adapter::GameActorPayloadRegistry::isRegistered<snf::adapter::PingMessage>());

        auto envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PingMessage{
            .connection =
                snf::worker::ConnectionRef{
                    .id = snf::worker::ConnectionId{1}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
                },
            .request_id = 42,
            .payload = {std::byte{0x01}, std::byte{0x02}},
        });

        assert(envelope.tag() == snf::worker::ActorPayloadTraits<snf::adapter::PingMessage>::TAG);
        assert(envelope.is<snf::adapter::PingMessage>());
        assert(!envelope.is<snf::adapter::PlayerCommandMessage>());
        assert(envelope.chargedBytes() >= sizeof(snf::adapter::PingMessage) + 2);

        auto ping_msg = envelope.take<snf::adapter::PingMessage>();
        assert(ping_msg.request_id == 42);
        assert(ping_msg.payload.size() == 2);
    }

    void test_timer_reservation_raii_lifecycle()
    {
        MockTimerAdmission admission(500);

        {
            auto res = admission.tryReserve(100, 1);
            assert(res.has_value());
            assert(res->isValid());
            assert(res->chargedBytes() == 100);
            assert(admission._reserved_bytes == 100);
            // Destructor fires here and releases
        }
        assert(admission._reserved_bytes == 0);

        admission.should_fail = true;
        auto res_failed = admission.tryReserve(100, 1);
        assert(!res_failed.has_value());
        assert(admission._reserved_bytes == 0);
    }

    void test_player_adapter_and_to_effects()
    {
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{101});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{5}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        auto envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 99,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{101}},
        });

        auto result = player_actor.dispatch(std::move(envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(result));
        auto& completed = std::get<snf::worker::CompletedTurn>(result);
        assert(completed.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(completed.effects.effects()[0]));

        const auto& send_effect = std::get<snf::worker::SendFrameEffect>(completed.effects.effects()[0]);
        assert(send_effect.connection == conn);
        assert(send_effect.frame.type == snf::protocol::MessageType::Authenticated);
        assert(send_effect.frame.request_id == 99);

        // Test ExperienceGrant
        auto grant_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ExperienceGrantMessage{
            .grant =
                snf::server::StreetExperienceGrant{
                    .player = snf::server::PlayerId{101},
                    .experience = 250,
                },
        });
        auto grant_result = player_actor.dispatch(std::move(grant_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(grant_result));
        assert(player_actor.player().state().streetExperience() == 250);
    }

    void test_zone_adapter_and_to_effects()
    {
        snf::adapter::ZoneActorAdapter zone_actor(snf::server::ZoneId{1});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{7}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        auto enter_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneCommandMessage{
            .connection = conn,
            .request_id = 12,
            .command =
                snf::server::EnterZoneCommand{
                    .player = snf::server::PlayerId{101},
                    .route_epoch = 1,
                    .position = snf::server::ZonePosition{.x = 10, .y = 20},
                },
        });

        auto result = zone_actor.dispatch(std::move(enter_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(result));
        auto& completed = std::get<snf::worker::CompletedTurn>(result);
        assert(completed.effects.size() == 2);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(completed.effects.effects()[0]));
        assert(std::holds_alternative<snf::worker::ScheduleTimerEffect>(completed.effects.effects()[1]));

        const auto& send_effect = std::get<snf::worker::SendFrameEffect>(completed.effects.effects()[0]);
        assert(send_effect.frame.type == snf::protocol::MessageType::ZoneEntered);

        // Test LeaveZone (terminal / empty zone stops actor)
        auto leave_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneCommandMessage{
            .connection = conn,
            .request_id = 13,
            .command =
                snf::server::LeaveZoneCommand{
                    .player = snf::server::PlayerId{101},
                    .route_epoch = 1,
                },
        });

        auto leave_result = zone_actor.dispatch(std::move(leave_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(leave_result));
        auto& leave_completed = std::get<snf::worker::CompletedTurn>(leave_result);
        assert(leave_completed.effects.size() == 2);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(leave_completed.effects.effects()[0]));
        const auto& leave_reply = std::get<snf::worker::SendFrameEffect>(leave_completed.effects.effects()[0]);
        assert(leave_reply.frame.type == snf::protocol::MessageType::ZoneLeft);
        assert(std::holds_alternative<snf::worker::StopActorEffect>(leave_completed.effects.effects()[1]));
    }

    void test_zone_adapter_outcome_reply_to_player()
    {
        snf::adapter::ZoneActorAdapter zone_actor(snf::server::ZoneId{1});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
        };

        auto enter_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneCommandMessage{
            .connection = std::nullopt,
            .request_id = 12,
            .command =
                snf::server::EnterZoneCommand{
                    .player = snf::server::PlayerId{101},
                    .route_epoch = 1,
                    .position = snf::server::ZonePosition{.x = 10, .y = 20},
                },
            .reply_to =
                snf::adapter::WorkflowReplyTo{
                    .player = snf::server::PlayerId{101},
                    .connection_generation = snf::worker::ConnectionGeneration{1},
                    .correlation_id = 42,
                    .step = snf::adapter::WorkflowStep::ZoneEnter,
                    .route_epoch = 1,
                    .request_id = 12,
                },
        });

        auto result = zone_actor.dispatch(std::move(enter_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(result));
        auto& completed = std::get<snf::worker::CompletedTurn>(result);
        assert(completed.effects.size() == 2);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(completed.effects.effects()[0]));
        const auto& tell = std::get<snf::worker::TellActorEffect>(completed.effects.effects()[0]);
        assert(tell.target.kind == snf::worker::ActorKind::Player);
        assert(tell.target.entity == 101);
        assert(tell.message.is<snf::adapter::ZoneOutcomeMessage>());
        const auto& outcome = tell.message.get<snf::adapter::ZoneOutcomeMessage>();
        assert(outcome.correlation_id == 42);
        assert(outcome.step == snf::adapter::WorkflowStep::ZoneEnter);
        assert(outcome.result.status == snf::server::ZoneCommandStatus::Applied);
        assert(outcome.result.position.has_value());
        assert(outcome.result.position->x == 10);
        assert(outcome.result.position->y == 20);
    }

    void test_player_persists_zone_location_on_save()
    {
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{101});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{5}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        auto auth_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{101}},
        });
        auto auth_res = player_actor.dispatch(std::move(auth_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(auth_res));

        auto enter_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 15, .y = 25},
            },
        });
        auto enter_res = player_actor.dispatch(std::move(enter_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(enter_res));
        assert(!player_actor.currentZone().has_value());
        assert(!player_actor.player().state().lastLocation().has_value());

        auto enter_outcome = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{101},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{101},
                .position = snf::server::ZonePosition{.x = 15, .y = 25},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto enter_out_res = player_actor.dispatch(std::move(enter_outcome), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(enter_out_res));
        assert(player_actor.currentZone() == snf::server::ZoneId{10});
        assert(player_actor.player().state().lastLocation().has_value());
        assert(player_actor.player().state().lastLocation()->zone == snf::server::ZoneId{10});
        assert(player_actor.player().state().lastLocation()->position.x == 15);
        assert(player_actor.player().state().lastLocation()->position.y == 25);

        auto move_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::MoveRequest{
                .position = snf::server::ZonePosition{.x = 35, .y = 45},
            },
        });
        auto move_res = player_actor.dispatch(std::move(move_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(move_res));

        auto move_outcome = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{101},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::ZoneMove,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 3,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{101},
                .position = snf::server::ZonePosition{.x = 35, .y = 45},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto move_out_res = player_actor.dispatch(std::move(move_outcome), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(move_out_res));

        assert(player_actor.player().state().lastLocation().has_value());
        assert(player_actor.player().state().lastLocation()->position.x == 35);
        assert(player_actor.player().state().lastLocation()->position.y == 45);

        const auto record = player_actor.player().snapshot();
        assert(record.last_location.has_value());
        assert(record.last_location->zone == snf::server::ZoneId{10});
        assert(record.last_location->position.x == 35);
        assert(record.last_location->position.y == 45);

        auto leave_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 4,
            .request = snf::adapter::LeaveRequest{},
        });
        auto leave_res = player_actor.dispatch(std::move(leave_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(leave_res));

        auto leave_outcome = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{101},
            .connection_generation = conn.generation,
            .correlation_id = 3,
            .step = snf::adapter::WorkflowStep::ZoneLeave,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 4,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{101},
                .position = std::nullopt,
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto leave_out_res = player_actor.dispatch(std::move(leave_outcome), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(leave_out_res));
        assert(!player_actor.currentZone().has_value());
        assert(!player_actor.player().state().lastLocation().has_value());
        const auto leave_record = player_actor.player().snapshot();
        assert(!leave_record.last_location.has_value());
    }

    void test_zone_tell_timeout_rolls_back_unconfirmed_route()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{102}, &admission);
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{6}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        auto auth_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{102}},
        });
        auto auth_res = player_actor.dispatch(std::move(auth_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(auth_res));

        auto enter_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 10,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{20},
                .position = snf::server::ZonePosition{.x = 0, .y = 0},
            },
        });
        auto enter_res = player_actor.dispatch(std::move(enter_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(enter_res));
        assert(!player_actor.currentZone().has_value());

        auto timeout_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerWorkflowTimeoutMessage{
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
        });
        auto timeout_res = player_actor.dispatch(std::move(timeout_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(timeout_res));
        auto& timeout_completed = std::get<snf::worker::CompletedTurn>(timeout_res);
        assert(timeout_completed.effects.size() == 2);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(timeout_completed.effects.effects()[0]));
        const auto& cleanup_tell = std::get<snf::worker::TellActorEffect>(timeout_completed.effects.effects()[0]);
        assert(cleanup_tell.target.kind == snf::worker::ActorKind::Zone);
        assert(cleanup_tell.target.entity == 20);

        assert(std::holds_alternative<snf::worker::SendFrameEffect>(timeout_completed.effects.effects()[1]));
        const auto& reply_frame = std::get<snf::worker::SendFrameEffect>(timeout_completed.effects.effects()[1]);
        assert(reply_frame.frame.type == snf::protocol::MessageType::ZoneEntered);
        assert(reply_frame.frame.request_id == 10);
        assert(reply_frame.frame.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::TransferFailed));
        assert(!player_actor.currentZone().has_value());
    }

    void test_stale_correlation_and_epoch_ignored()
    {
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{103});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 2,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{8}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        auto auth_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{103}},
        });
        auto auth_res = player_actor.dispatch(std::move(auth_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(auth_res));

        auto enter_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 5,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{30},
                .position = snf::server::ZonePosition{.x = 1, .y = 1},
            },
        });
        auto enter_res = player_actor.dispatch(std::move(enter_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(enter_res));

        auto stale_outcome = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{103},
            .connection_generation = conn.generation,
            .correlation_id = 99,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{30},
            .route_epoch = 1,
            .request_id = 5,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{103},
                .position = std::nullopt,
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto stale_res = player_actor.dispatch(std::move(stale_outcome), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(stale_res));
        assert(std::get<snf::worker::CompletedTurn>(stale_res).effects.empty());
        assert(!player_actor.currentZone().has_value());

        auto valid_outcome = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{103},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{30},
            .route_epoch = 1,
            .request_id = 5,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{103},
                .position = snf::server::ZonePosition{.x = 1, .y = 1},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto valid_res = player_actor.dispatch(std::move(valid_outcome), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(valid_res));
        assert(player_actor.currentZone() == snf::server::ZoneId{30});

        auto dup_outcome = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{103},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{30},
            .route_epoch = 1,
            .request_id = 5,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{103},
                .position = snf::server::ZonePosition{.x = 99, .y = 99},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto dup_res = player_actor.dispatch(std::move(dup_outcome), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(dup_res));
        assert(std::get<snf::worker::CompletedTurn>(dup_res).effects.empty());
        assert(player_actor.player().state().lastLocation()->position.x == 1);
    }

    void test_zone_timeout_cleans_up_zone_participant()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{104}, &admission);
        snf::adapter::ZoneActorAdapter zone_actor(snf::server::ZoneId{50});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{9}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        auto auth_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{104}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_envelope), turn_ctx));

        // Player requests EnterZone
        auto enter_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 10,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{50},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
            },
        });
        auto enter_res = player_actor.dispatch(std::move(enter_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(enter_res));
        auto& enter_completed = std::get<snf::worker::CompletedTurn>(enter_res);

        // Zone receives EnterZoneCommand and applies it
        auto& tell_effect = std::get<snf::worker::TellActorEffect>(enter_completed.effects.mutableEffects()[0]);
        auto zone_res = zone_actor.dispatch(std::move(tell_effect.message), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(zone_res));
        assert(zone_actor.zone().playerCount() == 1);

        // Suppose the ZoneOutcomeMessage was lost in network/mailbox. PlayerActor timeout fires!
        auto timeout_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerWorkflowTimeoutMessage{
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
        });
        auto timeout_res = player_actor.dispatch(std::move(timeout_envelope), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(timeout_res));
        auto& timeout_completed = std::get<snf::worker::CompletedTurn>(timeout_res);

        // PlayerActor emitted cleanup LeaveZone tell to Zone 50
        assert(timeout_completed.effects.size() == 2);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(timeout_completed.effects.effects()[0]));
        auto& cleanup_tell = std::get<snf::worker::TellActorEffect>(timeout_completed.effects.mutableEffects()[0]);
        assert(cleanup_tell.target.kind == snf::worker::ActorKind::Zone);
        assert(cleanup_tell.target.entity == 50);

        // Zone receives cleanup LeaveZoneCommand: player count drops back to 0!
        auto zone_cleanup_res = zone_actor.dispatch(std::move(cleanup_tell.message), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(zone_cleanup_res));
        assert(zone_actor.zone().playerCount() == 0);

        // Player route remains unconfirmed
        assert(!player_actor.currentZone().has_value());
    }

    void test_cross_zone_handoff_hides_route_and_completes_with_new_epoch()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{120}, &admission);
        snf::adapter::ZoneActorAdapter source_actor(snf::server::ZoneId{10});
        snf::adapter::ZoneActorAdapter target_actor(snf::server::ZoneId{20});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };
        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{30}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };
        authenticateAndEnterZone(player_actor, source_actor, conn, snf::server::PlayerId{120}, {.x = 4, .y = 5}, turn_ctx);

        auto handoff = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{20}, .position = {.x = 30, .y = 40}},
        });
        auto handoff_result = player_actor.dispatch(std::move(handoff), turn_ctx);
        assert(!player_actor.currentZone().has_value());
        assert(player_actor.routeEpoch() == 2);
        assert(std::holds_alternative<snf::adapter::TransferringRoute>(player_actor.workflowState()));
        assert(std::get<snf::adapter::TransferringRoute>(player_actor.workflowState()).step == snf::adapter::WorkflowStep::CrossZoneLeaveSource);

        auto stale = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{120},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::CrossZoneLeaveSource,
            .zone = snf::server::ZoneId{999},
            .route_epoch = 1,
            .request_id = 3,
            .result =
                snf::server::ZoneResult{
                    .status = snf::server::ZoneCommandStatus::Applied,
                    .player = snf::server::PlayerId{120},
                    .position = snf::server::ZonePosition{.x = 4, .y = 5},
                    .route_epoch = 1,
                    .tick = 0,
                    .visible_players = {},
                },
        });
        auto stale_result = player_actor.dispatch(std::move(stale), turn_ctx);
        assert(completedTurn(stale_result).effects.empty());
        assert(std::get<snf::adapter::TransferringRoute>(player_actor.workflowState()).step == snf::adapter::WorkflowStep::CrossZoneLeaveSource);

        auto busy_move = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 4,
            .request = snf::adapter::MoveRequest{.position = {.x = 99, .y = 99}},
        });
        auto busy_result = player_actor.dispatch(std::move(busy_move), turn_ctx);
        const auto busy_effects = completedTurn(busy_result).effects.effects();
        assert(busy_effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(busy_effects[0]));
        assert(
            std::get<snf::worker::SendFrameEffect>(busy_effects[0]).frame.payload[0] ==
            static_cast<std::byte>(snf::server::ZoneCommandStatus::TransitionInProgress)
        );

        auto source_result = source_actor.dispatch(takeTellMessage(handoff_result), turn_ctx);
        assert(source_actor.zone().playerCount() == 0);
        auto target_stage_result = player_actor.dispatch(takeTellMessage(source_result), turn_ctx);
        assert(!player_actor.currentZone().has_value());
        assert(std::get<snf::adapter::TransferringRoute>(player_actor.workflowState()).step == snf::adapter::WorkflowStep::CrossZoneEnterTarget);
        assert(
            std::count_if(
                completedTurn(target_stage_result).effects.effects().begin(),
                completedTurn(target_stage_result).effects.effects().end(),
                [](const auto& effect)
                {
                    return std::holds_alternative<snf::worker::ScheduleTimerEffect>(effect);
                }
            ) == 1
        );

        auto stale_source_timeout = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerWorkflowTimeoutMessage{
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::CrossZoneLeaveSource,
        });
        auto stale_source_timeout_result = player_actor.dispatch(std::move(stale_source_timeout), turn_ctx);
        assert(completedTurn(stale_source_timeout_result).effects.empty());
        assert(std::get<snf::adapter::TransferringRoute>(player_actor.workflowState()).step == snf::adapter::WorkflowStep::CrossZoneEnterTarget);

        auto target_result = target_actor.dispatch(takeTellMessage(target_stage_result), turn_ctx);
        auto final_result = player_actor.dispatch(takeTellMessage(target_result), turn_ctx);
        assert(player_actor.currentZone() == snf::server::ZoneId{20});
        assert(player_actor.routeEpoch() == 2);
        assert(target_actor.zone().playerCount() == 1);
        assert(
            (player_actor.player().state().lastLocation() ==
             snf::server::PlayerLocation{.zone = snf::server::ZoneId{20}, .position = {.x = 30, .y = 40}})
        );

        const auto& final_effects = completedTurn(final_result).effects.effects();
        const auto reply = std::find_if(
            final_effects.begin(),
            final_effects.end(),
            [](const auto& effect)
            {
                return std::holds_alternative<snf::worker::SendFrameEffect>(effect);
            }
        );
        assert(reply != final_effects.end());
        const auto& frame = std::get<snf::worker::SendFrameEffect>(*reply).frame;
        assert(frame.type == snf::protocol::MessageType::ZoneEntered);
        assert(frame.request_id == 3);
        assert(frame.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
    }

    void test_cross_zone_definite_target_failure_restores_source_with_newer_epoch()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{121}, &admission);
        snf::adapter::ZoneActorAdapter source_actor(snf::server::ZoneId{11});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 2,
        };
        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{31}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };
        authenticateAndEnterZone(player_actor, source_actor, conn, snf::server::PlayerId{121}, {.x = 6, .y = 7}, turn_ctx);

        auto handoff = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{21}, .position = {.x = 30, .y = 40}},
        });
        auto handoff_result = player_actor.dispatch(std::move(handoff), turn_ctx);
        auto source_result = source_actor.dispatch(takeTellMessage(handoff_result), turn_ctx);
        auto target_stage_result = player_actor.dispatch(takeTellMessage(source_result), turn_ctx);
        static_cast<void>(completedTurn(target_stage_result));

        auto target_failure = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{121},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::CrossZoneEnterTarget,
            .zone = snf::server::ZoneId{21},
            .route_epoch = 2,
            .request_id = 3,
            .result =
                snf::server::ZoneResult{
                    .status = snf::server::ZoneCommandStatus::TransferFailed,
                    .player = snf::server::PlayerId{121},
                    .position = std::nullopt,
                    .route_epoch = 2,
                    .tick = 0,
                    .visible_players = {},
                },
        });
        auto restore_stage_result = player_actor.dispatch(std::move(target_failure), turn_ctx);
        assert(!player_actor.currentZone().has_value());
        assert(player_actor.routeEpoch() == 3);
        const auto& transfer = std::get<snf::adapter::TransferringRoute>(player_actor.workflowState());
        assert(transfer.step == snf::adapter::WorkflowStep::CrossZoneRestoreSource);
        assert(transfer.restore_epoch == 3);
        assert(
            std::count_if(
                completedTurn(restore_stage_result).effects.effects().begin(),
                completedTurn(restore_stage_result).effects.effects().end(),
                [](const auto& effect)
                {
                    return std::holds_alternative<snf::worker::ScheduleTimerEffect>(effect);
                }
            ) == 1
        );

        auto restored_zone_result = source_actor.dispatch(takeTellMessage(restore_stage_result), turn_ctx);
        auto restored_player_result = player_actor.dispatch(takeTellMessage(restored_zone_result), turn_ctx);
        assert(player_actor.currentZone() == snf::server::ZoneId{11});
        assert(player_actor.routeEpoch() == 3);
        assert(source_actor.zone().playerCount() == 1);
        assert((
            player_actor.player().state().lastLocation() == snf::server::PlayerLocation{.zone = snf::server::ZoneId{11}, .position = {.x = 6, .y = 7}}
        ));

        const auto& effects = completedTurn(restored_player_result).effects.effects();
        const auto reply = std::find_if(
            effects.begin(),
            effects.end(),
            [](const auto& effect)
            {
                return std::holds_alternative<snf::worker::SendFrameEffect>(effect);
            }
        );
        assert(reply != effects.end());
        const auto& frame = std::get<snf::worker::SendFrameEffect>(*reply).frame;
        assert(frame.request_id == 3);
        assert(frame.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::TransferFailed));
    }

    void test_cross_zone_stale_target_cleans_observed_epoch_and_closes()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{124}, &admission);
        snf::adapter::ZoneActorAdapter source_actor(snf::server::ZoneId{14});
        snf::adapter::ZoneActorAdapter target_actor(snf::server::ZoneId{24});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 5,
        };
        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{34}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };
        authenticateAndEnterZone(player_actor, source_actor, conn, snf::server::PlayerId{124}, {.x = 12, .y = 13}, turn_ctx);

        auto existing_target = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneCommandMessage{
            .connection = std::nullopt,
            .request_id = 0,
            .command =
                snf::server::EnterZoneCommand{
                    .player = snf::server::PlayerId{124},
                    .route_epoch = 5,
                    .position = {.x = 90, .y = 91},
                },
            .reply_to = std::nullopt,
        });
        auto existing_target_result = target_actor.dispatch(std::move(existing_target), turn_ctx);
        static_cast<void>(completedTurn(existing_target_result));
        assert(target_actor.zone().playerCount() == 1);

        auto handoff = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{24}, .position = {.x = 30, .y = 40}},
        });
        auto handoff_result = player_actor.dispatch(std::move(handoff), turn_ctx);
        auto source_result = source_actor.dispatch(takeTellMessage(handoff_result), turn_ctx);
        auto target_stage_result = player_actor.dispatch(takeTellMessage(source_result), turn_ctx);
        auto stale_target_result = target_actor.dispatch(takeTellMessage(target_stage_result), turn_ctx);
        auto terminal_result = player_actor.dispatch(takeTellMessage(stale_target_result), turn_ctx);

        assert(!player_actor.currentZone().has_value());
        assert(!player_actor.player().state().lastLocation().has_value());
        assert(player_actor.routeEpoch() == 5);
        const auto& effects = completedTurn(terminal_result).effects.effects();
        assert(effects.size() == 3);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(effects[0]));
        assert(std::holds_alternative<snf::worker::TellActorEffect>(effects[1]));
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(effects[2]));

        auto source_cleanup_result = source_actor.dispatch(takeTellMessage(terminal_result, 0), turn_ctx);
        auto target_cleanup_result = target_actor.dispatch(takeTellMessage(terminal_result, 1), turn_ctx);
        static_cast<void>(completedTurn(source_cleanup_result));
        static_cast<void>(completedTurn(target_cleanup_result));
        assert(source_actor.zone().playerCount() == 0);
        assert(target_actor.zone().playerCount() == 0);
    }

    void assert_cross_zone_stale_source_cleans_observed_epoch(const snf::adapter::WorkflowStep step)
    {
        MockTimerAdmission admission(10000);
        const snf::server::PlayerId player{127};
        snf::adapter::PlayerActorAdapter player_actor(player, &admission);
        snf::adapter::ZoneActorAdapter source_actor(snf::server::ZoneId{17});
        snf::adapter::ZoneActorAdapter target_actor(snf::server::ZoneId{27});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = {},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 7,
        };
        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{37},
            .generation = snf::worker::ConnectionGeneration{1},
            .owner = snf::worker::WorkerId{0},
        };
        authenticateAndEnterZone(player_actor, source_actor, conn, player, {.x = 18, .y = 19}, turn_ctx);

        auto handoff_result = player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
                .connection = conn,
                .request_id = 3,
                .request = snf::adapter::EnterZoneRequest{.zone = target_actor.zone().id(), .position = {.x = 40, .y = 41}},
            }),
            turn_ctx
        );
        auto source_command = takeTellMessage(handoff_result);
        if (step == snf::adapter::WorkflowStep::CrossZoneRestoreSource)
        {
            auto left = source_actor.dispatch(std::move(source_command), turn_ctx);
            auto target_stage = player_actor.dispatch(takeTellMessage(left), turn_ctx);
            auto target_command = takeTellMessage(target_stage).take<snf::adapter::ZoneCommandMessage>();
            const auto reply = *target_command.reply_to;
            // Inject a definite rejection before target Enter is applied, then
            // exercise the real source adapter's response to the restore command.
            auto restore_stage = player_actor.dispatch(
                snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
                    .player = player,
                    .connection_generation = conn.generation,
                    .correlation_id = reply.correlation_id,
                    .step = reply.step,
                    .zone = target_actor.zone().id(),
                    .route_epoch = reply.route_epoch,
                    .request_id = reply.request_id,
                    .result =
                        snf::server::ZoneResult{
                            .status = snf::server::ZoneCommandStatus::TransferFailed,
                            .player = player,
                            .position = std::nullopt,
                            .route_epoch = reply.route_epoch,
                            .tick = 0,
                            .visible_players = {},
                        },
                }),
                turn_ctx
            );
            source_command = takeTellMessage(restore_stage);
        }
        assert(std::get<snf::adapter::TransferringRoute>(player_actor.workflowState()).step == step);

        auto seeded = source_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneCommandMessage{
                .command = snf::server::EnterZoneCommand{.player = player, .route_epoch = 7, .position = {.x = 90, .y = 91}},
            }),
            turn_ctx
        );
        static_cast<void>(completedTurn(seeded));
        auto source_result = source_actor.dispatch(std::move(source_command), turn_ctx);
        auto outcome = takeTellMessage(source_result).take<snf::adapter::ZoneOutcomeMessage>();
        assert(outcome.result.status == snf::server::ZoneCommandStatus::StaleRoute);
        assert(outcome.result.route_epoch == 7);

        auto terminal = player_actor.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{outcome}), turn_ctx);
        const auto& effects = completedTurn(terminal).effects.effects();
        assert(effects.size() == 3);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(effects[2]));
        assert(!player_actor.currentZone().has_value());
        assert(!player_actor.player().state().lastLocation().has_value());
        assert(player_actor.routeEpoch() == 7);

        auto source_cleanup = takeTellMessage(terminal, 0);
        const auto& cleanup_command = source_cleanup.get<snf::adapter::ZoneCommandMessage>().command;
        assert(std::get<snf::server::LeaveZoneCommand>(cleanup_command).route_epoch == 7);
        auto cleaned_source = source_actor.dispatch(std::move(source_cleanup), turn_ctx);
        auto cleaned_target = target_actor.dispatch(takeTellMessage(terminal, 1), turn_ctx);
        static_cast<void>(completedTurn(cleaned_source));
        static_cast<void>(completedTurn(cleaned_target));
        assert(source_actor.zone().playerCount() == 0);
        assert(target_actor.zone().playerCount() == 0);

        auto duplicate = player_actor.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{outcome}), turn_ctx);
        assert(completedTurn(duplicate).effects.empty());
        auto old_timeout = player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerWorkflowTimeoutMessage{
                .correlation_id = outcome.correlation_id,
                .step = step,
            }),
            turn_ctx
        );
        assert(completedTurn(old_timeout).effects.empty());

        auto closed = player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerConnectionClosedMessage{.connection = conn}), turn_ctx
        );
        assert(completedTurn(closed).effects.empty());
        assert(!player_actor.boundConnection().has_value());

        auto next_conn = conn;
        next_conn.generation = snf::worker::ConnectionGeneration{2};
        auto authenticated = player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
                .connection = next_conn,
                .request_id = 4,
                .command = snf::server::AuthenticateCommand{.player = player},
            }),
            turn_ctx
        );
        static_cast<void>(completedTurn(authenticated));
        auto enter = player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
                .connection = next_conn,
                .request_id = 5,
                .request = snf::adapter::EnterZoneRequest{.zone = source_actor.zone().id(), .position = {.x = 20, .y = 21}},
            }),
            turn_ctx
        );
        auto entered = source_actor.dispatch(takeTellMessage(enter), turn_ctx);
        auto confirmed = player_actor.dispatch(takeTellMessage(entered), turn_ctx);
        static_cast<void>(completedTurn(confirmed));
        assert(player_actor.currentZone() == source_actor.zone().id());
        assert(player_actor.routeEpoch() == 8);
        assert(source_actor.zone().playerCount() == 1);
        assert(target_actor.zone().playerCount() == 0);
    }

    void test_cross_zone_stale_source_leave_cleans_observed_epoch_and_closes()
    {
        assert_cross_zone_stale_source_cleans_observed_epoch(snf::adapter::WorkflowStep::CrossZoneLeaveSource);
    }

    void test_cross_zone_stale_source_restore_cleans_observed_epoch_and_closes()
    {
        assert_cross_zone_stale_source_cleans_observed_epoch(snf::adapter::WorkflowStep::CrossZoneRestoreSource);
    }

    void test_cross_zone_target_timeout_cleans_both_zones_and_closes()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{122}, &admission);
        snf::adapter::ZoneActorAdapter source_actor(snf::server::ZoneId{12});
        snf::adapter::ZoneActorAdapter target_actor(snf::server::ZoneId{22});
        snf::adapter::ZoneActorAdapter queued_target_actor(snf::server::ZoneId{32});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 3,
        };
        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{32}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };
        authenticateAndEnterZone(player_actor, source_actor, conn, snf::server::PlayerId{122}, {.x = 8, .y = 9}, turn_ctx);

        auto handoff = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{22}, .position = {.x = 50, .y = 60}},
        });
        auto handoff_result = player_actor.dispatch(std::move(handoff), turn_ctx);
        auto source_result = source_actor.dispatch(takeTellMessage(handoff_result), turn_ctx);
        auto target_stage_result = player_actor.dispatch(takeTellMessage(source_result), turn_ctx);
        auto dropped_target_result = target_actor.dispatch(takeTellMessage(target_stage_result), turn_ctx);
        static_cast<void>(completedTurn(dropped_target_result));
        assert(source_actor.zone().playerCount() == 0);
        assert(target_actor.zone().playerCount() == 1);

        auto timeout = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerWorkflowTimeoutMessage{
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::CrossZoneEnterTarget,
        });
        auto timeout_result = player_actor.dispatch(std::move(timeout), turn_ctx);
        auto source_cleanup_result = source_actor.dispatch(takeTellMessage(timeout_result, 0), turn_ctx);
        auto target_cleanup_result = target_actor.dispatch(takeTellMessage(timeout_result, 1), turn_ctx);
        static_cast<void>(completedTurn(source_cleanup_result));
        static_cast<void>(completedTurn(target_cleanup_result));

        assert(source_actor.zone().playerCount() == 0);
        assert(target_actor.zone().playerCount() == 0);

        // The close effect has been decided, but the owner Worker has not yet
        // posted ConnectionClosed. Input already queued for the same binding must
        // not start a new unconfirmed route during that gap.
        auto queued_enter = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 4,
            .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{32}, .position = {.x = 70, .y = 80}},
        });
        auto queued_enter_result = player_actor.dispatch(std::move(queued_enter), turn_ctx);
        assert(completedTurn(queued_enter_result).effects.empty());
        assert(queued_target_actor.zone().playerCount() == 0);

        auto closed = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerConnectionClosedMessage{.connection = conn});
        auto closed_result = player_actor.dispatch(std::move(closed), turn_ctx);
        static_cast<void>(completedTurn(closed_result));
        assert(!player_actor.boundConnection().has_value());
        assert(!player_actor.currentZone().has_value());
        assert(!player_actor.player().state().lastLocation().has_value());
        const auto& effects = completedTurn(timeout_result).effects.effects();
        assert(
            std::count_if(
                effects.begin(),
                effects.end(),
                [](const auto& effect)
                {
                    return std::holds_alternative<snf::worker::TellActorEffect>(effect);
                }
            ) == 2
        );
        assert(
            std::count_if(
                effects.begin(),
                effects.end(),
                [](const auto& effect)
                {
                    return std::holds_alternative<snf::worker::CloseConnectionEffect>(effect);
                }
            ) == 1
        );
    }

    void test_cross_zone_admission_failure_keeps_source_route()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{123}, &admission);
        snf::adapter::ZoneActorAdapter source_actor(snf::server::ZoneId{13});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 4,
        };
        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{33}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };
        authenticateAndEnterZone(player_actor, source_actor, conn, snf::server::PlayerId{123}, {.x = 10, .y = 11}, turn_ctx);
        admission.should_fail = true;

        auto handoff = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{23}, .position = {.x = 70, .y = 80}},
        });
        auto result = player_actor.dispatch(std::move(handoff), turn_ctx);
        assert(player_actor.currentZone() == snf::server::ZoneId{13});
        assert(player_actor.routeEpoch() == 1);
        assert(source_actor.zone().playerCount() == 1);
        const auto& effects = completedTurn(result).effects.effects();
        assert(effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(effects[0]));
        assert(
            std::get<snf::worker::SendFrameEffect>(effects[0]).frame.payload[0] ==
            static_cast<std::byte>(snf::server::ZoneCommandStatus::TransferFailed)
        );
    }

    void test_cross_zone_stage_timer_admission_failures_close_known_none()
    {
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 6,
        };

        {
            MockTimerAdmission admission(10000);
            snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{125}, &admission);
            snf::adapter::ZoneActorAdapter source_actor(snf::server::ZoneId{15});
            const snf::worker::ConnectionRef conn{
                .id = snf::worker::ConnectionId{35},
                .generation = snf::worker::ConnectionGeneration{1},
                .owner = snf::worker::WorkerId{0},
            };
            authenticateAndEnterZone(player_actor, source_actor, conn, snf::server::PlayerId{125}, {.x = 14, .y = 15}, turn_ctx);

            auto handoff = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
                .connection = conn,
                .request_id = 3,
                .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{25}, .position = {.x = 40, .y = 41}},
            });
            auto handoff_result = player_actor.dispatch(std::move(handoff), turn_ctx);
            auto source_result = source_actor.dispatch(takeTellMessage(handoff_result), turn_ctx);
            admission.should_fail = true;
            auto terminal_result = player_actor.dispatch(takeTellMessage(source_result), turn_ctx);

            assert(admission.try_reserve_calls == 3);
            assert(!player_actor.currentZone().has_value());
            const auto& effects = completedTurn(terminal_result).effects.effects();
            assert(effects.size() == 3);
            assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(effects[2]));
        }

        {
            MockTimerAdmission admission(10000);
            snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{126}, &admission);
            snf::adapter::ZoneActorAdapter source_actor(snf::server::ZoneId{16});
            const snf::worker::ConnectionRef conn{
                .id = snf::worker::ConnectionId{36},
                .generation = snf::worker::ConnectionGeneration{1},
                .owner = snf::worker::WorkerId{0},
            };
            authenticateAndEnterZone(player_actor, source_actor, conn, snf::server::PlayerId{126}, {.x = 16, .y = 17}, turn_ctx);

            auto handoff = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
                .connection = conn,
                .request_id = 3,
                .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{26}, .position = {.x = 50, .y = 51}},
            });
            auto handoff_result = player_actor.dispatch(std::move(handoff), turn_ctx);
            auto source_result = source_actor.dispatch(takeTellMessage(handoff_result), turn_ctx);
            auto target_stage_result = player_actor.dispatch(takeTellMessage(source_result), turn_ctx);
            static_cast<void>(completedTurn(target_stage_result));

            admission.should_fail = true;
            auto target_failure = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
                .player = snf::server::PlayerId{126},
                .connection_generation = conn.generation,
                .correlation_id = 2,
                .step = snf::adapter::WorkflowStep::CrossZoneEnterTarget,
                .zone = snf::server::ZoneId{26},
                .route_epoch = 2,
                .request_id = 3,
                .result =
                    snf::server::ZoneResult{
                        .status = snf::server::ZoneCommandStatus::TransferFailed,
                        .player = snf::server::PlayerId{126},
                        .position = std::nullopt,
                        .route_epoch = 2,
                        .tick = 0,
                        .visible_players = {},
                    },
            });
            auto terminal_result = player_actor.dispatch(std::move(target_failure), turn_ctx);

            assert(admission.try_reserve_calls == 4);
            assert(!player_actor.currentZone().has_value());
            const auto& effects = completedTurn(terminal_result).effects.effects();
            assert(effects.size() == 3);
            assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(effects[2]));
        }
    }

    void test_room_join_and_return_saga_round_trip()
    {
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{105});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{15}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // 1. Authenticate
        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{105}},
        });
        auto auth_res = player_actor.dispatch(std::move(auth_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(auth_res));

        // 2. Enter Zone 10 at (10, 20)
        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 10, .y = 20},
            },
        });
        auto enter_res = player_actor.dispatch(std::move(enter_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(enter_res));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{105},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{105},
                .position = snf::server::ZonePosition{.x = 10, .y = 20},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto zone_out_res = player_actor.dispatch(std::move(zone_out), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(zone_out_res));
        assert(player_actor.currentZone() == snf::server::ZoneId{10});

        // 3. Request RoomJoin for Room 1
        auto join_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::RoomJoinRequest{
                .room = snf::server::RoomId{1},
            },
        });
        auto join_res = player_actor.dispatch(std::move(join_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(join_res));
        auto& join_comp = std::get<snf::worker::CompletedTurn>(join_res);
        assert(join_comp.effects.size() >= 1);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(join_comp.effects.effects()[0]));
        const auto& room_tell = std::get<snf::worker::TellActorEffect>(join_comp.effects.effects()[0]);
        assert(room_tell.target.kind == snf::worker::ActorKind::Room);
        assert(room_tell.target.entity == 1);
        assert(std::holds_alternative<snf::adapter::EnteringRoute>(player_actor.workflowState()));

        // 4. RoomActor returns RoomOutcomeMessage with status = Applied (correlation 2)
        auto room_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{105},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom,
            .room = snf::server::RoomId{1},
            .request_id = 3,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::Applied,
                .phase = snf::server::RoomPhase::Waiting,
            },
        });
        auto room_out_res = player_actor.dispatch(std::move(room_out), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(room_out_res));
        auto& room_out_comp = std::get<snf::worker::CompletedTurn>(room_out_res);
        // PlayerActor tells Zone 10 LeaveZoneCommand
        assert(room_out_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(room_out_comp.effects.effects()[0]));
        const auto& zone_leave_tell = std::get<snf::worker::TellActorEffect>(room_out_comp.effects.effects()[0]);
        assert(zone_leave_tell.target.kind == snf::worker::ActorKind::Zone);
        assert(zone_leave_tell.target.entity == 10);

        // 5. ZoneActor returns ZoneOutcomeMessage for RoomJoinStep2_LeaveZone
        auto zone_leave_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{105},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep2_LeaveZone,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 3,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{105},
                .position = std::nullopt,
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto zone_leave_out_res = player_actor.dispatch(std::move(zone_leave_out), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(zone_leave_out_res));
        auto& zone_leave_out_comp = std::get<snf::worker::CompletedTurn>(zone_leave_out_res);
        assert(zone_leave_out_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(zone_leave_out_comp.effects.effects()[0]));
        const auto& joined_frame = std::get<snf::worker::SendFrameEffect>(zone_leave_out_comp.effects.effects()[0]);
        assert(joined_frame.frame.type == snf::protocol::MessageType::RoomJoined);
        assert(joined_frame.frame.request_id == 3);
        assert(joined_frame.frame.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));

        // Player is now authoritative InRoom!
        assert(player_actor.isInRoom());
        assert(player_actor.currentRoom() == snf::server::RoomId{1});

        // 6. Zone Move during InRoom receives InRoom status and keeps connection
        auto move_in_room_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 4,
            .request = snf::adapter::MoveRequest{
                .position = snf::server::ZonePosition{.x = 99, .y = 99},
            },
        });
        auto move_in_room_res = player_actor.dispatch(std::move(move_in_room_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(move_in_room_res));
        auto& move_comp = std::get<snf::worker::CompletedTurn>(move_in_room_res);
        assert(move_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(move_comp.effects.effects()[0]));
        const auto& move_reply = std::get<snf::worker::SendFrameEffect>(move_comp.effects.effects()[0]);
        assert(move_reply.frame.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::InRoom));

        // 7. Request RoomLeave
        auto leave_room_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 5,
            .request = snf::adapter::RoomLeaveRequest{},
        });
        auto leave_room_res = player_actor.dispatch(std::move(leave_room_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(leave_room_res));
        auto& leave_room_comp = std::get<snf::worker::CompletedTurn>(leave_room_res);
        // Emits LeaveRoom tell to Room 1 and EnterZone tell to Zone 10
        assert(leave_room_comp.effects.size() >= 2);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(leave_room_comp.effects.effects()[0]));
        const auto& room_leave_tell = std::get<snf::worker::TellActorEffect>(leave_room_comp.effects.effects()[0]);
        assert(room_leave_tell.target.kind == snf::worker::ActorKind::Room);
        assert(room_leave_tell.target.entity == 1);

        assert(std::holds_alternative<snf::worker::TellActorEffect>(leave_room_comp.effects.effects()[1]));
        const auto& zone_enter_tell = std::get<snf::worker::TellActorEffect>(leave_room_comp.effects.effects()[1]);
        assert(zone_enter_tell.target.kind == snf::worker::ActorKind::Zone);
        assert(zone_enter_tell.target.entity == 10);
        assert(std::holds_alternative<snf::adapter::ReturningRoute>(player_actor.workflowState()));

        // 8. ZoneActor returns ZoneOutcomeMessage for RoomReturnStep1_ZoneEnter
        auto return_zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{105},
            .connection_generation = conn.generation,
            .correlation_id = 3,
            .step = snf::adapter::WorkflowStep::RoomReturnStep1_ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 2,
            .request_id = 0,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{105},
                .position = snf::server::ZonePosition{.x = 10, .y = 20},
                .route_epoch = 2,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto return_res = player_actor.dispatch(std::move(return_zone_out), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(return_res));
        auto& return_comp = std::get<snf::worker::CompletedTurn>(return_res);
        assert(return_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(return_comp.effects.effects()[0]));
        const auto& return_frame = std::get<snf::worker::SendFrameEffect>(return_comp.effects.effects()[0]);
        assert(return_frame.frame.type == snf::protocol::MessageType::ReturnedToZone);
        assert(return_frame.frame.request_id == 0);

        // Player is back in StableRoute(Zone 10)!
        assert(player_actor.currentZone() == snf::server::ZoneId{10});
        assert(!player_actor.isInRoom());
    }

    void test_room_join_refusal_keeps_zone_route()
    {
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{106});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{16}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // Authenticate & enter zone 10
        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{106}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_env), turn_ctx));

        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(enter_env), turn_ctx));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{106},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{106},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_out), turn_ctx));
        assert(player_actor.currentZone() == snf::server::ZoneId{10});

        // Request RoomJoin
        auto join_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{2}},
        });
        static_cast<void>(player_actor.dispatch(std::move(join_env), turn_ctx));
        assert(std::holds_alternative<snf::adapter::EnteringRoute>(player_actor.workflowState()));

        // Room returns RoomFull
        auto room_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{106},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom,
            .room = snf::server::RoomId{2},
            .request_id = 3,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::RoomFull,
                .phase = snf::server::RoomPhase::Waiting,
            },
        });
        auto room_res = player_actor.dispatch(std::move(room_out), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(room_res));
        auto& room_comp = std::get<snf::worker::CompletedTurn>(room_res);
        assert(room_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(room_comp.effects.effects()[0]));
        const auto& reply_frame = std::get<snf::worker::SendFrameEffect>(room_comp.effects.effects()[0]);
        assert(reply_frame.frame.type == snf::protocol::MessageType::RoomJoined);
        assert(reply_frame.frame.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::RoomFull));

        // Route rolled back to StableRoute(Zone 10)!
        assert(player_actor.currentZone() == snf::server::ZoneId{10});
        assert(!player_actor.isInRoom());
    }

    void test_room_join_zone_leave_failure_compensates_and_keeps_zone_route()
    {
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{107});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{17}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{107}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_env), turn_ctx));

        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(enter_env), turn_ctx));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{107},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{107},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_out), turn_ctx));

        // Request RoomJoin
        auto join_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{3}},
        });
        static_cast<void>(player_actor.dispatch(std::move(join_env), turn_ctx));

        // Room returns Applied -> enters step 2
        auto room_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{107},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom,
            .room = snf::server::RoomId{3},
            .request_id = 3,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::Applied,
                .phase = snf::server::RoomPhase::Waiting,
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(room_out), turn_ctx));

        // Zone returns failure for LeaveZone
        auto zone_leave_fail = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{107},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep2_LeaveZone,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 3,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::PlayerMissing,
                .player = snf::server::PlayerId{107},
                .position = std::nullopt,
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto fail_res = player_actor.dispatch(std::move(zone_leave_fail), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(fail_res));
        auto& fail_comp = std::get<snf::worker::CompletedTurn>(fail_res);

        // Compensation: LeaveRoom tell to Room 3 + EntryFailed reply to client
        assert(fail_comp.effects.size() == 2);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(fail_comp.effects.effects()[0]));
        const auto& comp_tell = std::get<snf::worker::TellActorEffect>(fail_comp.effects.effects()[0]);
        assert(comp_tell.target.kind == snf::worker::ActorKind::Room);
        assert(comp_tell.target.entity == 3);

        assert(std::holds_alternative<snf::worker::SendFrameEffect>(fail_comp.effects.effects()[1]));
        const auto& fail_reply = std::get<snf::worker::SendFrameEffect>(fail_comp.effects.effects()[1]);
        assert(fail_reply.frame.type == snf::protocol::MessageType::RoomJoined);
        assert(fail_reply.frame.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::EntryFailed));

        // Zone route maintained!
        assert(player_actor.currentZone() == snf::server::ZoneId{10});
        assert(!player_actor.isInRoom());
    }

    void test_battle_start_only_allowed_when_in_room()
    {
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{108});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{18}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{108}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_env), turn_ctx));

        // Not in room: BattleStart closes connection
        auto bs_not_in_room = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 10,
            .request = snf::adapter::BattleStartRequest{.room = snf::server::RoomId{1}},
        });
        auto res1 = player_actor.dispatch(std::move(bs_not_in_room), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(res1));
        const auto& eff1 = std::get<snf::worker::CompletedTurn>(res1).effects;
        assert(eff1.size() == 1);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(eff1.effects()[0]));

        // Other room requests while not in room also close connection
        auto skill_not_in_room = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 101,
            .request = snf::adapter::UseSkillRequest{.room = snf::server::RoomId{1}, .skill_id = snf::server::SkillId{1}, .request_sequence = 1},
        });
        auto res_skill = player_actor.dispatch(std::move(skill_not_in_room), turn_ctx);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(std::get<snf::worker::CompletedTurn>(res_skill).effects.effects()[0]));

        auto move_not_in_room = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 102,
            .request = snf::adapter::SetMoveIntentRequest{.room = snf::server::RoomId{1}, .direction = snf::server::MoveDirection::North, .request_sequence = 1},
        });
        auto res_move = player_actor.dispatch(std::move(move_not_in_room), turn_ctx);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(std::get<snf::worker::CompletedTurn>(res_move).effects.effects()[0]));

        auto leave_not_in_room = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 103,
            .request = snf::adapter::RoomLeaveRequest{},
        });
        auto res_leave = player_actor.dispatch(std::move(leave_not_in_room), turn_ctx);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(std::get<snf::worker::CompletedTurn>(res_leave).effects.effects()[0]));

        // Join room 5 via saga
        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(enter_env), turn_ctx));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{108},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{108},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_out), turn_ctx));

        auto join_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{5}},
        });
        static_cast<void>(player_actor.dispatch(std::move(join_env), turn_ctx));

        auto room_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{108},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom,
            .room = snf::server::RoomId{5},
            .request_id = 3,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::Applied,
                .phase = snf::server::RoomPhase::Waiting,
                .player = snf::server::PlayerId{108},
                .deadline_after = std::nullopt,
                .tick_after = std::nullopt,
                .boss_health = 0,
                .boss_spawned = false,
                .digest = std::nullopt,
                .outcome = std::nullopt,
                .failure_reason = std::nullopt,
                .audience = {},
                .grants = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(room_out), turn_ctx));

        auto zone_leave_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{108},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep2_LeaveZone,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 3,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{108},
                .position = std::nullopt,
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_leave_out), turn_ctx));
        assert(player_actor.isInRoom());
        assert(player_actor.currentRoom() == snf::server::RoomId{5});

        // Now in room 5: BattleStart for room 99 (different room!) closes connection
        auto bs_wrong_room = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 11,
            .request = snf::adapter::BattleStartRequest{.room = snf::server::RoomId{99}},
        });
        auto res2 = player_actor.dispatch(std::move(bs_wrong_room), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(res2));
        const auto& eff2 = std::get<snf::worker::CompletedTurn>(res2).effects;
        assert(eff2.size() == 1);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(eff2.effects()[0]));

        // UseSkill for wrong room closes connection
        auto skill_wrong_room = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 111,
            .request = snf::adapter::UseSkillRequest{.room = snf::server::RoomId{99}, .skill_id = snf::server::SkillId{1}, .request_sequence = 1},
        });
        auto res_skill_wrong = player_actor.dispatch(std::move(skill_wrong_room), turn_ctx);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(std::get<snf::worker::CompletedTurn>(res_skill_wrong).effects.effects()[0]));

        // SetMoveIntent for wrong room closes connection
        auto move_wrong_room = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 112,
            .request = snf::adapter::SetMoveIntentRequest{.room = snf::server::RoomId{99}, .direction = snf::server::MoveDirection::South, .request_sequence = 1},
        });
        auto res_move_wrong = player_actor.dispatch(std::move(move_wrong_room), turn_ctx);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(std::get<snf::worker::CompletedTurn>(res_move_wrong).effects.effects()[0]));

        // BattleStart for room 5 (matching room!) is forwarded to Room 5
        auto bs_matching_room = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 12,
            .request = snf::adapter::BattleStartRequest{.room = snf::server::RoomId{5}},
        });
        auto res3 = player_actor.dispatch(std::move(bs_matching_room), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(res3));
        auto& comp3 = std::get<snf::worker::CompletedTurn>(res3);
        assert(comp3.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(comp3.effects.effects()[0]));
        const auto& bs_tell = std::get<snf::worker::TellActorEffect>(comp3.effects.effects()[0]);
        assert(bs_tell.target.kind == snf::worker::ActorKind::Room);
        assert(bs_tell.target.entity == 5);
    }

    void test_room_join_step1_timeout_compensates_applied_room_join()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{116}, &admission);
        snf::adapter::RoomActorAdapter room_actor(snf::server::RoomId{7});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{25}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        static_cast<void>(player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
                .connection = conn,
                .request_id = 1,
                .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{116}},
            }),
            turn_ctx
        ));
        static_cast<void>(player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
                .connection = conn,
                .request_id = 2,
                .request =
                    snf::adapter::EnterZoneRequest{
                        .zone = snf::server::ZoneId{10},
                        .position = snf::server::ZonePosition{.x = 5, .y = 5},
                    },
            }),
            turn_ctx
        ));
        static_cast<void>(player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
                .player = snf::server::PlayerId{116},
                .connection_generation = conn.generation,
                .correlation_id = 1,
                .step = snf::adapter::WorkflowStep::ZoneEnter,
                .zone = snf::server::ZoneId{10},
                .route_epoch = 1,
                .request_id = 2,
                .result =
                    snf::server::ZoneResult{
                        .status = snf::server::ZoneCommandStatus::Applied,
                        .player = snf::server::PlayerId{116},
                        .position = snf::server::ZonePosition{.x = 5, .y = 5},
                        .route_epoch = 1,
                        .tick = 0,
                        .visible_players = {},
                    },
            }),
            turn_ctx
        ));

        auto join_result = player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
                .connection = conn,
                .request_id = 3,
                .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{7}},
            }),
            turn_ctx
        );
        assert(std::holds_alternative<snf::worker::CompletedTurn>(join_result));
        auto& join_completed = std::get<snf::worker::CompletedTurn>(join_result);
        assert(join_completed.effects.size() == 2);
        auto& join_tell = std::get<snf::worker::TellActorEffect>(join_completed.effects.mutableEffects()[0]);

        // Room applies JoinRoom, but its RoomOutcomeMessage is lost.
        auto room_join_result = room_actor.dispatch(std::move(join_tell.message), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(room_join_result));
        assert(room_actor.room().participantCount() == 1);

        auto timeout_result = player_actor.dispatch(
            snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerWorkflowTimeoutMessage{
                .correlation_id = 2,
                .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom,
            }),
            turn_ctx
        );
        assert(std::holds_alternative<snf::worker::CompletedTurn>(timeout_result));
        auto& timeout_completed = std::get<snf::worker::CompletedTurn>(timeout_result);
        assert(timeout_completed.effects.size() == 2);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(timeout_completed.effects.effects()[0]));
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(timeout_completed.effects.effects()[1]));

        auto& cleanup_tell = std::get<snf::worker::TellActorEffect>(timeout_completed.effects.mutableEffects()[0]);
        assert(cleanup_tell.target.kind == snf::worker::ActorKind::Room);
        assert(cleanup_tell.target.entity == 7);
        auto room_cleanup_result = room_actor.dispatch(std::move(cleanup_tell.message), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(room_cleanup_result));
        assert(room_actor.room().participantCount() == 0);

        const auto& failure_reply = std::get<snf::worker::SendFrameEffect>(timeout_completed.effects.effects()[1]);
        assert(failure_reply.frame.type == snf::protocol::MessageType::RoomJoined);
        assert(failure_reply.frame.request_id == 3);
        assert(failure_reply.frame.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::EntryFailed));
        assert(player_actor.currentZone() == snf::server::ZoneId{10});
        assert(!player_actor.isInRoom());
    }

    void test_room_join_step2_leave_zone_timeout_compensates()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{110}, &admission);
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{19}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // Authenticate
        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{110}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_env), turn_ctx));

        // Enter Zone 10
        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(enter_env), turn_ctx));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{110},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{110},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_out), turn_ctx));

        // Join Room 7
        auto join_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{7}},
        });
        auto join_res = player_actor.dispatch(std::move(join_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(join_res));

        // Step 1 outcome arrives: Room Applied.
        auto room_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{110},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom,
            .room = snf::server::RoomId{7},
            .request_id = 3,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::Applied,
                .phase = snf::server::RoomPhase::Waiting,
                .player = snf::server::PlayerId{110},
                .deadline_after = std::nullopt,
                .tick_after = std::nullopt,
                .boss_health = 0,
                .boss_spawned = false,
                .digest = std::nullopt,
                .outcome = std::nullopt,
                .failure_reason = std::nullopt,
                .audience = {},
                .grants = {},
            },
        });
        auto step1_res = player_actor.dispatch(std::move(room_out), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(step1_res));
        auto& step1_comp = std::get<snf::worker::CompletedTurn>(step1_res);
        // Step 1 must schedule Step 2 timer!
        assert(step1_comp.effects.size() == 2);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(step1_comp.effects.effects()[0]));
        assert(std::holds_alternative<snf::worker::ScheduleTimerEffect>(step1_comp.effects.effects()[1]));

        // Now suppose Zone LeaveZone outcome is lost!
        // Step 2 timer fires!
        auto timeout_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerWorkflowTimeoutMessage{
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep2_LeaveZone,
        });
        auto timeout_res = player_actor.dispatch(std::move(timeout_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(timeout_res));
        auto& timeout_comp = std::get<snf::worker::CompletedTurn>(timeout_res);
        // Must compensate: Tell Room LeaveRoom, Tell Zone LeaveZone, Send RoomJoined EntryFailed frame, CloseConnectionEffect
        assert(timeout_comp.effects.size() == 4);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(timeout_comp.effects.effects()[0]));
        const auto& leave_room_tell = std::get<snf::worker::TellActorEffect>(timeout_comp.effects.effects()[0]);
        assert(leave_room_tell.target.kind == snf::worker::ActorKind::Room);
        assert(leave_room_tell.target.entity == 7);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(timeout_comp.effects.effects()[1]));
        const auto& leave_zone_tell = std::get<snf::worker::TellActorEffect>(timeout_comp.effects.effects()[1]);
        assert(leave_zone_tell.target.kind == snf::worker::ActorKind::Zone);
        assert(leave_zone_tell.target.entity == 10);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(timeout_comp.effects.effects()[2]));
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(timeout_comp.effects.effects()[3]));

        // Route is confirmed as nullopt (fail-closed, not falsely claimed to be in zone 10)
        assert(!player_actor.currentZone().has_value());
        assert(!player_actor.isInRoom());
    }

    void test_zone_leave_timeout_sends_terminal_outcome()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{111}, &admission);
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{20}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // Authenticate
        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{111}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_env), turn_ctx));

        // Enter Zone 10
        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(enter_env), turn_ctx));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{111},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{111},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_out), turn_ctx));

        // Player requests LeaveZone
        auto leave_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::LeaveRequest{},
        });
        auto leave_res = player_actor.dispatch(std::move(leave_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(leave_res));

        // ZoneLeave timeout fires!
        auto timeout_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerWorkflowTimeoutMessage{
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::ZoneLeave,
        });
        auto timeout_res = player_actor.dispatch(std::move(timeout_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(timeout_res));
        auto& timeout_comp = std::get<snf::worker::CompletedTurn>(timeout_res);
        // Must emit TellActorEffect(LeaveZone cleanup), SendFrameEffect(ZoneLeft TransferFailed), CloseConnectionEffect
        assert(timeout_comp.effects.size() == 3);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(timeout_comp.effects.effects()[0]));
        const auto& leave_tell = std::get<snf::worker::TellActorEffect>(timeout_comp.effects.effects()[0]);
        assert(leave_tell.target.kind == snf::worker::ActorKind::Zone);
        assert(leave_tell.target.entity == 10);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(timeout_comp.effects.effects()[1]));
        const auto& send = std::get<snf::worker::SendFrameEffect>(timeout_comp.effects.effects()[1]);
        assert(send.frame.type == snf::protocol::MessageType::ZoneLeft);
        assert(send.frame.request_id == 3);
        assert(send.frame.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::TransferFailed));
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(timeout_comp.effects.effects()[2]));

        // Route is confirmed as nullopt (fail-closed, not falsely claimed to be in zone 10)
        assert(!player_actor.currentZone().has_value());
    }

    void test_pipelined_zone_moves_handled_individually()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{112}, &admission);
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{21}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // Authenticate & Enter Zone 10
        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{112}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_env), turn_ctx));

        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(enter_env), turn_ctx));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{112},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{112},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_out), turn_ctx));

        // Pipeline two Move requests: Move 1 (req 10, corr 2) and Move 2 (req 11, corr 3)
        auto move1_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 10,
            .request = snf::adapter::MoveRequest{.position = snf::server::ZonePosition{.x = 6, .y = 6}},
        });
        auto move1_res = player_actor.dispatch(std::move(move1_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(move1_res));

        auto move2_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 11,
            .request = snf::adapter::MoveRequest{.position = snf::server::ZonePosition{.x = 7, .y = 7}},
        });
        auto move2_res = player_actor.dispatch(std::move(move2_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(move2_res));

        // Outcome for Move 1 arrives:
        auto out1 = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{112},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::ZoneMove,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 10,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{112},
                .position = snf::server::ZonePosition{.x = 6, .y = 6},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto out1_res = player_actor.dispatch(std::move(out1), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(out1_res));
        auto& out1_comp = std::get<snf::worker::CompletedTurn>(out1_res);
        assert(out1_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(out1_comp.effects.effects()[0]));
        assert(std::get<snf::worker::SendFrameEffect>(out1_comp.effects.effects()[0]).frame.request_id == 10);

        // Outcome for Move 2 arrives:
        auto out2 = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{112},
            .connection_generation = conn.generation,
            .correlation_id = 3,
            .step = snf::adapter::WorkflowStep::ZoneMove,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 11,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{112},
                .position = snf::server::ZonePosition{.x = 7, .y = 7},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto out2_res = player_actor.dispatch(std::move(out2), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(out2_res));
        auto& out2_comp = std::get<snf::worker::CompletedTurn>(out2_res);
        assert(out2_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(out2_comp.effects.effects()[0]));
        assert(std::get<snf::worker::SendFrameEffect>(out2_comp.effects.effects()[0]).frame.request_id == 11);
    }

    void test_room_return_timer_admission_failure_safely_closes()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{113}, &admission);
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{22}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // 1. Authenticate
        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{113}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_env), turn_ctx));

        // 2. Enter Zone 10
        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(enter_env), turn_ctx));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{113},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{113},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_out), turn_ctx));

        // 3. Join Room 5
        auto join_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{5}},
        });
        static_cast<void>(player_actor.dispatch(std::move(join_env), turn_ctx));

        auto room_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{113},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom,
            .room = snf::server::RoomId{5},
            .request_id = 3,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::Applied,
                .phase = snf::server::RoomPhase::Waiting,
                .player = snf::server::PlayerId{113},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(room_out), turn_ctx));

        auto zone_leave_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{113},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep2_LeaveZone,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 3,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{113},
                .position = std::nullopt,
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_leave_out), turn_ctx));
        assert(player_actor.isInRoom());
        assert(player_actor.currentRoom() == snf::server::RoomId{5});

        // Case A: Player is in room, attempts RoomLeaveRequest, but timer admission FAILS!
        admission.should_fail = true;
        auto leave_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 5,
            .request = snf::adapter::RoomLeaveRequest{},
        });
        auto leave_res = player_actor.dispatch(std::move(leave_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(leave_res));
        auto& leave_comp = std::get<snf::worker::CompletedTurn>(leave_res);
        assert(leave_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(leave_comp.effects.effects()[0]));

        // Case B: Player in room receives RoomTerminalNotification, but timer admission FAILS!
        // Setup player_actor2 in room 8
        MockTimerAdmission admission2(10000);
        snf::adapter::PlayerActorAdapter player_actor2(snf::server::PlayerId{114}, &admission2);
        const snf::worker::ConnectionRef conn2{
            .id = snf::worker::ConnectionId{23}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };
        static_cast<void>(player_actor2.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn2, .request_id = 1, .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{114}},
        }), turn_ctx));
        static_cast<void>(player_actor2.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn2, .request_id = 2, .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{10}, .position = snf::server::ZonePosition{.x = 5, .y = 5}},
        }), turn_ctx));
        static_cast<void>(player_actor2.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{114}, .connection_generation = conn2.generation, .correlation_id = 1, .step = snf::adapter::WorkflowStep::ZoneEnter, .zone = snf::server::ZoneId{10}, .route_epoch = 1, .request_id = 2, .result = snf::server::ZoneResult{.status = snf::server::ZoneCommandStatus::Applied, .player = snf::server::PlayerId{114}, .position = snf::server::ZonePosition{.x = 5, .y = 5}, .route_epoch = 1, .tick = 0, .visible_players = {}},
        }), turn_ctx));
        static_cast<void>(player_actor2.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn2, .request_id = 3, .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{8}},
        }), turn_ctx));
        static_cast<void>(player_actor2.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{114}, .connection_generation = conn2.generation, .correlation_id = 2, .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom, .room = snf::server::RoomId{8}, .request_id = 3, .result = snf::server::RoomResult{.status = snf::server::RoomCommandStatus::Applied, .player = snf::server::PlayerId{114}},
        }), turn_ctx));
        static_cast<void>(player_actor2.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{114}, .connection_generation = conn2.generation, .correlation_id = 2, .step = snf::adapter::WorkflowStep::RoomJoinStep2_LeaveZone, .zone = snf::server::ZoneId{10}, .route_epoch = 1, .request_id = 3, .result = snf::server::ZoneResult{.status = snf::server::ZoneCommandStatus::Applied, .player = snf::server::PlayerId{114}, .position = std::nullopt, .route_epoch = 1, .tick = 0, .visible_players = {}},
        }), turn_ctx));
        assert(player_actor2.isInRoom());
        assert(player_actor2.currentRoom() == snf::server::RoomId{8});

        // Now set admission2 to fail
        admission2.should_fail = true;
        auto term_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{114},
            .connection_generation = conn2.generation,
            .correlation_id = 0,
            .step = snf::adapter::WorkflowStep::RoomTerminalNotification,
            .room = snf::server::RoomId{8},
            .request_id = 0,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::Applied,
                .phase = snf::server::RoomPhase::Cleared,
                .player = snf::server::PlayerId{114},
            },
        });
        auto term_res = player_actor2.dispatch(std::move(term_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(term_res));
        auto& term_comp = std::get<snf::worker::CompletedTurn>(term_res);
        assert(term_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(term_comp.effects.effects()[0]));
        assert(!player_actor2.player().state().lastLocation().has_value());
        assert(!player_actor2.isInRoom());
        assert(!player_actor2.currentZone().has_value());
    }

    void test_room_return_timeout_cleans_up_zone_participant()
    {
        MockTimerAdmission admission(10000);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{115}, &admission);
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{24}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // Authenticate, enter zone 10, join room 5
        static_cast<void>(player_actor.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn, .request_id = 1, .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{115}},
        }), turn_ctx));
        static_cast<void>(player_actor.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn, .request_id = 2, .request = snf::adapter::EnterZoneRequest{.zone = snf::server::ZoneId{10}, .position = snf::server::ZonePosition{.x = 3, .y = 3}},
        }), turn_ctx));
        static_cast<void>(player_actor.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{115}, .connection_generation = conn.generation, .correlation_id = 1, .step = snf::adapter::WorkflowStep::ZoneEnter, .zone = snf::server::ZoneId{10}, .route_epoch = 1, .request_id = 2, .result = snf::server::ZoneResult{.status = snf::server::ZoneCommandStatus::Applied, .player = snf::server::PlayerId{115}, .position = snf::server::ZonePosition{.x = 3, .y = 3}, .route_epoch = 1, .tick = 0, .visible_players = {}},
        }), turn_ctx));
        static_cast<void>(player_actor.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn, .request_id = 3, .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{5}},
        }), turn_ctx));
        static_cast<void>(player_actor.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{115}, .connection_generation = conn.generation, .correlation_id = 2, .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom, .room = snf::server::RoomId{5}, .request_id = 3, .result = snf::server::RoomResult{.status = snf::server::RoomCommandStatus::Applied, .player = snf::server::PlayerId{115}},
        }), turn_ctx));
        static_cast<void>(player_actor.dispatch(snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{115}, .connection_generation = conn.generation, .correlation_id = 2, .step = snf::adapter::WorkflowStep::RoomJoinStep2_LeaveZone, .zone = snf::server::ZoneId{10}, .route_epoch = 1, .request_id = 3, .result = snf::server::ZoneResult{.status = snf::server::ZoneCommandStatus::Applied, .player = snf::server::PlayerId{115}, .position = std::nullopt, .route_epoch = 1, .tick = 0, .visible_players = {}},
        }), turn_ctx));
        assert(player_actor.isInRoom());

        // Room terminates -> begins return saga (correlation = 3, epoch = 2)
        auto term_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{115},
            .connection_generation = conn.generation,
            .correlation_id = 0,
            .step = snf::adapter::WorkflowStep::RoomTerminalNotification,
            .room = snf::server::RoomId{5},
            .request_id = 0,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::Applied,
                .phase = snf::server::RoomPhase::Cleared,
                .player = snf::server::PlayerId{115},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(term_env), turn_ctx));

        // Return EnterZone outcome is lost! Timeout fires for correlation 3!
        auto timeout_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerWorkflowTimeoutMessage{
            .correlation_id = 3,
            .step = snf::adapter::WorkflowStep::RoomReturnStep1_ZoneEnter,
        });
        auto timeout_res = player_actor.dispatch(std::move(timeout_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(timeout_res));
        auto& timeout_comp = std::get<snf::worker::CompletedTurn>(timeout_res);
        // Must emit TellActorEffect(LeaveZoneCommand cleanup) and CloseConnectionEffect
        assert(timeout_comp.effects.size() == 2);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(timeout_comp.effects.effects()[0]));
        const auto& cleanup_tell = std::get<snf::worker::TellActorEffect>(timeout_comp.effects.effects()[0]);
        assert(cleanup_tell.target.kind == snf::worker::ActorKind::Zone);
        assert(cleanup_tell.target.entity == 10);
        assert(std::holds_alternative<snf::worker::CloseConnectionEffect>(timeout_comp.effects.effects()[1]));
        assert(!player_actor.currentZone().has_value());
        assert(!player_actor.isInRoom());
    }

    void test_room_terminal_outcome_initiates_return_saga()
    {
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{109});
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{19}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // Authenticate & enter zone 10 & join room 1
        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{109}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_env), turn_ctx));

        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 12, .y = 34},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(enter_env), turn_ctx));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{109},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{109},
                .position = snf::server::ZonePosition{.x = 12, .y = 34},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_out), turn_ctx));

        auto join_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{1}},
        });
        static_cast<void>(player_actor.dispatch(std::move(join_env), turn_ctx));

        auto room_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{109},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep1_JoinRoom,
            .room = snf::server::RoomId{1},
            .request_id = 3,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::Applied,
                .phase = snf::server::RoomPhase::Waiting,
                .player = snf::server::PlayerId{109},
                .deadline_after = std::nullopt,
                .tick_after = std::nullopt,
                .boss_health = 0,
                .boss_spawned = false,
                .digest = std::nullopt,
                .outcome = std::nullopt,
                .failure_reason = std::nullopt,
                .audience = {},
                .grants = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(room_out), turn_ctx));

        auto zone_leave_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{109},
            .connection_generation = conn.generation,
            .correlation_id = 2,
            .step = snf::adapter::WorkflowStep::RoomJoinStep2_LeaveZone,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 3,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{109},
                .position = std::nullopt,
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_leave_out), turn_ctx));
        assert(player_actor.isInRoom());

        // Room emits terminal notification: BattleCleared
        auto term_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomOutcomeMessage{
            .player = snf::server::PlayerId{109},
            .connection_generation = conn.generation,
            .correlation_id = 0,
            .step = snf::adapter::WorkflowStep::RoomTerminalNotification,
            .room = snf::server::RoomId{1},
            .request_id = 0,
            .result = snf::server::RoomResult{
                .status = snf::server::RoomCommandStatus::Applied,
                .phase = snf::server::RoomPhase::Cleared,
                .player = snf::server::PlayerId{109},
                .deadline_after = std::nullopt,
                .tick_after = std::nullopt,
                .boss_health = 0,
                .boss_spawned = false,
                .digest = std::nullopt,
                .outcome = snf::server::BattleOutcome::Cleared,
                .failure_reason = std::nullopt,
                .audience = {snf::server::PlayerId{109}},
                .grants = {},
            },
        });
        auto term_res = player_actor.dispatch(std::move(term_out), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(term_res));
        auto& term_comp = std::get<snf::worker::CompletedTurn>(term_res);
        assert(std::holds_alternative<snf::adapter::ReturningRoute>(player_actor.workflowState()));
        assert(term_comp.effects.size() >= 1);
        assert(std::holds_alternative<snf::worker::TellActorEffect>(term_comp.effects.effects()[0]));
        const auto& return_tell = std::get<snf::worker::TellActorEffect>(term_comp.effects.effects()[0]);
        assert(return_tell.target.kind == snf::worker::ActorKind::Zone);
        assert(return_tell.target.entity == 10);

        // Zone returns Applied for RoomReturnStep1_ZoneEnter
        auto return_zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{109},
            .connection_generation = conn.generation,
            .correlation_id = 3,
            .step = snf::adapter::WorkflowStep::RoomReturnStep1_ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 2,
            .request_id = 0,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{109},
                .position = snf::server::ZonePosition{.x = 12, .y = 34},
                .route_epoch = 2,
                .tick = 0,
                .visible_players = {},
            },
        });
        auto ret_res = player_actor.dispatch(std::move(return_zone_out), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(ret_res));
        auto& ret_comp = std::get<snf::worker::CompletedTurn>(ret_res);
        assert(ret_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(ret_comp.effects.effects()[0]));
        const auto& ret_frame = std::get<snf::worker::SendFrameEffect>(ret_comp.effects.effects()[0]);
        assert(ret_frame.frame.type == snf::protocol::MessageType::ReturnedToZone);
        assert(player_actor.currentZone() == snf::server::ZoneId{10});
        assert(!player_actor.isInRoom());
    }

    void test_room_admission_pre_check_failure()
    {
        MockTimerAdmission admission(100);
        snf::adapter::PlayerActorAdapter player_actor(snf::server::PlayerId{110}, &admission);
        const snf::worker::ActorTurnContext turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{20}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        auto auth_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command = snf::server::AuthenticateCommand{.player = snf::server::PlayerId{110}},
        });
        static_cast<void>(player_actor.dispatch(std::move(auth_env), turn_ctx));

        auto enter_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerZoneRequestMessage{
            .connection = conn,
            .request_id = 2,
            .request = snf::adapter::EnterZoneRequest{
                .zone = snf::server::ZoneId{10},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(enter_env), turn_ctx));

        auto zone_out = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneOutcomeMessage{
            .player = snf::server::PlayerId{110},
            .connection_generation = conn.generation,
            .correlation_id = 1,
            .step = snf::adapter::WorkflowStep::ZoneEnter,
            .zone = snf::server::ZoneId{10},
            .route_epoch = 1,
            .request_id = 2,
            .result = snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{110},
                .position = snf::server::ZonePosition{.x = 5, .y = 5},
                .route_epoch = 1,
                .tick = 0,
                .visible_players = {},
            },
        });
        static_cast<void>(player_actor.dispatch(std::move(zone_out), turn_ctx));
        assert(player_actor.currentZone() == snf::server::ZoneId{10});

        // Set admission failure
        admission.should_fail = true;

        auto join_env = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PlayerRoomRequestMessage{
            .connection = conn,
            .request_id = 3,
            .request = snf::adapter::RoomJoinRequest{.room = snf::server::RoomId{1}},
        });
        auto join_res = player_actor.dispatch(std::move(join_env), turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(join_res));
        auto& join_comp = std::get<snf::worker::CompletedTurn>(join_res);
        assert(join_comp.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(join_comp.effects.effects()[0]));
        const auto& reply = std::get<snf::worker::SendFrameEffect>(join_comp.effects.effects()[0]);
        assert(reply.frame.type == snf::protocol::MessageType::RoomJoined);
        assert(reply.frame.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::RuntimeOverloaded));

        // State unchanged: remains StableRoute(Zone 10)!
        assert(player_actor.currentZone() == snf::server::ZoneId{10});
        assert(!player_actor.isInRoom());
    }

    void test_room_adapter_critical_deadline_pre_admission_success()
    {
        MockTimerAdmission admission(2048);
        snf::adapter::RoomActorAdapter room_actor(snf::server::RoomId{1}, &admission);
        const snf::worker::ActorTurnContext join_turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{3}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // Join room first
        auto join_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command =
                snf::server::JoinRoom{
                    .player = snf::server::PlayerId{101},
                    .stats = snf::server::CombatStats{.attack = 10, .health = 100},
                    .equipped_skill_id = snf::server::SLASH_SKILL_ID,
                },
        });

        auto join_result = room_actor.dispatch(std::move(join_envelope), join_turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(join_result));
        assert(room_actor.room().participantCount() == 1);
        assert(room_actor.room().canStartBattle());

        // Start battle with sufficient admission -> pre-reserves deadline timer
        auto start_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomCommandMessage{
            .connection = conn,
            .request_id = 2,
            .command = snf::server::StartBattle{},
        });

        const snf::worker::ActorTurnContext start_turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = join_turn_ctx.now,
            .turn_id = 2,
        };
        auto start_result = room_actor.dispatch(std::move(start_envelope), start_turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(start_result));
        auto& start_completed = std::get<snf::worker::CompletedTurn>(start_result);

        // Verify that the FIRST effect is the reserved ScheduleTimerEffect!
        assert(start_completed.effects.size() >= 2);
        assert(std::holds_alternative<snf::worker::ScheduleTimerEffect>(start_completed.effects.effects()[0]));
        const auto& timer_effect = std::get<snf::worker::ScheduleTimerEffect>(start_completed.effects.effects()[0]);
        assert(timer_effect.reservation.has_value());
        assert(timer_effect.reservation->isValid());
        assert(timer_effect.reservation->admission() == &admission);
        assert(timer_effect.reservation->turnId() == start_turn_ctx.turn_id);

        const auto& reply_effect = std::get<snf::worker::SendFrameEffect>(start_completed.effects.effects()[1]);
        assert(reply_effect.frame.type == snf::protocol::MessageType::BattleStarted);

        assert(room_actor.room().phase() == snf::server::RoomPhase::Running);
    }

    void test_room_adapter_critical_deadline_pre_admission_failure()
    {
        MockTimerAdmission admission(2048);
        admission.should_fail = true; // Inject admission failure!

        snf::adapter::RoomActorAdapter room_actor(snf::server::RoomId{1}, &admission);
        const snf::worker::ActorTurnContext join_turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };

        const snf::worker::ConnectionRef conn{
            .id = snf::worker::ConnectionId{3}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
        };

        // Join room first
        auto join_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomCommandMessage{
            .connection = conn,
            .request_id = 1,
            .command =
                snf::server::JoinRoom{
                    .player = snf::server::PlayerId{101},
                    .stats = snf::server::CombatStats{.attack = 10, .health = 100},
                    .equipped_skill_id = snf::server::SLASH_SKILL_ID,
                },
        });

        auto join_result = room_actor.dispatch(std::move(join_envelope), join_turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(join_result));
        assert(room_actor.room().canStartBattle());

        // Attempt StartBattle -> admission fails!
        auto start_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomCommandMessage{
            .connection = conn,
            .request_id = 2,
            .command = snf::server::StartBattle{},
        });

        const snf::worker::ActorTurnContext start_turn_ctx{
            .activation = snf::worker::ActivationRef{},
            .now = join_turn_ctx.now,
            .turn_id = 2,
        };
        auto start_result = room_actor.dispatch(std::move(start_envelope), start_turn_ctx);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(start_result));
        auto& start_completed = std::get<snf::worker::CompletedTurn>(start_result);

        // Room state MUST NOT change! Phase remains Waiting!
        assert(room_actor.room().phase() == snf::server::RoomPhase::Waiting);

        // Completed effects must contain only the RuntimeOverloaded reply
        assert(start_completed.effects.size() == 1);
        assert(std::holds_alternative<snf::worker::SendFrameEffect>(start_completed.effects.effects()[0]));
        const auto& reply_effect = std::get<snf::worker::SendFrameEffect>(start_completed.effects.effects()[0]);
        assert(reply_effect.frame.type == snf::protocol::MessageType::BattleStarted);
        assert(static_cast<snf::server::RoomCommandStatus>(reply_effect.frame.payload[0]) == snf::server::RoomCommandStatus::RuntimeOverloaded);
    }

    void test_room_routes_remain_bounded_and_follow_result_audience()
    {
        MockTimerAdmission admission(2048);
        snf::server::RoomConfig config{};
        config.max_participants = 1;
        snf::adapter::RoomActorAdapter room_actor(snf::server::RoomId{1}, &admission, config);

        const snf::worker::ActorTurnContext turn_context{
            .activation = snf::worker::ActivationRef{},
            .now = std::chrono::steady_clock::now(),
            .turn_id = 1,
        };
        const snf::worker::ConnectionRef first_connection{
            .id = snf::worker::ConnectionId{3},
            .generation = snf::worker::ConnectionGeneration{1},
            .owner = snf::worker::WorkerId{0},
        };
        const snf::worker::ConnectionRef rejected_connection{
            .id = snf::worker::ConnectionId{4},
            .generation = snf::worker::ConnectionGeneration{1},
            .owner = snf::worker::WorkerId{0},
        };

        auto join_first = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomCommandMessage{
            .connection = first_connection,
            .request_id = 1,
            .command =
                snf::server::JoinRoom{
                    .player = snf::server::PlayerId{101},
                    .stats = snf::server::CombatStats{.attack = 10, .health = 100},
                    .equipped_skill_id = snf::server::SLASH_SKILL_ID,
                },
        });
        auto first_result = room_actor.dispatch(std::move(join_first), turn_context);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(first_result));

        auto join_rejected = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomCommandMessage{
            .connection = rejected_connection,
            .request_id = 2,
            .command =
                snf::server::JoinRoom{
                    .player = snf::server::PlayerId{202},
                    .stats = snf::server::CombatStats{.attack = 10, .health = 100},
                    .equipped_skill_id = snf::server::SLASH_SKILL_ID,
                },
        });
        auto rejected_result = room_actor.dispatch(std::move(join_rejected), turn_context);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(rejected_result));
        const auto& rejected_effects = std::get<snf::worker::CompletedTurn>(rejected_result).effects;
        assert(rejected_effects.size() == 1);
        const auto& rejected_reply = std::get<snf::worker::SendFrameEffect>(rejected_effects.effects()[0]);
        assert(static_cast<snf::server::RoomCommandStatus>(rejected_reply.frame.payload[0]) == snf::server::RoomCommandStatus::RoomFull);

        auto start = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::RoomCommandMessage{
            .connection = first_connection,
            .request_id = 3,
            .command = snf::server::StartBattle{},
        });
        const snf::worker::ActorTurnContext start_context{
            .activation = snf::worker::ActivationRef{},
            .now = turn_context.now,
            .turn_id = 2,
        };
        auto start_result = room_actor.dispatch(std::move(start), start_context);
        assert(std::holds_alternative<snf::worker::CompletedTurn>(start_result));
        for (const auto& effect : std::get<snf::worker::CompletedTurn>(start_result).effects.effects())
        {
            if (const auto* send = std::get_if<snf::worker::SendFrameEffect>(&effect))
            {
                assert(send->connection != rejected_connection);
            }
        }

        const snf::adapter::RoomTurnContext audience_context{
            .room = snf::server::RoomId{1},
            .now = turn_context.now,
            .audience_routes =
                {
                    snf::adapter::RoomAudienceRoute{
                        .player = snf::server::PlayerId{101},
                        .connection = first_connection,
                    },
                    snf::adapter::RoomAudienceRoute{
                        .player = snf::server::PlayerId{202},
                        .connection = rejected_connection,
                    },
                },
        };
        const snf::server::RoomResult audience_result{
            .phase = snf::server::RoomPhase::Running,
            .digest = snf::server::BattleDigest{.sequence = 9},
            .audience = {snf::server::PlayerId{101}},
        };
        const auto audience_effects = snf::adapter::toEffects(audience_context, audience_result);
        assert(audience_effects.size() == 1);
        const auto& audience_send = std::get<snf::worker::SendFrameEffect>(audience_effects.effects()[0]);
        assert(audience_send.connection == first_connection);

        config.max_participants = snf::adapter::MAX_ROOM_PARTICIPANTS + 1;
        bool rejected_config = false;
        try
        {
            [[maybe_unused]] snf::adapter::RoomActorAdapter invalid_room(snf::server::RoomId{2}, &admission, config);
        }
        catch (const std::invalid_argument&)
        {
            rejected_config = true;
        }
        assert(rejected_config);
    }

    void test_timer_queue_application_accounting()
    {
        snf::worker::TimerQueue queue(512);
        assert(queue.tryReserveApplicationTimer(60));
        assert(queue.reservedApplicationTimerCount() == 1);
        assert(queue.reservedApplicationTimerBytes() == 60);
        assert(!queue.tryReserveApplicationTimer(std::numeric_limits<std::uint64_t>::max()));
        queue.releaseApplicationTimerReservation(60);
        assert(queue.reservedApplicationTimerCount() == 0);
        assert(queue.reservedApplicationTimerBytes() == 0);

        assert(queue.tryReserveApplicationTimer(0));
        assert(queue.reservedApplicationTimerCount() == 1);
        queue.releaseApplicationTimerReservation(0);
        assert(queue.reservedApplicationTimerCount() == 0);

        auto message = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::PingMessage{
            .connection =
                snf::worker::ConnectionRef{
                    .id = snf::worker::ConnectionId{1},
                    .generation = snf::worker::ConnectionGeneration{1},
                    .owner = snf::worker::WorkerId{0},
                },
            .request_id = 7,
            .payload = {std::byte{0x01}, std::byte{0x02}},
        });
        const auto charge = message.chargedBytes();
        assert(queue.tryScheduleApplicationTimer(std::chrono::steady_clock::now() + 1h, snf::worker::ActivationRef{}, std::move(message)));
        assert(queue.applicationTimerCount() == 1);
        assert(queue.applicationTimerBytes() == charge);
        assert(queue.cancelApplicationTimers() == 1);
        assert(queue.applicationTimerCount() == 0);
        assert(queue.applicationTimerBytes() == 0);
    }

    void test_effect_batch_validation_and_mutual_exclusion()
    {
        snf::worker::EffectBatch batch;
        for (std::size_t i = 0; i < snf::worker::EffectBatch::MAX_EFFECTS; ++i)
        {
            assert(batch.tryPush(snf::worker::SendFrameEffect{
                .connection =
                    snf::worker::ConnectionRef{
                        .id = snf::worker::ConnectionId{1}, .generation = snf::worker::ConnectionGeneration{1}, .owner = snf::worker::WorkerId{0}
                    },
                .frame = snf::protocol::Frame{},
                .critical = false,
            }));
        }
        assert(batch.size() == snf::worker::EffectBatch::MAX_EFFECTS);
        assert(!batch.tryPush(snf::worker::StopActorEffect{}));
    }

    void test_worker_application_timer_schedule_and_delivery()
    {
        MockRequestSink sink;
        snf::worker::WorkerActorConfig actor_config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 2048,
            .max_mailbox_messages_total = 50,
            .max_mailbox_bytes_total = 10240,
            .max_application_timer_bytes_total = 4096,
        };

        snf::adapter::GameActorFactory factory;
        snf::worker::Worker worker(
            snf::worker::WorkerId{0}, 1, snf::worker::WorkerBudgets::defaults(), snf::worker::WorkerInboxConfig{}, actor_config, factory
        );
        factory.setTimerAdmission(worker);

        const snf::worker::ActorKey key{.kind = snf::worker::ActorKind::Zone, .entity = 1};

        // Deliver EnterZone message
        auto enter_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneCommandMessage{
            .connection = std::nullopt,
            .request_id = 1,
            .command =
                snf::server::EnterZoneCommand{
                    .player = snf::server::PlayerId{101},
                    .route_epoch = 1,
                    .position = snf::server::ZonePosition{.x = 10, .y = 20},
                },
        });

        assert(worker.tryDeliverLocal(key, std::move(enter_envelope)) == snf::worker::DeliveryResult::Accepted);

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(150ms);
        worker.requestStop();
        th.join();

        // Zone scheduled a tick timer and it was delivered!
        assert(worker.metrics().actor.application_timers_scheduled >= 1);
        assert(worker.metrics().actor.application_timers_delivered >= 1);
    }

    void test_shutdown_cancels_application_timers()
    {
        MockRequestSink sink;
        snf::worker::WorkerActorConfig actor_config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 2048,
            .max_mailbox_messages_total = 50,
            .max_mailbox_bytes_total = 10240,
            .max_application_timer_bytes_total = 4096,
        };

        snf::adapter::GameActorFactory factory;
        snf::worker::Worker worker(
            snf::worker::WorkerId{0}, 1, snf::worker::WorkerBudgets::defaults(), snf::worker::WorkerInboxConfig{}, actor_config, factory
        );
        factory.setTimerAdmission(worker);

        const snf::worker::ActorKey key{.kind = snf::worker::ActorKind::Zone, .entity = 1};

        auto enter_envelope = snf::adapter::GameActorPayloadRegistry::create(snf::adapter::ZoneCommandMessage{
            .connection = std::nullopt,
            .request_id = 1,
            .command =
                snf::server::EnterZoneCommand{
                    .player = snf::server::PlayerId{101},
                    .route_epoch = 1,
                    .position = snf::server::ZonePosition{.x = 10, .y = 20},
                },
        });

        assert(worker.tryDeliverLocal(key, std::move(enter_envelope)) == snf::worker::DeliveryResult::Accepted);

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        std::this_thread::sleep_for(20ms);
        worker.requestStop();
        th.join();

        // Shutdown cleanly completed
        assert(worker.metrics().actor.application_timers_scheduled >= 1);
        assert(worker.metrics().actor.cancelled_application_timers >= 1);
    }

    [[nodiscard]] snf::protocol::Frame authenticateFrame(const std::uint32_t request_id, const std::uint64_t player)
    {
        std::vector<std::byte> payload(8);
        for (std::size_t index = 0; index < payload.size(); ++index)
        {
            payload[index] = static_cast<std::byte>((player >> (8 * (7 - index))) & 0xFFULL);
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Authenticate,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    [[nodiscard]] snf::protocol::Frame authenticatedFrame(const std::uint32_t request_id, const std::uint64_t player)
    {
        std::vector<std::byte> payload(8);
        for (std::size_t index = 0; index < payload.size(); ++index)
        {
            payload[index] = static_cast<std::byte>((player >> (8 * (7 - index))) & 0xFFULL);
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Authenticated,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    void test_ping_request_sink_vertical_slice()
    {
        snf::worker::WorkerActorConfig actor_config{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 2048,
            .max_mailbox_messages_total = 50,
            .max_mailbox_bytes_total = 10240,
            .max_application_timer_bytes_total = 4096,
        };

        snf::adapter::GameActorFactory factory;
        snf::adapter::GameRequestSink request_sink;

        snf::worker::WorkerNetworkConfig network_config{};
        network_config.table.capacity = 8;
        network_config.poll_registration_capacity = 9;
        network_config.max_accepts_per_poll = 8;
        network_config.receive_chunk_bytes = 1024;

        snf::worker::Worker worker(
            snf::worker::WorkerId{0},
            1,
            snf::worker::WorkerBudgets::defaults(),
            snf::worker::WorkerInboxConfig{},
            network_config,
            request_sink,
            actor_config,
            factory
        );
        factory.setTimerAdmission(worker);
        request_sink.setWorker(worker);

        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));

        const snf::protocol::Frame ping_frame{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 1234,
            .payload = {std::byte{0xDE}, std::byte{0xAD}},
        };

        std::thread th(
            [&]()
            {
                worker.run();
            }
        );
        auto client = connectClient(port);

        // Before authentication the sink answers Ping itself. That it allocates no
        // actor is asserted in worker_session_test, where the Worker can be
        // stopped before WorkerMetrics is read.
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(ping_frame));

        const snf::protocol::Frame expected_pong{
            .type = snf::protocol::MessageType::Pong,
            .request_id = ping_frame.request_id,
            .payload = ping_frame.payload,
        };
        const auto encoded_pong = receiveExact(client.getDescriptor(), snf::protocol::encode_frame(expected_pong).size());
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(encoded_pong);
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        assert(decoded.frames.front() == expected_pong);

        // After authentication the same frame reaches the PlayerActor, so the
        // domain sees the command and the vertical slice covers socket, actor,
        // effect and socket write.
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(4242, 7)));
        const auto encoded_authenticated = receiveExact(client.getDescriptor(), snf::protocol::encode_frame(authenticatedFrame(4242, 7)).size());
        snf::protocol::FrameDecoder authenticated_decoder;
        const auto authenticated = authenticated_decoder.append(encoded_authenticated);
        assert(authenticated.ok());
        assert(authenticated.frames.size() == 1);
        assert(authenticated.frames.front() == authenticatedFrame(4242, 7));

        const snf::protocol::Frame second_ping{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 1235,
            .payload = {std::byte{0xBE}, std::byte{0xEF}},
        };
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(second_ping));
        const snf::protocol::Frame expected_second_pong{
            .type = snf::protocol::MessageType::Pong,
            .request_id = second_ping.request_id,
            .payload = second_ping.payload,
        };
        const auto encoded_second = receiveExact(client.getDescriptor(), snf::protocol::encode_frame(expected_second_pong).size());
        snf::protocol::FrameDecoder second_decoder;
        const auto second = second_decoder.append(encoded_second);
        assert(second.ok());
        assert(second.frames.size() == 1);
        assert(second.frames.front() == expected_second_pong);

        worker.requestStop();
        th.join();

        assert(worker.metrics().network.received_frames == 3);
        assert(worker.metrics().network.sent_frames == 3);
        assert(worker.metrics().actor.actor_turns >= 2);
        assert(worker.metrics().actor.effect_send_failures == 0);
        assert(request_sink.sessionCount() == 0);
    }
}

void run_worker_session_tests();

int main()
{
    std::cout << "Running Stage 7 Adapter and Effect Tests..." << std::endl;

    test_actor_envelope_and_registry_contracts();
    std::cout << "  - test_actor_envelope_and_registry_contracts PASSED" << std::endl;

    test_timer_reservation_raii_lifecycle();
    std::cout << "  - test_timer_reservation_raii_lifecycle PASSED" << std::endl;

    test_player_adapter_and_to_effects();
    std::cout << "  - test_player_adapter_and_to_effects PASSED" << std::endl;

    test_zone_adapter_and_to_effects();
    std::cout << "  - test_zone_adapter_and_to_effects PASSED" << std::endl;

    test_zone_adapter_outcome_reply_to_player();
    std::cout << "  - test_zone_adapter_outcome_reply_to_player PASSED" << std::endl;

    test_player_persists_zone_location_on_save();
    std::cout << "  - test_player_persists_zone_location_on_save PASSED" << std::endl;

    test_zone_tell_timeout_rolls_back_unconfirmed_route();
    std::cout << "  - test_zone_tell_timeout_rolls_back_unconfirmed_route PASSED" << std::endl;

    test_stale_correlation_and_epoch_ignored();
    std::cout << "  - test_stale_correlation_and_epoch_ignored PASSED" << std::endl;

    test_zone_timeout_cleans_up_zone_participant();
    std::cout << "  - test_zone_timeout_cleans_up_zone_participant PASSED" << std::endl;

    test_cross_zone_handoff_hides_route_and_completes_with_new_epoch();
    std::cout << "  - test_cross_zone_handoff_hides_route_and_completes_with_new_epoch PASSED" << std::endl;

    test_cross_zone_definite_target_failure_restores_source_with_newer_epoch();
    std::cout << "  - test_cross_zone_definite_target_failure_restores_source_with_newer_epoch PASSED" << std::endl;

    test_cross_zone_stale_target_cleans_observed_epoch_and_closes();
    std::cout << "  - test_cross_zone_stale_target_cleans_observed_epoch_and_closes PASSED" << std::endl;

    test_cross_zone_stale_source_leave_cleans_observed_epoch_and_closes();
    std::cout << "  - test_cross_zone_stale_source_leave_cleans_observed_epoch_and_closes PASSED" << std::endl;

    test_cross_zone_stale_source_restore_cleans_observed_epoch_and_closes();
    std::cout << "  - test_cross_zone_stale_source_restore_cleans_observed_epoch_and_closes PASSED" << std::endl;

    test_cross_zone_target_timeout_cleans_both_zones_and_closes();
    std::cout << "  - test_cross_zone_target_timeout_cleans_both_zones_and_closes PASSED" << std::endl;

    test_cross_zone_admission_failure_keeps_source_route();
    std::cout << "  - test_cross_zone_admission_failure_keeps_source_route PASSED" << std::endl;

    test_cross_zone_stage_timer_admission_failures_close_known_none();
    std::cout << "  - test_cross_zone_stage_timer_admission_failures_close_known_none PASSED" << std::endl;

    test_room_join_and_return_saga_round_trip();
    std::cout << "  - test_room_join_and_return_saga_round_trip PASSED" << std::endl;

    test_room_join_refusal_keeps_zone_route();
    std::cout << "  - test_room_join_refusal_keeps_zone_route PASSED" << std::endl;

    test_room_join_zone_leave_failure_compensates_and_keeps_zone_route();
    std::cout << "  - test_room_join_zone_leave_failure_compensates_and_keeps_zone_route PASSED" << std::endl;

    test_battle_start_only_allowed_when_in_room();
    std::cout << "  - test_battle_start_only_allowed_when_in_room PASSED" << std::endl;

    test_room_join_step1_timeout_compensates_applied_room_join();
    std::cout << "  - test_room_join_step1_timeout_compensates_applied_room_join PASSED" << std::endl;

    test_room_join_step2_leave_zone_timeout_compensates();
    std::cout << "  - test_room_join_step2_leave_zone_timeout_compensates PASSED" << std::endl;

    test_zone_leave_timeout_sends_terminal_outcome();
    std::cout << "  - test_zone_leave_timeout_sends_terminal_outcome PASSED" << std::endl;

    test_pipelined_zone_moves_handled_individually();
    std::cout << "  - test_pipelined_zone_moves_handled_individually PASSED" << std::endl;

    test_room_return_timer_admission_failure_safely_closes();
    std::cout << "  - test_room_return_timer_admission_failure_safely_closes PASSED" << std::endl;

    test_room_return_timeout_cleans_up_zone_participant();
    std::cout << "  - test_room_return_timeout_cleans_up_zone_participant PASSED" << std::endl;

    test_room_terminal_outcome_initiates_return_saga();
    std::cout << "  - test_room_terminal_outcome_initiates_return_saga PASSED" << std::endl;

    test_room_admission_pre_check_failure();
    std::cout << "  - test_room_admission_pre_check_failure PASSED" << std::endl;

    test_room_adapter_critical_deadline_pre_admission_success();
    std::cout << "  - test_room_adapter_critical_deadline_pre_admission_success PASSED" << std::endl;

    test_room_adapter_critical_deadline_pre_admission_failure();
    std::cout << "  - test_room_adapter_critical_deadline_pre_admission_failure PASSED" << std::endl;

    test_room_routes_remain_bounded_and_follow_result_audience();
    std::cout << "  - test_room_routes_remain_bounded_and_follow_result_audience PASSED" << std::endl;

    test_timer_queue_application_accounting();
    std::cout << "  - test_timer_queue_application_accounting PASSED" << std::endl;

    test_effect_batch_validation_and_mutual_exclusion();
    std::cout << "  - test_effect_batch_validation_and_mutual_exclusion PASSED" << std::endl;

    test_worker_application_timer_schedule_and_delivery();
    std::cout << "  - test_worker_application_timer_schedule_and_delivery PASSED" << std::endl;

    test_shutdown_cancels_application_timers();
    std::cout << "  - test_shutdown_cancels_application_timers PASSED" << std::endl;

    test_ping_request_sink_vertical_slice();
    std::cout << "  - test_ping_request_sink_vertical_slice PASSED" << std::endl;

    run_worker_session_tests();
    std::cout << "  - run_worker_session_tests PASSED" << std::endl;

    std::cout << "All Stage 7 tests passed successfully!" << std::endl;
    return 0;
}
