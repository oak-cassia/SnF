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

        worker.requestStop();
        th.join();

        // The request crossed the actual socket, actor, effect, and socket-write path.
        assert(worker.metrics().network.received_frames == 1);
        assert(worker.metrics().network.sent_frames == 1);
        assert(worker.metrics().actor.actor_turns >= 1);
        assert(worker.metrics().actor.effect_send_failures == 0);
    }
}

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

    std::cout << "All Stage 7 tests passed successfully!" << std::endl;
    return 0;
}
