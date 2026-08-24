#include "outbound_reservation_test_support.hpp"
#include "snf/game/skill_id.hpp"
#include "snf/server/outbound_channel.hpp"
#include "snf/server/protocol_room_result_sink.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <optional>
#include <variant>
#include <vector>

import snf.game.skill_catalog;

namespace
{
    using snf::server::ArenaStarted;
    using snf::server::BattleDigest;
    using snf::server::BattleFailureReason;
    using snf::server::BattleOutcome;
    using snf::server::EnemyDamaged;
    using snf::server::EnemyDied;
    using snf::server::EnemyId;
    using snf::server::EnemyKind;
    using snf::server::EnemyPositioned;
    using snf::server::EnemySpawned;
    using snf::server::ParticipantDamaged;
    using snf::server::ParticipantDied;
    using snf::server::ParticipantLeft;
    using snf::server::ParticipantMoved;
    using snf::server::ParticipantSpawned;
    using snf::server::PlayerId;
    using snf::server::RoomCommandStatus;
    using snf::server::RoomId;
    using snf::server::RoomInboundCommand;
    using snf::server::RoomPhase;
    using snf::server::RoomResult;
    using snf::server::SkillWhiffed;

    struct SinkFixture
    {
        explicit SinkFixture(const std::size_t capacity, const std::size_t max_slots_per_connection = 0)
            : wake(snf::test::make_wake_descriptor())
            , outbound(
                  snf::server::OutboundChannelConfig{
                      .capacity = capacity,
                      .max_slots_per_connection = max_slots_per_connection == 0 ? capacity : max_slots_per_connection,
                  },
                  wake.getDescriptor()
              )
            , sink(outbound, sessions)
        {
        }

        [[nodiscard]] snf::net::ConnectionId attach(const int descriptor, const PlayerId player)
        {
            const snf::net::ConnectionId connection{.descriptor = descriptor, .generation = 1};
            assert(sessions.tryAttach(connection, player) == snf::server::PlayerAttachResult::Attached);
            return connection;
        }

        [[nodiscard]] std::optional<snf::protocol::Frame> pop()
        {
            const auto posted = outbound.tryPop();
            if (!posted)
            {
                return std::nullopt;
            }
            const auto* send = std::get_if<snf::server::SendFrame>(&posted->action);
            assert(send != nullptr);
            return send->frame;
        }

        snf::net::UniqueFileDescriptor wake;
        snf::server::OutboundChannel outbound;
        snf::server::PlayerSessionDirectory sessions;
        snf::server::ProtocolRoomResultSink sink;
    };

    [[nodiscard]] RoomInboundCommand cast_command(const snf::net::ConnectionId connection, const PlayerId player, const std::uint32_t request_id)
    {
        return RoomInboundCommand{
            .room = RoomId{.value = 7},
            .command =
                snf::server::UseSkill{
                    .player = player,
                    .skill = snf::server::SLASH,
                    .request_sequence = 1,
                },
            .reply =
                snf::server::RoomReplyContext{
                    .connection = connection,
                    .request_id = request_id,
                    .kind = snf::server::RoomReplyKind::SkillAcknowledged,
                },
        };
    }

    [[nodiscard]] RoomInboundCommand move_command(const snf::net::ConnectionId connection, const PlayerId player, const std::uint32_t request_id)
    {
        return RoomInboundCommand{
            .room = RoomId{.value = 7},
            .command =
                snf::server::SetMoveIntent{
                    .player = player,
                    .direction = snf::server::MoveDirection::Stop,
                    .request_sequence = 1,
                },
            .reply =
                snf::server::RoomReplyContext{
                    .connection = connection,
                    .request_id = request_id,
                    .kind = snf::server::RoomReplyKind::MoveAcknowledged,
                },
        };
    }

    [[nodiscard]] std::uint16_t payload_u16(const snf::protocol::Frame& frame, const std::size_t offset)
    {
        return (std::to_integer<std::uint16_t>(frame.payload[offset]) << 8U) | std::to_integer<std::uint16_t>(frame.payload[offset + 1]);
    }

    [[nodiscard]] std::uint32_t payload_u32(const snf::protocol::Frame& frame, const std::size_t offset)
    {
        std::uint32_t value = 0;
        for (std::size_t index = 0; index < 4; ++index)
        {
            value = (value << 8U) | std::to_integer<std::uint32_t>(frame.payload[offset + index]);
        }
        return value;
    }

    [[nodiscard]] std::uint64_t payload_u64(const snf::protocol::Frame& frame, const std::size_t offset)
    {
        return (static_cast<std::uint64_t>(payload_u32(frame, offset)) << 32U) | payload_u32(frame, offset + 4);
    }

    [[nodiscard]] BattleDigest every_event_digest()
    {
        return BattleDigest{
            .sequence = 9,
            .events =
                {
                    EnemySpawned{.id = EnemyId{.value = 1}, .kind = EnemyKind::Minion, .health = 30},
                    EnemyDamaged{
                        .target = EnemyId{.value = 1},
                        .actor = PlayerId{.value = 10},
                        .skill = snf::server::SLASH,
                        .amount = 10,
                        .health = 20,
                    },
                    EnemyDied{.id = EnemyId{.value = 1}},
                    SkillWhiffed{.actor = PlayerId{.value = 20}, .skill = snf::server::SLASH},
                    ArenaStarted{.width = 100, .height = 100},
                    ParticipantSpawned{.player = PlayerId{.value = 10}, .position = {.x = 50, .y = 50}, .health = 100},
                    ParticipantMoved{.player = PlayerId{.value = 10}, .position = {.x = 54, .y = 46}},
                    EnemyPositioned{.enemy = EnemyId{.value = 1}, .position = {.x = 50, .y = 25}},
                    ParticipantDamaged{.target = PlayerId{.value = 10}, .attacker = EnemyId{.value = 1}, .amount = 3, .health = 97},
                    ParticipantDied{.player = PlayerId{.value = 20}},
                    ParticipantLeft{.player = PlayerId{.value = 30}},
                },
        };
    }

    void test_a_skill_acknowledgement_is_the_two_byte_request_reply()
    {
        SinkFixture fixture{4};
        const PlayerId caster{.value = 10};
        const auto connection = fixture.attach(4, caster);

        fixture.sink.accept(
            cast_command(connection, caster, 77),
            RoomResult{
                .status = RoomCommandStatus::SkillOnCooldown,
                .phase = RoomPhase::Running,
                .player = caster,
            }
        );

        const auto frame = fixture.pop();
        assert(frame && frame->type == snf::protocol::MessageType::SkillAcknowledged);
        assert(frame->request_id == 77);
        assert(
            (frame->payload ==
             std::vector<std::byte>{
                 std::byte{static_cast<std::uint8_t>(RoomCommandStatus::SkillOnCooldown)},
                 std::byte{static_cast<std::uint8_t>(RoomPhase::Running)},
             })
        );
        assert(!fixture.pop());
    }

    void test_a_move_acknowledgement_uses_its_own_two_byte_reply_type()
    {
        SinkFixture fixture{2};
        const PlayerId player{.value = 10};
        const auto connection = fixture.attach(4, player);

        fixture.sink.accept(
            move_command(connection, player, 78), RoomResult{.status = RoomCommandStatus::Applied, .phase = RoomPhase::Running, .player = player}
        );

        const auto frame = fixture.pop();
        assert(frame && frame->type == snf::protocol::MessageType::MoveAcknowledged && frame->request_id == 78);
        assert(frame->payload == (std::vector<std::byte>{std::byte{0}, std::byte{1}}));
        assert(!fixture.pop());
    }

    void test_runtime_overload_is_a_battle_start_reply_that_leaves_the_room_waiting()
    {
        SinkFixture fixture{1};
        const PlayerId player{.value = 10};
        const auto connection = fixture.attach(4, player);

        fixture.sink.accept(
            RoomInboundCommand{
                .room = RoomId{.value = 7},
                .command = snf::server::StartBattle{},
                .reply =
                    snf::server::RoomReplyContext{
                        .connection = connection,
                        .request_id = 79,
                        .kind = snf::server::RoomReplyKind::BattleStarted,
                    },
            },
            RoomResult{
                .status = RoomCommandStatus::RuntimeOverloaded,
                .phase = RoomPhase::Waiting,
            }
        );

        const auto frame = fixture.pop();
        assert(frame && frame->type == snf::protocol::MessageType::BattleStarted && frame->request_id == 79);
        assert(frame->payload.size() == 10);
        assert(frame->payload[0] == static_cast<std::byte>(RoomCommandStatus::RuntimeOverloaded));
        assert(frame->payload[1] == static_cast<std::byte>(RoomPhase::Waiting));
        assert(payload_u64(*frame, 2) == 7);
        assert(!fixture.pop());
    }

    void test_a_digest_encodes_every_tag_and_body_in_event_order()
    {
        SinkFixture fixture{4};
        const PlayerId player{.value = 10};
        static_cast<void>(fixture.attach(4, player));

        fixture.sink.accept(
            RoomInboundCommand{.room = RoomId{.value = 7}, .command = snf::server::RoomSimulationTick{}, .reply = std::nullopt},
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Running,
                .digest = every_event_digest(),
                .audience = {player},
            }
        );

        const auto frame = fixture.pop();
        assert(frame && frame->type == snf::protocol::MessageType::BattleDigest);
        assert(frame->request_id == snf::protocol::UNSOLICITED_REQUEST_ID);
        assert(payload_u64(*frame, 0) == 9);
        assert(frame->payload[8] == std::byte{static_cast<std::uint8_t>(RoomPhase::Running)});
        assert(payload_u16(*frame, 9) == 11);

        assert(frame->payload[11] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::EnemySpawned)});
        assert(payload_u32(*frame, 12) == 1);
        assert(frame->payload[16] == std::byte{static_cast<std::uint8_t>(EnemyKind::Minion)});
        assert(payload_u64(*frame, 17) == 30);

        assert(frame->payload[25] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::EnemyDamaged)});
        assert(payload_u32(*frame, 26) == 1);
        assert(payload_u64(*frame, 30) == 10);
        assert(payload_u32(*frame, 38) == snf::server::SLASH.value);
        assert(payload_u64(*frame, 42) == 10);
        assert(payload_u64(*frame, 50) == 20);

        assert(frame->payload[58] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::EnemyDied)});
        assert(payload_u32(*frame, 59) == 1);
        assert(frame->payload[63] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::SkillWhiffed)});
        assert(payload_u64(*frame, 64) == 20);
        assert(payload_u32(*frame, 72) == snf::server::SLASH.value);
        assert(frame->payload[76] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::ArenaStarted)});
        assert(payload_u32(*frame, 77) == 100);
        assert(payload_u32(*frame, 81) == 100);

        assert(frame->payload[85] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::ParticipantSpawned)});
        assert(payload_u64(*frame, 86) == 10);
        assert(payload_u32(*frame, 94) == 50);
        assert(payload_u32(*frame, 98) == 50);
        assert(payload_u64(*frame, 102) == 100);

        assert(frame->payload[110] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::ParticipantMoved)});
        assert(payload_u64(*frame, 111) == 10);
        assert(payload_u32(*frame, 119) == 54);
        assert(payload_u32(*frame, 123) == 46);

        assert(frame->payload[127] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::EnemyPositioned)});
        assert(payload_u32(*frame, 128) == 1);
        assert(payload_u32(*frame, 132) == 50);
        assert(payload_u32(*frame, 136) == 25);

        assert(frame->payload[140] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::ParticipantDamaged)});
        assert(payload_u64(*frame, 141) == 10);
        assert(payload_u32(*frame, 149) == 1);
        assert(payload_u64(*frame, 153) == 3);
        assert(payload_u64(*frame, 161) == 97);

        assert(frame->payload[169] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::ParticipantDied)});
        assert(payload_u64(*frame, 170) == 20);
        assert(frame->payload[178] == std::byte{static_cast<std::uint8_t>(snf::server::BattleEventKind::ParticipantLeft)});
        assert(payload_u64(*frame, 179) == 30);
        assert(frame->payload.size() == 187);
        const auto stats = fixture.sink.stats();
        assert(stats.battle_digest_frames == 1);
        assert(stats.battle_digest_fanout_bytes == 197);
    }

    void test_ack_precedes_the_same_digest_for_caster_and_observer()
    {
        SinkFixture fixture{8};
        const PlayerId caster{.value = 10};
        const PlayerId observer{.value = 20};
        const auto connection = fixture.attach(4, caster);
        static_cast<void>(fixture.attach(5, observer));

        fixture.sink.accept(
            cast_command(connection, caster, 8),
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Running,
                .player = caster,
                .digest = every_event_digest(),
                .audience = {caster, observer},
            }
        );

        const auto ack = fixture.pop();
        const auto caster_digest = fixture.pop();
        const auto observer_digest = fixture.pop();
        assert(ack && ack->type == snf::protocol::MessageType::SkillAcknowledged && ack->request_id == 8);
        assert(caster_digest && caster_digest->type == snf::protocol::MessageType::BattleDigest);
        assert(observer_digest && observer_digest->type == snf::protocol::MessageType::BattleDigest);
        assert(caster_digest->payload == observer_digest->payload);
        assert(!fixture.pop());
    }

    void test_terminal_frames_follow_the_terminal_digest()
    {
        SinkFixture fixture{8};
        const PlayerId first{.value = 10};
        const PlayerId second{.value = 20};
        static_cast<void>(fixture.attach(4, first));
        static_cast<void>(fixture.attach(5, second));

        fixture.sink.accept(
            RoomInboundCommand{.room = RoomId{.value = 7}, .command = snf::server::BattleDeadline{}, .reply = std::nullopt},
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Failed,
                .boss_health = 40,
                .boss_spawned = true,
                .digest = BattleDigest{.sequence = 2, .events = {SkillWhiffed{.actor = first, .skill = snf::server::SLASH}}},
                .outcome = BattleOutcome::Failed,
                .failure_reason = BattleFailureReason::Deadline,
                .audience = {first, second},
            }
        );

        const auto first_digest = fixture.pop();
        const auto second_digest = fixture.pop();
        assert(first_digest && first_digest->type == snf::protocol::MessageType::BattleDigest);
        assert(second_digest && second_digest->type == snf::protocol::MessageType::BattleDigest);
        for (int index = 0; index < 2; ++index)
        {
            const auto failed = fixture.pop();
            assert(failed && failed->type == snf::protocol::MessageType::BattleFailed);
            assert(payload_u64(*failed, 0) == 40);
            assert(failed->payload[8] == std::byte{1});
            assert(failed->payload[9] == std::byte{static_cast<std::uint8_t>(BattleFailureReason::Deadline)});
        }
        assert(!fixture.pop());
    }

    void test_a_clear_follows_the_digest_and_pays_each_grant()
    {
        SinkFixture fixture{8};
        const PlayerId caster{.value = 10};
        const PlayerId observer{.value = 20};
        const auto connection = fixture.attach(4, caster);
        static_cast<void>(fixture.attach(5, observer));

        fixture.sink.accept(
            cast_command(connection, caster, 9),
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Cleared,
                .player = caster,
                .boss_spawned = true,
                .digest = BattleDigest{.sequence = 3, .events = {EnemyDied{.id = EnemyId{.value = 5}}}},
                .outcome = BattleOutcome::Cleared,
                .audience = {caster, observer},
                .grants = {{.player = caster, .experience = 300}, {.player = observer, .experience = 300}},
            }
        );

        assert(fixture.pop()->type == snf::protocol::MessageType::SkillAcknowledged);
        assert(fixture.pop()->type == snf::protocol::MessageType::BattleDigest);
        assert(fixture.pop()->type == snf::protocol::MessageType::BattleDigest);
        assert(fixture.pop()->type == snf::protocol::MessageType::BattleCleared);
        assert(fixture.pop()->type == snf::protocol::MessageType::BattleCleared);
        assert(!fixture.pop());
    }

    void test_failure_before_boss_spawn_carries_zero_health_and_false_gate()
    {
        SinkFixture fixture{2};
        const PlayerId player{.value = 10};
        static_cast<void>(fixture.attach(4, player));

        fixture.sink.accept(
            RoomInboundCommand{.room = RoomId{.value = 7}, .command = snf::server::BattleDeadline{}, .reply = std::nullopt},
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Failed,
                .boss_health = 0,
                .boss_spawned = false,
                .outcome = BattleOutcome::Failed,
                .failure_reason = BattleFailureReason::ParticipantsDefeated,
                .audience = {player},
            }
        );

        const auto failed = fixture.pop();
        assert(failed && failed->type == snf::protocol::MessageType::BattleFailed);
        assert(failed->payload.size() == 10);
        assert(payload_u64(*failed, 0) == 0);
        assert(failed->payload[8] == std::byte{0});
        assert(failed->payload[9] == std::byte{static_cast<std::uint8_t>(BattleFailureReason::ParticipantsDefeated)});
        assert(!fixture.pop());
    }

    void test_maximum_movement_fanout_reports_wire_bytes_for_every_client()
    {
        SinkFixture fixture{4};
        const std::vector<PlayerId> audience{
            PlayerId{.value = 10},
            PlayerId{.value = 20},
            PlayerId{.value = 30},
            PlayerId{.value = 40},
        };
        for (std::size_t index = 0; index < audience.size(); ++index)
        {
            static_cast<void>(fixture.attach(static_cast<int>(4 + index), audience[index]));
        }

        std::vector<snf::server::BattleEvent> events;
        events.reserve(4 + 64);
        for (const PlayerId player : audience)
        {
            events.emplace_back(ParticipantMoved{.player = player, .position = {.x = 50, .y = 50}});
        }
        for (std::uint32_t enemy = 1; enemy <= 64; ++enemy)
        {
            events.emplace_back(EnemyPositioned{.enemy = EnemyId{.value = enemy}, .position = {.x = enemy, .y = enemy}});
        }

        fixture.sink.accept(
            RoomInboundCommand{.room = RoomId{.value = 7}, .command = snf::server::RoomSimulationTick{}, .reply = std::nullopt},
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Running,
                .digest = BattleDigest{.sequence = 1, .events = std::move(events)},
                .audience = audience,
            }
        );

        constexpr std::size_t DIGEST_PAYLOAD_BYTES = 11 + 4 * 17 + 64 * 13;
        constexpr std::size_t ENCODED_FRAME_BYTES = 10 + DIGEST_PAYLOAD_BYTES;
        static_assert(DIGEST_PAYLOAD_BYTES == 911);
        static_assert(ENCODED_FRAME_BYTES == 921);
        for (std::size_t index = 0; index < audience.size(); ++index)
        {
            const auto frame = fixture.pop();
            assert(frame && frame->type == snf::protocol::MessageType::BattleDigest);
            assert(frame->payload.size() == DIGEST_PAYLOAD_BYTES);
        }
        assert(!fixture.pop());
        assert(fixture.sink.stats().battle_digest_frames == audience.size());
        assert(fixture.sink.stats().battle_digest_fanout_bytes == audience.size() * ENCODED_FRAME_BYTES);
    }

    void test_a_saturated_client_does_not_block_a_healthy_client_or_terminal_result()
    {
        SinkFixture fixture{3, 1};
        const PlayerId slow{.value = 10};
        const PlayerId healthy{.value = 20};
        const auto slow_connection = fixture.attach(4, slow);
        static_cast<void>(fixture.attach(5, healthy));
        auto held = fixture.outbound.tryReserve(slow_connection, 1);
        assert(held);

        for (std::uint64_t sequence = 1; sequence <= 2; ++sequence)
        {
            fixture.sink.accept(
                RoomInboundCommand{.room = RoomId{.value = 7}, .command = snf::server::RoomSimulationTick{}, .reply = std::nullopt},
                RoomResult{
                    .status = RoomCommandStatus::Applied,
                    .phase = RoomPhase::Running,
                    .digest =
                        BattleDigest{
                            .sequence = sequence,
                            .events = {ParticipantMoved{.player = healthy, .position = {.x = 50, .y = 50}}},
                        },
                    .audience = {slow, healthy},
                }
            );
            const auto digest = fixture.pop();
            assert(digest && digest->type == snf::protocol::MessageType::BattleDigest);
            assert(payload_u64(*digest, 0) == sequence);
            assert(!fixture.pop());
        }

        fixture.sink.accept(
            RoomInboundCommand{.room = RoomId{.value = 7}, .command = snf::server::BattleDeadline{}, .reply = std::nullopt},
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Failed,
                .outcome = BattleOutcome::Failed,
                .failure_reason = BattleFailureReason::Deadline,
                .audience = {slow, healthy},
            }
        );
        const auto failed = fixture.pop();
        assert(failed && failed->type == snf::protocol::MessageType::BattleFailed);
        assert(!fixture.pop());

        std::vector<snf::net::ConnectionId> failures;
        assert(!fixture.outbound.takePendingAdmissionFailures(failures));
        assert(failures == std::vector<snf::net::ConnectionId>{slow_connection});
        assert(fixture.sink.stats().battle_digest_frames == 2);
    }

    void test_an_oversized_digest_closes_its_audience_instead_of_encoding()
    {
        SinkFixture fixture{4};
        const PlayerId first{.value = 10};
        const PlayerId second{.value = 20};
        const auto first_connection = fixture.attach(4, first);
        const auto second_connection = fixture.attach(5, second);
        std::vector<snf::server::BattleEvent> events(
            snf::protocol::MAX_PAYLOAD_SIZE / 33 + 1,
            EnemyDamaged{
                .target = EnemyId{.value = 1},
                .actor = first,
                .skill = snf::server::SLASH,
                .amount = 1,
                .health = 1,
            }
        );

        fixture.sink.accept(
            RoomInboundCommand{.room = RoomId{.value = 7}, .command = snf::server::RoomSimulationTick{}, .reply = std::nullopt},
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Running,
                .digest = BattleDigest{.sequence = 1, .events = std::move(events)},
                .audience = {first, second},
            }
        );

        assert(!fixture.pop());
        assert(fixture.sink.stats().oversized_battle_digests == 1);
        std::vector<snf::net::ConnectionId> failures;
        assert(!fixture.outbound.takePendingAdmissionFailures(failures));
        assert(failures.size() == 2);
        assert(std::ranges::find(failures, first_connection) != failures.end());
        assert(std::ranges::find(failures, second_connection) != failures.end());
    }
}

void run_protocol_room_result_sink_tests()
{
    test_a_skill_acknowledgement_is_the_two_byte_request_reply();
    test_a_move_acknowledgement_uses_its_own_two_byte_reply_type();
    test_runtime_overload_is_a_battle_start_reply_that_leaves_the_room_waiting();
    test_a_digest_encodes_every_tag_and_body_in_event_order();
    test_ack_precedes_the_same_digest_for_caster_and_observer();
    test_terminal_frames_follow_the_terminal_digest();
    test_a_clear_follows_the_digest_and_pays_each_grant();
    test_failure_before_boss_spawn_carries_zero_health_and_false_gate();
    test_maximum_movement_fanout_reports_wire_bytes_for_every_client();
    test_a_saturated_client_does_not_block_a_healthy_client_or_terminal_result();
    test_an_oversized_digest_closes_its_audience_instead_of_encoding();
}
