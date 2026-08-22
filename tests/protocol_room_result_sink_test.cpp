#include "outbound_reservation_test_support.hpp"
#include "snf/game/skill_catalog.hpp"
#include "snf/server/outbound_channel.hpp"
#include "snf/server/protocol_room_result_sink.hpp"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace
{
    using snf::server::BattleDigest;
    using snf::server::BattleOutcome;
    using snf::server::EnemyDamaged;
    using snf::server::EnemyDied;
    using snf::server::EnemyId;
    using snf::server::EnemyKind;
    using snf::server::EnemySpawned;
    using snf::server::PlayerId;
    using snf::server::RoomCommandStatus;
    using snf::server::RoomId;
    using snf::server::RoomInboundCommand;
    using snf::server::RoomPhase;
    using snf::server::RoomResult;
    using snf::server::SkillWhiffed;

    struct SinkFixture
    {
        explicit SinkFixture(const std::size_t capacity)
            : wake(snf::test::make_wake_descriptor())
            , outbound(snf::server::OutboundChannelConfig{.capacity = capacity, .max_slots_per_connection = capacity}, wake.getDescriptor())
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
        assert(payload_u16(*frame, 9) == 4);

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
        assert(frame->payload.size() == 76);
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
                .audience = {player},
            }
        );

        const auto failed = fixture.pop();
        assert(failed && failed->type == snf::protocol::MessageType::BattleFailed);
        assert(failed->payload.size() == 9);
        assert(payload_u64(*failed, 0) == 0);
        assert(failed->payload[8] == std::byte{0});
        assert(!fixture.pop());
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
    test_a_digest_encodes_every_tag_and_body_in_event_order();
    test_ack_precedes_the_same_digest_for_caster_and_observer();
    test_terminal_frames_follow_the_terminal_digest();
    test_a_clear_follows_the_digest_and_pays_each_grant();
    test_failure_before_boss_spawn_carries_zero_health_and_false_gate();
    test_an_oversized_digest_closes_its_audience_instead_of_encoding();
}
