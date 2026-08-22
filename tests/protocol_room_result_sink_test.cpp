#include "outbound_reservation_test_support.hpp"
#include "snf/game/skill_catalog.hpp"
#include "snf/server/outbound_channel.hpp"
#include "snf/server/protocol_room_result_sink.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace
{
    using snf::server::PlayerId;
    using snf::server::RoomCommandStatus;
    using snf::server::RoomId;
    using snf::server::RoomInboundCommand;
    using snf::server::RoomPhase;
    using snf::server::RoomResult;
    using snf::server::StreetExperienceGrant;

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

    [[nodiscard]] RoomInboundCommand join_command(const snf::net::ConnectionId connection, const PlayerId player, const std::uint32_t request_id)
    {
        return RoomInboundCommand{
            .room = RoomId{.value = 7},
            .command = snf::server::JoinRoom{.player = player},
            .reply =
                snf::server::RoomReplyContext{
                    .connection = connection,
                    .request_id = request_id,
                    .kind = snf::server::RoomReplyKind::Joined,
                },
        };
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
                    .kind = snf::server::RoomReplyKind::SkillApplied,
                },
        };
    }

    void test_a_join_is_answered_on_the_asking_connection()
    {
        SinkFixture fixture{4};
        const PlayerId player{.value = 10};
        const auto connection = fixture.attach(4, player);

        fixture.sink.accept(
            join_command(connection, player, 8),
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Waiting,
                .player = player,
            }
        );

        const auto frame = fixture.pop();
        assert(frame);
        assert(frame->type == snf::protocol::MessageType::RoomJoined);
        assert(frame->request_id == 8);
        assert(frame->payload[0] == static_cast<std::byte>(RoomCommandStatus::Applied));
        assert(frame->payload[1] == static_cast<std::byte>(RoomPhase::Waiting));
        assert(!fixture.pop());
    }

    void test_a_command_with_no_reply_context_answers_nobody()
    {
        SinkFixture fixture{4};
        // BattleDeadline arrives from the Room's own timer, so there is no request.
        fixture.sink.accept(
            RoomInboundCommand{
                .room = RoomId{.value = 7},
                .command = snf::server::BattleDeadline{},
                .reply = std::nullopt,
            },
            RoomResult{
                .status = RoomCommandStatus::WrongPhase,
                .phase = RoomPhase::Cleared,
            }
        );

        assert(!fixture.pop());
    }

    void test_a_clear_notifies_every_online_participant_unsolicited()
    {
        SinkFixture fixture{4};
        const PlayerId first{.value = 10};
        const PlayerId second{.value = 20};
        static_cast<void>(fixture.attach(4, first));
        static_cast<void>(fixture.attach(5, second));

        fixture.sink.accept(
            RoomInboundCommand{
                .room = RoomId{.value = 7},
                .command = snf::server::BattleDeadline{},
                .reply = std::nullopt,
            },
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Cleared,
                .grants =
                    {
                        StreetExperienceGrant{.player = first, .experience = 300},
                        StreetExperienceGrant{.player = second, .experience = 300},
                    },
            }
        );

        for (int index = 0; index < 2; ++index)
        {
            const auto frame = fixture.pop();
            assert(frame);
            assert(frame->type == snf::protocol::MessageType::BattleCleared);
            // Nothing asked for this frame, so there is no request to correlate it with.
            assert(frame->request_id == snf::protocol::UNSOLICITED_REQUEST_ID);
        }
        assert(!fixture.pop());
    }

    void test_an_offline_participant_is_skipped_and_the_others_still_hear()
    {
        SinkFixture fixture{4};
        const PlayerId online{.value = 10};
        const PlayerId offline{.value = 99};
        static_cast<void>(fixture.attach(4, online));

        fixture.sink.accept(
            RoomInboundCommand{
                .room = RoomId{.value = 7},
                .command = snf::server::BattleDeadline{},
                .reply = std::nullopt,
            },
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Cleared,
                .grants =
                    {
                        StreetExperienceGrant{.player = offline, .experience = 300},
                        StreetExperienceGrant{.player = online, .experience = 300},
                    },
            }
        );

        // The reward itself still reached the offline player: the tell that carries it
        // applies against the stored record. Only the notification has nowhere to go.
        const auto frame = fixture.pop();
        assert(frame);
        assert(frame->type == snf::protocol::MessageType::BattleCleared);
        assert(!fixture.pop());
    }

    void test_a_full_outbound_channel_drops_the_notification()
    {
        SinkFixture fixture{1};
        const PlayerId first{.value = 10};
        const PlayerId second{.value = 20};
        static_cast<void>(fixture.attach(4, first));
        static_cast<void>(fixture.attach(5, second));

        fixture.sink.accept(
            RoomInboundCommand{
                .room = RoomId{.value = 7},
                .command = snf::server::BattleDeadline{},
                .reply = std::nullopt,
            },
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Cleared,
                .grants =
                    {
                        StreetExperienceGrant{.player = first, .experience = 300},
                        StreetExperienceGrant{.player = second, .experience = 300},
                    },
            }
        );

        // One slot for two notifications. The second is dropped rather than blocking
        // the Worker, and the reward it announced is already durable.
        assert(fixture.pop());
        assert(!fixture.pop());
    }

    void test_a_cast_answers_the_caster_and_tells_the_others_once()
    {
        SinkFixture fixture{4};
        const PlayerId caster{.value = 10};
        const PlayerId observer{.value = 20};
        const auto casting_connection = fixture.attach(4, caster);
        static_cast<void>(fixture.attach(5, observer));

        fixture.sink.accept(
            cast_command(casting_connection, caster, 8),
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Running,
                .player = caster,
                .boss_health = 970,
                .skill =
                    snf::server::SkillOutcome{
                        .actor = caster,
                        .skill = snf::server::SLASH,
                        .damage = 30,
                    },
                .audience = {caster, observer},
            }
        );

        const auto answer = fixture.pop();
        assert(answer);
        assert(answer->type == snf::protocol::MessageType::SkillApplied);
        assert(answer->request_id == 8);
        assert(answer->payload[0] == static_cast<std::byte>(RoomCommandStatus::Applied));
        assert(answer->payload[1] == static_cast<std::byte>(RoomPhase::Running));
        assert(payload_u64(*answer, 2) == caster.value);
        assert(payload_u32(*answer, 10) == snf::server::SLASH.value);
        assert(payload_u64(*answer, 14) == 30);
        assert(payload_u64(*answer, 22) == 970);

        // The observer reads the same layout, on no request of their own.
        const auto broadcast = fixture.pop();
        assert(broadcast);
        assert(broadcast->type == snf::protocol::MessageType::SkillApplied);
        assert(broadcast->request_id == snf::protocol::UNSOLICITED_REQUEST_ID);
        assert(broadcast->payload == answer->payload);

        // And the caster is told once: the reply is their copy.
        assert(!fixture.pop());
    }

    void test_a_refused_cast_answers_only_the_caster()
    {
        SinkFixture fixture{4};
        const PlayerId caster{.value = 10};
        const PlayerId observer{.value = 20};
        const auto casting_connection = fixture.attach(4, caster);
        static_cast<void>(fixture.attach(5, observer));

        // A resend changed nothing, so the Room named no audience for it.
        fixture.sink.accept(
            cast_command(casting_connection, caster, 9),
            RoomResult{
                .status = RoomCommandStatus::DuplicateRequest,
                .phase = RoomPhase::Running,
                .player = caster,
                .boss_health = 970,
            }
        );

        const auto answer = fixture.pop();
        assert(answer);
        assert(answer->type == snf::protocol::MessageType::SkillApplied);
        assert(answer->payload[0] == static_cast<std::byte>(RoomCommandStatus::DuplicateRequest));
        // It still names who asked, and reports that nothing landed.
        assert(payload_u64(*answer, 2) == caster.value);
        assert(payload_u32(*answer, 10) == 0);
        assert(payload_u64(*answer, 14) == 0);
        assert(payload_u64(*answer, 22) == 970);
        assert(!fixture.pop());
    }

    void test_the_killing_blow_reports_the_cast_and_the_clear()
    {
        SinkFixture fixture{8};
        const PlayerId caster{.value = 10};
        const PlayerId observer{.value = 20};
        const auto casting_connection = fixture.attach(4, caster);
        static_cast<void>(fixture.attach(5, observer));

        fixture.sink.accept(
            cast_command(casting_connection, caster, 8),
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Cleared,
                .player = caster,
                .boss_health = 0,
                .skill =
                    snf::server::SkillOutcome{
                        .actor = caster,
                        .skill = snf::server::SLASH,
                        .damage = 20,
                    },
                .audience = {caster, observer},
                .grants =
                    {
                        StreetExperienceGrant{.player = caster, .experience = 300},
                        StreetExperienceGrant{.player = observer, .experience = 300},
                    },
            }
        );

        // The cast that killed the boss, then the clear it caused. Both leave in the
        // same accept, so no client sees a boss at zero health in a running battle.
        const auto answer = fixture.pop();
        assert(answer);
        assert(answer->type == snf::protocol::MessageType::SkillApplied);
        assert(answer->payload[1] == static_cast<std::byte>(RoomPhase::Cleared));
        const auto broadcast = fixture.pop();
        assert(broadcast);
        assert(broadcast->type == snf::protocol::MessageType::SkillApplied);
        for (int index = 0; index < 2; ++index)
        {
            const auto cleared = fixture.pop();
            assert(cleared);
            assert(cleared->type == snf::protocol::MessageType::BattleCleared);
        }
        assert(!fixture.pop());
    }

    void test_a_failure_tells_every_participant()
    {
        SinkFixture fixture{4};
        const PlayerId first{.value = 10};
        const PlayerId second{.value = 20};
        static_cast<void>(fixture.attach(4, first));
        static_cast<void>(fixture.attach(5, second));

        fixture.sink.accept(
            RoomInboundCommand{
                .room = RoomId{.value = 7},
                .command = snf::server::BattleDeadline{},
                .reply = std::nullopt,
            },
            RoomResult{
                .status = RoomCommandStatus::Applied,
                .phase = RoomPhase::Failed,
                .boss_health = 40,
                .audience = {first, second},
            }
        );

        // A failure pays nobody, so the grants a clear would be read from are empty.
        // The audience is what carries it, and everybody in the battle hears it.
        for (int index = 0; index < 2; ++index)
        {
            const auto frame = fixture.pop();
            assert(frame);
            assert(frame->type == snf::protocol::MessageType::BattleFailed);
            assert(frame->request_id == snf::protocol::UNSOLICITED_REQUEST_ID);
            assert(payload_u64(*frame, 0) == 40);
        }
        assert(!fixture.pop());
    }
}

void run_protocol_room_result_sink_tests()
{
    test_a_join_is_answered_on_the_asking_connection();
    test_a_command_with_no_reply_context_answers_nobody();
    test_a_clear_notifies_every_online_participant_unsolicited();
    test_an_offline_participant_is_skipped_and_the_others_still_hear();
    test_a_full_outbound_channel_drops_the_notification();
    test_a_cast_answers_the_caster_and_tells_the_others_once();
    test_a_refused_cast_answers_only_the_caster();
    test_the_killing_blow_reports_the_cast_and_the_clear();
    test_a_failure_tells_every_participant();
}
