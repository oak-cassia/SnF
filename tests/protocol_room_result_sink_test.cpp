#include "outbound_reservation_test_support.hpp"
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
        // BattleCompleted arrives from the Room's own timer, so there is no request.
        fixture.sink.accept(
            RoomInboundCommand{
                .room = RoomId{.value = 7},
                .command = snf::server::BattleCompleted{},
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
                .command = snf::server::BattleCompleted{},
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
                .command = snf::server::BattleCompleted{},
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
                .command = snf::server::BattleCompleted{},
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
}

void run_protocol_room_result_sink_tests()
{
    test_a_join_is_answered_on_the_asking_connection();
    test_a_command_with_no_reply_context_answers_nobody();
    test_a_clear_notifies_every_online_participant_unsolicited();
    test_an_offline_participant_is_skipped_and_the_others_still_hear();
    test_a_full_outbound_channel_drops_the_notification();
}
