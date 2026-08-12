#include "outbound_reservation_test_support.hpp"
#include "snf/server/outbound_channel.hpp"
#include "snf/server/protocol_zone_result_sink.hpp"

#include <cassert>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace
{
    void test_zone_result_maps_to_a_bounded_wire_response()
    {
        const auto wake = snf::test::make_wake_descriptor();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 2, .max_slots_per_connection = 2},
            wake.getDescriptor()};
        snf::server::ProtocolZoneResultSink sink{outbound};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};

        sink.accept(
            snf::server::ZoneInboundCommand{
                .zone = snf::server::ZoneId{.value = 3},
                .command =
                    snf::server::MoveInZoneCommand{
                        .player = snf::server::PlayerId{.value = 7},
                        .route_epoch = 5,
                        .position = {.x = -2, .y = 4},
                    },
                .reply =
                    snf::server::ZoneReplyContext{
                        .connection = connection,
                        .request_id = 42,
                        .kind = snf::server::ZoneReplyKind::Moved,
                    },
            },
            snf::server::ZoneResult{
                .status = snf::server::ZoneCommandStatus::Applied,
                .player = snf::server::PlayerId{.value = 7},
                .position = snf::server::ZonePosition{.x = -2, .y = 4},
                .route_epoch = 5,
                .tick = 0,
                .visible_players = {snf::server::PlayerId{.value = 8}},
            });

        const auto posted = outbound.tryPop();
        assert(posted.has_value());
        const auto* send = std::get_if<snf::server::SendFrame>(&posted->action);
        assert(send != nullptr);
        assert(send->connection == connection);
        assert(send->frame.type == snf::protocol::MessageType::Moved);
        assert(send->frame.request_id == 42);
        assert(send->frame.payload.size() == 35);
        assert(send->frame.payload[0] == std::byte{0});
    }

    void test_zone_result_without_reply_is_internal_and_saturation_closes_explicitly()
    {
        const auto wake = snf::test::make_wake_descriptor();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1},
            wake.getDescriptor()};
        snf::server::ProtocolZoneResultSink sink{outbound};
        const snf::net::ConnectionId connection{.descriptor = 5, .generation = 10};
        const snf::server::ZoneResult result{};

        sink.accept(
            snf::server::ZoneInboundCommand{
                .zone = snf::server::ZoneId{.value = 3},
                .command = snf::server::ZoneSimulationTick{.tick = 1},
                .reply = std::nullopt,
            },
            result);
        assert(outbound.size() == 0);

        auto held = outbound.tryReserve(connection, 1);
        assert(held.has_value());
        sink.accept(
            snf::server::ZoneInboundCommand{
                .zone = snf::server::ZoneId{.value = 3},
                .command = snf::server::ZoneSimulationTick{.tick = 2},
                .reply =
                    snf::server::ZoneReplyContext{
                        .connection = connection,
                        .request_id = 2,
                        .kind = snf::server::ZoneReplyKind::Moved,
                    },
            },
            result);

        std::vector<snf::net::ConnectionId> failures;
        assert(!outbound.takePendingAdmissionFailures(failures));
        assert(failures == std::vector<snf::net::ConnectionId>{connection});
    }
}

void run_protocol_zone_result_sink_tests()
{
    test_zone_result_maps_to_a_bounded_wire_response();
    test_zone_result_without_reply_is_internal_and_saturation_closes_explicitly();
}
