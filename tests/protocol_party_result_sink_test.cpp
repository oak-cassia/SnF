#include "outbound_reservation_test_support.hpp"
#include "snf/server/outbound_channel.hpp"
#include "snf/server/protocol_party_result_sink.hpp"

#include <cassert>
#include <variant>

namespace
{
    void test_party_result_maps_sorted_members_to_wire()
    {
        const auto wake = snf::test::make_wake_descriptor();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 1, .max_slots_per_connection = 1},
            wake.getDescriptor()};
        snf::server::ProtocolPartyResultSink sink{outbound};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 5};

        sink.accept(
            snf::server::PartyInboundCommand{
                .party = snf::server::PartyId{.value = 7},
                .connection = connection,
                .command =
                    snf::server::JoinPartyCommand{
                        .player = snf::server::PlayerId{.value = 10},
                        .membership_epoch = 3,
                    },
                .reply =
                    snf::server::PartyReplyContext{
                        .connection = connection,
                        .request_id = 8,
                        .kind = snf::server::PartyReplyKind::Joined,
                    },
            },
            snf::server::PartyResult{
                .status = snf::server::PartyCommandStatus::Applied,
                .party = snf::server::PartyId{.value = 7},
                .player = snf::server::PlayerId{.value = 10},
                .membership_epoch = 3,
                .members =
                    {
                        snf::server::PlayerId{.value = 10},
                        snf::server::PlayerId{.value = 20},
                    },
            });

        const auto posted = outbound.tryPop();
        assert(posted.has_value());
        const auto* send = std::get_if<snf::server::SendFrame>(&posted->action);
        assert(send != nullptr);
        assert(send->frame.type == snf::protocol::MessageType::PartyJoined);
        assert(send->frame.request_id == 8);
        assert(send->frame.payload.size() == 35);
        assert(send->frame.payload[0] == std::byte{0});
        assert(send->frame.payload[17] == std::byte{0});
        assert(send->frame.payload[18] == std::byte{2});
    }
}

void run_protocol_party_result_sink_tests()
{
    test_party_result_maps_sorted_members_to_wire();
}
