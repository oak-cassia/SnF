#include "snf/server/protocol_response_mapper.hpp"

#include <cassert>

namespace
{
    void test_maps_typed_pong_response_to_wire_frame()
    {
        const snf::server::ProtocolResponseMapper mapper;
        const auto frame = mapper.map(snf::server::PongResponse{
            .request_id = 19,
            .payload = {std::byte{0xAB}},
        });

        assert(frame.type == snf::protocol::MessageType::Pong);
        assert(frame.request_id == 19);
        assert(frame.payload == std::vector<std::byte>{std::byte{0xAB}});
    }

    void test_maps_authenticated_response_with_persistent_player_id()
    {
        const snf::server::ProtocolResponseMapper mapper;
        const auto frame = mapper.map(snf::server::AuthenticatedResponse{
            .request_id = 20,
            .player = snf::server::PlayerId{.value = 0x0102030405060708ULL},
        });

        assert(frame.type == snf::protocol::MessageType::Authenticated);
        assert(frame.request_id == 20);
        assert(frame.payload == std::vector<std::byte>({
                                    std::byte{0x01},
                                    std::byte{0x02},
                                    std::byte{0x03},
                                    std::byte{0x04},
                                    std::byte{0x05},
                                    std::byte{0x06},
                                    std::byte{0x07},
                                    std::byte{0x08},
                                }));
    }
}

void run_protocol_response_mapper_tests()
{
    test_maps_typed_pong_response_to_wire_frame();
    test_maps_authenticated_response_with_persistent_player_id();
}
