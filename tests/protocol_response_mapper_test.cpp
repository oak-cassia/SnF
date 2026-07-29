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
}

void run_protocol_response_mapper_tests()
{
    test_maps_typed_pong_response_to_wire_frame();
}
