#include "snf/protocol/frame_codec.hpp"

#include <cassert>
#include <cstddef>
#include <vector>

int main()
{
    const snf::protocol::Frame frame{
        .type = snf::protocol::MessageType::Ping,
        .request_id = 0x01020304,
        .payload = {std::byte{0xAA}, std::byte{0xBB}}
    };

    const auto result = snf::protocol::encode_frame(frame);

    const std::vector<std::byte> expected{
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x08},
        std::byte{0x00}, std::byte{0x01},
        std::byte{0x01}, std::byte{0x02}, std::byte{0x03}, std::byte{0x04},
        std::byte{0xAA}, std::byte{0xBB},
    };

    assert(result == expected);
}
