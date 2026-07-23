#include "snf/protocol/frame_codec.hpp"

#include <cassert>
#include <cstddef>
#include <span>
#include <vector>

using namespace snf::protocol;

void test_encode_frame()
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

void test_decodes_a_frame_received_in_two_chunks()
{
    const Frame frame{
        .type = MessageType::Ping,
        .request_id = 0x01020304,
        .payload = {std::byte{0xAA}, std::byte{0xBB}},
    };

    const auto encoded = encode_frame(frame);
    const std::span<const std::byte> encoded_view{encoded};
    constexpr std::size_t first_chunk_size = 5;

    FrameDecoder decoder{};

    const auto first_result = decoder.append(encoded_view.first(first_chunk_size));
    assert(first_result.ok());
    assert(first_result.frames.empty());

    const auto second_result = decoder.append(encoded_view.subspan(first_chunk_size));
    assert(second_result.ok());
    assert(second_result.frames.size() == 1);

    const auto& decoded = second_result.frames.front();
    assert(decoded.type == frame.type);
    assert(decoded.request_id == frame.request_id);
    assert(decoded.payload == frame.payload);
}

int main()
{
    test_encode_frame();
    test_decodes_a_frame_received_in_two_chunks();
}
