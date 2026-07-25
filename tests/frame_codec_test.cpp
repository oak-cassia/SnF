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

void test_decodes_two_frames_received_together()
{
    const Frame ping{
        .type = MessageType::Ping,
        .request_id = 1,
        .payload = {std::byte{0xAA}},
    };
    const Frame pong{
        .type = MessageType::Pong,
        .request_id = 2,
        .payload = {std::byte{0xBB}, std::byte{0xCC}},
    };

    const auto encoded_ping = encode_frame(ping);
    const auto encoded_pong = encode_frame(pong);

    std::vector<std::byte> received;
    received.insert(received.end(), encoded_ping.begin(), encoded_ping.end());
    received.insert(received.end(), encoded_pong.begin(), encoded_pong.end());

    const std::span<const std::byte> received_view{received};

    FrameDecoder decoder{};
    const auto result = decoder.append(received_view);

    assert(result.ok());
    assert(result.frames.size() == 2);

    assert(result.frames[0].type == ping.type);
    assert(result.frames[0].request_id == ping.request_id);
    assert(result.frames[0].payload == ping.payload);

    assert(result.frames[1].type == pong.type);
    assert(result.frames[1].request_id == pong.request_id);
    assert(result.frames[1].payload == pong.payload);
}

void test_keeps_a_partial_second_frame_for_the_next_append()
{
    const Frame ping{
        .type = MessageType::Ping,
        .request_id = 1,
        .payload = {std::byte{0xAA}, std::byte{0xBB}},
    };
    const Frame pong{
        .type = MessageType::Pong,
        .request_id = 2,
        .payload = {std::byte{0xBB}, std::byte{0xCC}},
    };

    const auto encoded_ping = encode_frame(ping);
    const auto encoded_pong = encode_frame(pong);
    constexpr std::size_t second_frame_prefix_size = 6;

    std::vector<std::byte> first_received;
    first_received.insert(first_received.end(), encoded_ping.begin(), encoded_ping.end());
    first_received.insert(
        first_received.end(),
        encoded_pong.begin(),
        encoded_pong.begin() + second_frame_prefix_size
    );

    const std::span<const std::byte> first_received_view{first_received};

    FrameDecoder decoder{};
    const auto first_result = decoder.append(first_received_view);

    assert(first_result.ok());
    assert(first_result.frames.size() == 1);
    assert(first_result.frames[0].type == ping.type);
    assert(first_result.frames[0].request_id == ping.request_id);
    assert(first_result.frames[0].payload == ping.payload);

    const std::span<const std::byte> pong_view{encoded_pong};
    const auto second_result = decoder.append(pong_view.subspan(second_frame_prefix_size));

    assert(second_result.ok());
    assert(second_result.frames.size() == 1);
    assert(second_result.frames[0].type == pong.type);
    assert(second_result.frames[0].request_id == pong.request_id);
    assert(second_result.frames[0].payload == pong.payload);
}

void test_rejects_a_body_smaller_than_minimum()
{
    const std::vector<std::byte> invalid_frame{
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x05},
    };
    const std::span<const std::byte> invalid_frame_view{invalid_frame};

    FrameDecoder decoder{};
    const auto result = decoder.append(invalid_frame_view);

    assert(!result.ok());
    assert(result.frames.empty());
    assert(result.error.has_value());
    assert(*result.error == DecodeError::InvalidBodyLength);
}
void test_rejects_a_body_larger_than_maximum()
{
    const std::vector<std::byte> invalid_frame{
        std::byte{0x00}, std::byte{0x01}, std::byte{0x00}, std::byte{0x01},
    };
    const std::span<const std::byte> invalid_frame_view{invalid_frame};

    FrameDecoder decoder{};
    const auto result = decoder.append(invalid_frame_view);

    assert(!result.ok());
    assert(result.frames.empty());
    assert(result.error.has_value());
    assert(*result.error == DecodeError::BodyTooLarge);
}

void test_rejects_an_unknown_message_type()
{
    const std::vector<std::byte> invalid_frame{
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x06},
        std::byte{0x00}, std::byte{0x03},
        std::byte{0x00}, std::byte{0x00}, std::byte{0x00}, std::byte{0x01},
    };
    const std::span<const std::byte> invalid_frame_view{invalid_frame};

    FrameDecoder decoder{};
    const auto result = decoder.append(invalid_frame_view);

    assert(!result.ok());
    assert(result.frames.empty());
    assert(result.error.has_value());
    assert(*result.error == DecodeError::UnknownMessageType);
}

void run_frame_codec_tests()
{
    test_encode_frame();
    test_decodes_a_frame_received_in_two_chunks();
    test_decodes_two_frames_received_together();
    test_keeps_a_partial_second_frame_for_the_next_append();
    test_rejects_a_body_smaller_than_minimum();
    test_rejects_a_body_larger_than_maximum();
    test_rejects_an_unknown_message_type();
}
