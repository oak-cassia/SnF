#include "snf/protocol/frame_codec.hpp"

#include <cassert>
#include <cstddef>
#include <span>
#include <vector>

using namespace snf::protocol;

void test_encode_frame()
{
    const snf::protocol::Frame frame{
        .type = snf::protocol::MessageType::Ping, .request_id = 0x01020304, .payload = {std::byte{0xAA}, std::byte{0xBB}}
    };

    const auto result = snf::protocol::encode_frame(frame);

    const std::vector<std::byte> expected{
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x08},
        std::byte{0x00},
        std::byte{0x01},
        std::byte{0x01},
        std::byte{0x02},
        std::byte{0x03},
        std::byte{0x04},
        std::byte{0xAA},
        std::byte{0xBB},
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

void test_pull_decoder_returns_one_frame_at_a_time()
{
    const Frame ping{
        .type = MessageType::Ping,
        .request_id = 11,
        .payload = {std::byte{0xAA}},
    };
    const Frame pong{
        .type = MessageType::Pong,
        .request_id = 12,
        .payload = {std::byte{0xBB}},
    };

    const auto encoded_ping = encode_frame(ping);
    const auto encoded_pong = encode_frame(pong);
    std::vector<std::byte> received;
    received.insert(received.end(), encoded_ping.begin(), encoded_ping.end());
    received.insert(received.end(), encoded_pong.begin(), encoded_pong.end());

    FrameDecoder decoder{};
    decoder.push(received);

    const auto first = decoder.tryDecodeNext();
    assert(first.hasFrame());
    assert(!first.error.has_value());
    assert(first.frame->type == ping.type);
    assert(first.frame->request_id == ping.request_id);
    assert(first.frame->payload == ping.payload);

    const auto second = decoder.tryDecodeNext();
    assert(second.hasFrame());
    assert(!second.error.has_value());
    assert(second.frame->type == pong.type);
    assert(second.frame->request_id == pong.request_id);
    assert(second.frame->payload == pong.payload);

    const auto empty = decoder.tryDecodeNext();
    assert(empty.needsMoreData());
}

void test_pull_decoder_keeps_a_partial_frame_until_more_data_arrives()
{
    const Frame ping{
        .type = MessageType::Ping,
        .request_id = 13,
        .payload = {std::byte{0xAA}, std::byte{0xBB}},
    };
    const auto encoded = encode_frame(ping);
    const std::span<const std::byte> encoded_view{encoded};
    constexpr std::size_t first_chunk_size = 5;

    FrameDecoder decoder{};
    decoder.push(encoded_view.first(first_chunk_size));
    assert(decoder.tryDecodeNext().needsMoreData());

    decoder.push(encoded_view.subspan(first_chunk_size));
    const auto decoded = decoder.tryDecodeNext();
    assert(decoded.hasFrame());
    assert(decoded.frame->type == ping.type);
    assert(decoded.frame->request_id == ping.request_id);
    assert(decoded.frame->payload == ping.payload);
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
    first_received.insert(first_received.end(), encoded_pong.begin(), encoded_pong.begin() + second_frame_prefix_size);

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

void test_decodes_the_minimum_body_size()
{
    const Frame frame{
        .type = MessageType::Ping,
        .request_id = 1,
        .payload = {},
    };

    const auto encoded = encode_frame(frame);
    FrameDecoder decoder{};
    const auto result = decoder.append(encoded);

    assert(result.ok());
    assert(result.frames.size() == 1);
    assert(result.frames[0].type == frame.type);
    assert(result.frames[0].request_id == frame.request_id);
    assert(result.frames[0].payload.empty());
}

void test_decodes_the_maximum_body_size()
{
    const Frame frame{
        .type = MessageType::Ping,
        .request_id = 2,
        .payload = std::vector<std::byte>(MAX_PAYLOAD_SIZE, std::byte{0xA5}),
    };

    const auto encoded = encode_frame(frame);
    FrameDecoder decoder{};
    const auto result = decoder.append(encoded);

    assert(result.ok());
    assert(result.frames.size() == 1);
    assert(result.frames[0].type == frame.type);
    assert(result.frames[0].request_id == frame.request_id);
    assert(result.frames[0].payload == frame.payload);
}

void test_rejects_a_body_smaller_than_minimum()
{
    const std::vector<std::byte> invalid_frame{
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x05},
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
        std::byte{0x00},
        std::byte{0x01},
        std::byte{0x00},
        std::byte{0x01},
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
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x06},
        std::byte{0xFF},
        std::byte{0xFF},
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x01},
    };
    const std::span<const std::byte> invalid_frame_view{invalid_frame};

    FrameDecoder decoder{};
    const auto result = decoder.append(invalid_frame_view);

    assert(!result.ok());
    assert(result.frames.empty());
    assert(result.error.has_value());
    assert(*result.error == DecodeError::UnknownMessageType);
}

void test_append_preserves_valid_frames_before_an_error()
{
    const Frame ping{
        .type = MessageType::Ping,
        .request_id = 21,
        .payload = {std::byte{0xAA}},
    };
    const auto encoded_ping = encode_frame(ping);
    const std::vector<std::byte> invalid_length{
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x05},
    };

    std::vector<std::byte> received;
    received.insert(received.end(), encoded_ping.begin(), encoded_ping.end());
    received.insert(received.end(), invalid_length.begin(), invalid_length.end());

    FrameDecoder decoder{};
    const auto result = decoder.append(received);

    assert(!result.ok());
    assert(result.frames.size() == 1);
    assert(result.frames.front().type == ping.type);
    assert(result.frames.front().request_id == ping.request_id);
    assert(result.frames.front().payload == ping.payload);
    assert(result.error == DecodeError::InvalidBodyLength);
}

void test_decoder_can_be_reused_after_an_error()
{
    const std::vector<std::byte> invalid_length{
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x00},
        std::byte{0x05},
    };
    const Frame pong{
        .type = MessageType::Pong,
        .request_id = 22,
        .payload = {std::byte{0xCC}},
    };

    FrameDecoder decoder{};
    decoder.push(invalid_length);
    const auto error = decoder.tryDecodeNext();
    assert(!error.hasFrame());
    assert(error.error == DecodeError::InvalidBodyLength);

    decoder.push(encode_frame(pong));
    const auto decoded = decoder.tryDecodeNext();
    assert(decoded.hasFrame());
    assert(!decoded.error.has_value());
    assert(decoded.frame->type == pong.type);
    assert(decoded.frame->request_id == pong.request_id);
    assert(decoded.frame->payload == pong.payload);
    assert(decoder.tryDecodeNext().needsMoreData());
}

void test_decodes_room_handoff_frames()
{
    const Frame leave{.type = MessageType::RoomLeave, .request_id = 10, .payload = {}};
    const Frame left{.type = MessageType::RoomLeft, .request_id = 11, .payload = {std::byte{0x00}}};
    const Frame returned{.type = MessageType::ReturnedToZone, .request_id = 0, .payload = {std::byte{0x01}, std::byte{0x02}}};
    const Frame move{.type = MessageType::SetMoveIntent, .request_id = 12, .payload = {std::byte{0x00}}};
    const Frame acknowledged{.type = MessageType::MoveAcknowledged, .request_id = 12, .payload = {std::byte{0x00}, std::byte{0x01}}};

    FrameDecoder decoder{};
    decoder.push(encode_frame(leave));
    decoder.push(encode_frame(left));
    decoder.push(encode_frame(returned));
    decoder.push(encode_frame(move));
    decoder.push(encode_frame(acknowledged));

    const auto first = decoder.tryDecodeNext();
    assert(first.hasFrame() && first.frame->type == MessageType::RoomLeave);
    const auto second = decoder.tryDecodeNext();
    assert(second.hasFrame() && second.frame->type == MessageType::RoomLeft);
    const auto third = decoder.tryDecodeNext();
    assert(third.hasFrame() && third.frame->type == MessageType::ReturnedToZone);
    const auto fourth = decoder.tryDecodeNext();
    assert(fourth.hasFrame() && fourth.frame->type == MessageType::SetMoveIntent);
    const auto fifth = decoder.tryDecodeNext();
    assert(fifth.hasFrame() && fifth.frame->type == MessageType::MoveAcknowledged);
    assert(decoder.tryDecodeNext().needsMoreData());
}

void run_frame_codec_tests()
{
    test_encode_frame();
    test_decodes_a_frame_received_in_two_chunks();
    test_decodes_two_frames_received_together();
    test_pull_decoder_returns_one_frame_at_a_time();
    test_pull_decoder_keeps_a_partial_frame_until_more_data_arrives();
    test_keeps_a_partial_second_frame_for_the_next_append();
    test_decodes_the_minimum_body_size();
    test_decodes_the_maximum_body_size();
    test_rejects_a_body_smaller_than_minimum();
    test_rejects_a_body_larger_than_maximum();
    test_rejects_an_unknown_message_type();
    test_append_preserves_valid_frames_before_an_error();
    test_decoder_can_be_reused_after_an_error();
    test_decodes_room_handoff_frames();
}
