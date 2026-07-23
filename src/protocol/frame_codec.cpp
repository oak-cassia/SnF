#include "snf/protocol/frame_codec.hpp"

#include <stdexcept>

namespace
{
    constexpr std::uint32_t BYTE_MASK = 0xFFU;

    void append_u16_be(std::vector<std::byte>& bytes, std::uint16_t value)
    {
        bytes.push_back(static_cast<std::byte>((value >> 8U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>(value & BYTE_MASK));
    }

    void append_u32_be(std::vector<std::byte>& bytes, std::uint32_t value)
    {
        bytes.push_back(static_cast<std::byte>((value >> 24U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>((value >> 16U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>((value >> 8U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>(value & BYTE_MASK));
    }
}

namespace snf::protocol
{
    std::vector<std::byte> encode_frame(const Frame& frame)
    {
        if (frame.type != MessageType::Ping && frame.type != MessageType::Pong)
        {
            throw std::invalid_argument("Unknown message type");
        }

        if (constexpr auto max_payload_size = static_cast<std::size_t>(MAX_BODY_SIZE - MIN_BODY_SIZE); frame.payload.size() > max_payload_size)
        {
            throw std::length_error("Frame body exceeds the configured maximum size");
        }

        const auto body_size = static_cast<std::uint32_t>(MIN_BODY_SIZE + frame.payload.size());

        std::vector<std::byte> encoded;
        encoded.reserve(FRAME_LENGTH_FIELD_SIZE + body_size);

        append_u32_be(encoded, body_size);
        append_u16_be(encoded, static_cast<std::uint16_t>(frame.type));
        append_u32_be(encoded, frame.request_id);
        encoded.insert(encoded.end(), frame.payload.begin(), frame.payload.end());

        return encoded;
    }
}
