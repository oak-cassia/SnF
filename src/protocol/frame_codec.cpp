#include "snf/protocol/frame_codec.hpp"

#include <stdexcept>

namespace
{
    constexpr std::uint32_t BYTE_MASK = 0xFFU;

    void append_u16_big_endian(std::vector<std::byte>& bytes, std::uint16_t value)
    {
        bytes.push_back(static_cast<std::byte>((value >> 8U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>(value & BYTE_MASK));
    }

    void append_u32_big_endian(std::vector<std::byte>& bytes, std::uint32_t value)
    {
        bytes.push_back(static_cast<std::byte>((value >> 24U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>((value >> 16U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>((value >> 8U) & BYTE_MASK));
        bytes.push_back(static_cast<std::byte>(value & BYTE_MASK));
    }

    std::uint32_t read_u32_big_endian(std::span<const std::byte> bytes, std::size_t offset)
    {
        return (std::to_integer<std::uint32_t>(bytes[offset]) << 24U) |
            (std::to_integer<std::uint32_t>(bytes[offset + 1]) << 16U) |
            (std::to_integer<std::uint32_t>(bytes[offset + 2]) << 8U) |
            std::to_integer<std::uint32_t>(bytes[offset + 3]);
    }

    std::uint16_t read_u16_big_endian(std::span<const std::byte> bytes, std::size_t offset)
    {
        return (std::to_integer<std::uint16_t>(bytes[offset]) << 8U) |
            (std::to_integer<std::uint16_t>(bytes[offset + 1]));
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

        if (frame.payload.size() > MAX_PAYLOAD_SIZE)
        {
            throw std::length_error("Frame body exceeds the configured maximum size");
        }

        const auto body_size = static_cast<std::uint32_t>(MIN_BODY_SIZE + frame.payload.size());

        std::vector<std::byte> encoded;
        encoded.reserve(FRAME_LENGTH_FIELD_SIZE + body_size);

        append_u32_big_endian(encoded, body_size);
        append_u16_big_endian(encoded, static_cast<std::uint16_t>(frame.type));
        append_u32_big_endian(encoded, frame.request_id);
        encoded.insert(encoded.end(), frame.payload.begin(), frame.payload.end());

        return encoded;
    }

    DecodeResult FrameDecoder::append(std::span<const std::byte> bytes)
    {
        _buffer.insert(_buffer.end(), bytes.begin(), bytes.end());

        DecodeResult result{};

        // 버퍼에 완성된 프레임이 남아 있지 않을 때까지 디코딩한다.
        while (true)
        {
            const auto available_bytes = _buffer.size() - _read_offset;

            if (available_bytes < FRAME_LENGTH_FIELD_SIZE)
            {
                break;
            }

            const std::span<const std::byte> buffer_view{_buffer};
            const auto body_size = read_u32_big_endian(buffer_view, _read_offset);
            if (body_size < MIN_BODY_SIZE)
            {
                return DecodeResult{
                    .frames = {},
                    .error = DecodeError::InvalidBodyLength,
                };
            }

            if (body_size > MAX_BODY_SIZE)
            {
                return DecodeResult{
                    .frames = {},
                    .error = DecodeError::BodyTooLarge,
                };
            }

            const auto full_frame_size = static_cast<std::size_t>(FRAME_LENGTH_FIELD_SIZE) + body_size;

            if (available_bytes < full_frame_size)
            {
                break;
            }

            const auto request_type = static_cast<MessageType>(
                read_u16_big_endian(buffer_view, _read_offset + FRAME_LENGTH_FIELD_SIZE)
            );

            if (request_type != MessageType::Ping && request_type != MessageType::Pong)
            {
                return DecodeResult{
                    .frames = {},
                    .error = DecodeError::UnknownMessageType,
                };
            }

            const auto request_id = read_u32_big_endian(
                buffer_view, _read_offset + FRAME_LENGTH_FIELD_SIZE + FRAME_TYPE_SIZE
            );

            const auto payload_begin = _read_offset + FRAME_LENGTH_FIELD_SIZE + MIN_BODY_SIZE;
            const auto payload_size = static_cast<std::size_t>(body_size - MIN_BODY_SIZE);
            const auto payload_end = payload_begin + payload_size;

            result.frames.push_back(Frame{
                .type = request_type,
                .request_id = request_id,
                .payload = std::vector<std::byte>(buffer_view.begin() + payload_begin,
                                                  buffer_view.begin() + payload_end),
            });

            _read_offset += full_frame_size;
        }

        if (_read_offset == _buffer.size())
        {
            _buffer.clear();
            _read_offset = 0;
        }
        else if (_read_offset > 0)
        {
            _buffer.erase(_buffer.begin(), _buffer.begin() + _read_offset);
            _read_offset = 0;
        }

        return result;
    }
}
