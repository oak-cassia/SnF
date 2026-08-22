#include "snf/protocol/frame_codec.hpp"

#include <stdexcept>
#include <utility>

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
        return (std::to_integer<std::uint32_t>(bytes[offset]) << 24U) | (std::to_integer<std::uint32_t>(bytes[offset + 1]) << 16U) |
               (std::to_integer<std::uint32_t>(bytes[offset + 2]) << 8U) | std::to_integer<std::uint32_t>(bytes[offset + 3]);
    }

    std::uint16_t read_u16_big_endian(std::span<const std::byte> bytes, std::size_t offset)
    {
        return (std::to_integer<std::uint16_t>(bytes[offset]) << 8U) | (std::to_integer<std::uint16_t>(bytes[offset + 1]));
    }

    bool is_known_message_type(const snf::protocol::MessageType type) noexcept
    {
        switch (type)
        {
        case snf::protocol::MessageType::Ping:
        case snf::protocol::MessageType::Pong:
        case snf::protocol::MessageType::Authenticate:
        case snf::protocol::MessageType::Authenticated:
        case snf::protocol::MessageType::EnterZone:
        case snf::protocol::MessageType::ZoneEntered:
        case snf::protocol::MessageType::Move:
        case snf::protocol::MessageType::Moved:
        case snf::protocol::MessageType::LeaveZone:
        case snf::protocol::MessageType::ZoneLeft:
        case snf::protocol::MessageType::Purchase:
        case snf::protocol::MessageType::PurchaseResult:
        case snf::protocol::MessageType::PartyJoin:
        case snf::protocol::MessageType::PartyJoined:
        case snf::protocol::MessageType::PartyLeave:
        case snf::protocol::MessageType::RoomJoin:
        case snf::protocol::MessageType::RoomJoined:
        case snf::protocol::MessageType::BattleStart:
        case snf::protocol::MessageType::BattleStarted:
        case snf::protocol::MessageType::BattleCleared:
        case snf::protocol::MessageType::RoomLeave:
        case snf::protocol::MessageType::RoomLeft:
        case snf::protocol::MessageType::ReturnedToZone:
        case snf::protocol::MessageType::PartyLeft:
        case snf::protocol::MessageType::UseSkill:
        case snf::protocol::MessageType::SkillApplied:
        case snf::protocol::MessageType::BattleFailed:
        case snf::protocol::MessageType::BattleDigest:
        case snf::protocol::MessageType::SkillAcknowledged:
            return true;
        }

        return false;
    }
}

namespace snf::protocol
{
    std::vector<std::byte> encode_frame(const Frame& frame)
    {
        if (!is_known_message_type(frame.type))
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

    void FrameDecoder::push(std::span<const std::byte> bytes)
    {
        compactConsumedPrefix();
        _buffer.insert(_buffer.end(), bytes.begin(), bytes.end());
    }

    DecodeNextResult FrameDecoder::tryDecodeNext()
    {
        const auto available_bytes = _buffer.size() - _read_offset;

        if (available_bytes < FRAME_LENGTH_FIELD_SIZE)
        {
            compactConsumedPrefix();
            return {};
        }

        const std::span<const std::byte> buffer_view{_buffer};
        const auto body_size = read_u32_big_endian(buffer_view, _read_offset);
        if (body_size < MIN_BODY_SIZE)
        {
            return fail(DecodeError::InvalidBodyLength);
        }

        if (body_size > MAX_BODY_SIZE)
        {
            return fail(DecodeError::BodyTooLarge);
        }

        const auto full_frame_size = static_cast<std::size_t>(FRAME_LENGTH_FIELD_SIZE) + body_size;

        if (available_bytes < full_frame_size)
        {
            compactConsumedPrefix();
            return {};
        }

        const auto request_type = static_cast<MessageType>(read_u16_big_endian(buffer_view, _read_offset + FRAME_LENGTH_FIELD_SIZE));

        if (!is_known_message_type(request_type))
        {
            return fail(DecodeError::UnknownMessageType);
        }

        const auto request_id = read_u32_big_endian(buffer_view, _read_offset + FRAME_LENGTH_FIELD_SIZE + FRAME_TYPE_SIZE);

        const auto payload_begin = _read_offset + FRAME_LENGTH_FIELD_SIZE + MIN_BODY_SIZE;
        const auto payload_size = static_cast<std::size_t>(body_size - MIN_BODY_SIZE);
        const auto payload_end = payload_begin + payload_size;

        Frame frame{
            .type = request_type,
            .request_id = request_id,
            .payload = std::vector<std::byte>(buffer_view.begin() + payload_begin, buffer_view.begin() + payload_end),
        };

        _read_offset += full_frame_size;
        if (_read_offset == _buffer.size())
        {
            compactConsumedPrefix();
        }

        return DecodeNextResult{.frame = std::move(frame), .error = std::nullopt};
    }

    DecodeResult FrameDecoder::append(std::span<const std::byte> bytes)
    {
        push(bytes);

        DecodeResult result{};
        while (true)
        {
            auto next = tryDecodeNext();
            if (next.hasFrame())
            {
                result.frames.push_back(std::move(*next.frame));
                continue;
            }

            result.error = next.error;
            break;
        }

        return result;
    }

    void FrameDecoder::compactConsumedPrefix()
    {
        if (_read_offset == 0)
        {
            return;
        }

        if (_read_offset == _buffer.size())
        {
            _buffer.clear();
        }
        else
        {
            _buffer.erase(_buffer.begin(), _buffer.begin() + _read_offset);
        }
        _read_offset = 0;
    }

    DecodeNextResult FrameDecoder::fail(DecodeError error)
    {
        _buffer.clear();
        _read_offset = 0;
        return DecodeNextResult{.frame = std::nullopt, .error = error};
    }
}
