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
        case snf::protocol::MessageType::RoomJoin:
        case snf::protocol::MessageType::RoomJoined:
        case snf::protocol::MessageType::BattleStart:
        case snf::protocol::MessageType::BattleStarted:
        case snf::protocol::MessageType::BattleCleared:
        case snf::protocol::MessageType::RoomLeave:
        case snf::protocol::MessageType::RoomLeft:
        case snf::protocol::MessageType::ReturnedToZone:
        case snf::protocol::MessageType::UseSkill:
        case snf::protocol::MessageType::SkillApplied:
        case snf::protocol::MessageType::BattleFailed:
        case snf::protocol::MessageType::BattleDigest:
        case snf::protocol::MessageType::SkillAcknowledged:
        case snf::protocol::MessageType::SetMoveIntent:
        case snf::protocol::MessageType::MoveAcknowledged:
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
        // TCP는 메시지 경계를 보존하지 않는다. 미완성 프레임 뒤에 새 수신 바이트를 이어 붙인다.
        compactConsumedPrefix();
        _buffer.insert(_buffer.end(), bytes.begin(), bytes.end());
    }

    DecodeNextResult FrameDecoder::tryDecodeNext()
    {
        const auto available_bytes = _buffer.size() - _read_offset;

        // 길이 필드가 부족하면 반환한다. 블로킹하지 않고 다음 수신에서 다시 시도한다.
        if (available_bytes < FRAME_LENGTH_FIELD_SIZE)
        {
            compactConsumedPrefix();
            return {};
        }

        // 버퍼의 읽기 전용 뷰. _read_offset에서 시작하는 현재 프레임의 첫 4바이트가 본문 길이다.
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

        // 버퍼 크기와 타입을 맞춰 size_t로 계산한다. 현재 길이 상한에서는 필수 변환은 아니다.
        const auto full_frame_size = static_cast<std::size_t>(FRAME_LENGTH_FIELD_SIZE) + body_size;

        // 유효한 길이라도 본문이 덜 도착했다면 미완성 데이터를 보존하고 반환한다.
        if (available_bytes < full_frame_size)
        {
            compactConsumedPrefix();
            return {};
        }

        // 길이 다음 2바이트가 메시지 종류
        const auto request_type = static_cast<MessageType>(read_u16_big_endian(buffer_view, _read_offset + FRAME_LENGTH_FIELD_SIZE));

        if (!is_known_message_type(request_type))
        {
            return fail(DecodeError::UnknownMessageType);
        }

        // 메시지 종류 다음 4바이트가 요청 식별자
        const auto request_id = read_u32_big_endian(buffer_view, _read_offset + FRAME_LENGTH_FIELD_SIZE + FRAME_TYPE_SIZE);

        const auto payload_begin = _read_offset + FRAME_LENGTH_FIELD_SIZE + MIN_BODY_SIZE;
        const auto payload_size = static_cast<std::size_t>(body_size - MIN_BODY_SIZE);
        const auto payload_end = payload_begin + payload_size;

        // vector에 [payload_begin, payload_end)의 바이트를 복사해 Frame이 독립적으로 소유한다.
        // - span으로 반환하면 이후 디코더 버퍼 정리나 재할당으로 참조가 무효화될 수 있다.
        // - payload 크기는 생성 후 고정이어도 프레임마다 런타임에 결정된다.
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
            DecodeNextResult next = tryDecodeNext();
            if (next.hasFrame())
            {
                // *는 optional 안의 Frame을 참조로 꺼낸다. 아래 삽입에서 vector 소유권을 이동해 바이트 재복사를 피한다.
                result.frames.push_back(std::move(*next.frame));

                // 한 번 수신한 데이터에 여러 프레임이 있을 수 있으니 계속 추출 시도
                continue;
            }

            result.error = next.error;
            break;
        }

        return result;
    }

    // _read_offset 앞의 해석 완료 구간만 제거하고, 아직 해석하지 못한 바이트는 남긴다.
    // 예: [완료된 A][미완성 B] -> [미완성 B]. 다음 해석 위치는 다시 0이 된다.
    void FrameDecoder::compactConsumedPrefix()
    {
        if (_read_offset == 0)
        {
            return;
        }

        if (_read_offset == _buffer.size())
        {
            // 모든 바이트를 소비했다. clear는 크기를 0으로 만들며 capacity는 유지한다.
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
