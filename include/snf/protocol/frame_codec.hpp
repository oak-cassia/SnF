#pragma once

#include "snf/protocol/frame.hpp"

#include <cstddef>
#include <optional>
#include <span>
#include <vector>

namespace snf::protocol
{
    enum class DecodeError
    {
        InvalidBodyLength,
        BodyTooLarge,
        UnknownMessageType,
    };

    struct DecodeResult
    {
        std::vector<Frame> frames;
        std::optional<DecodeError> error;

        [[nodiscard]] bool ok() const noexcept
        {
            return !error.has_value();
        }
    };

    [[nodiscard]] std::vector<std::byte> encode_frame(const Frame& frame);

    class FrameDecoder
    {
    public:
        [[nodiscard]] DecodeResult append(std::span<const std::byte> bytes);

    private:
        std::vector<std::byte> _buffer;
        std::size_t _read_offset{0};
    };
}
