#pragma once

#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"

#include <cstddef>
#include <span>

namespace snf::net
{
    class Session
    {
    public:
        explicit Session(UniqueFileDescriptor socket) noexcept;

        Session(const Session&) = delete;
        Session& operator=(const Session&) = delete;

        Session(Session&&) noexcept = default;
        Session& operator=(Session&&) noexcept = default;

        [[nodiscard]] int getDescriptor() const noexcept;

        [[nodiscard]] snf::protocol::DecodeResult
        appendReceivedBytes(std::span<const std::byte> bytes);

    private:
        UniqueFileDescriptor _socket;
        snf::protocol::FrameDecoder _frame_decoder;
    };
}
