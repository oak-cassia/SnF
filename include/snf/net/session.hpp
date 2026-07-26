#pragma once

#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"

#include <cstddef>
#include <deque>
#include <span>
#include <vector>

namespace snf::net
{
    inline constexpr std::size_t MAX_PENDING_SEND_BYTES = 1024 * 1024;

    class Session
    {
    public:
        explicit Session(UniqueFileDescriptor socket,
                         std::size_t max_pending_send_bytes = MAX_PENDING_SEND_BYTES) noexcept;

        Session(const Session&) = delete;
        Session& operator=(const Session&) = delete;

        Session(Session&&) noexcept = default;
        Session& operator=(Session&&) noexcept = default;

        [[nodiscard]] int getDescriptor() const noexcept;

        [[nodiscard]] snf::protocol::DecodeResult
        appendReceivedBytes(std::span<const std::byte> bytes);

        [[nodiscard]] bool enqueueFrame(const snf::protocol::Frame& frame);
        [[nodiscard]] bool hasPendingSend() const noexcept;
        [[nodiscard]] std::size_t getPendingSendByteCount() const noexcept;
        [[nodiscard]] std::span<const std::byte> getPendingSendBytes() const noexcept;

        [[nodiscard]] bool consumeSentBytes(std::size_t byte_count);

    private:
        struct PendingSend
        {
            std::vector<std::byte> bytes;
            std::size_t offset{0};
        };

        UniqueFileDescriptor _socket;
        snf::protocol::FrameDecoder _frame_decoder;
        std::deque<PendingSend> _send_queue;
        std::size_t _pending_send_byte_count{0};
        std::size_t _max_pending_send_bytes;
    };
}
