#pragma once

#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/identity.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <span>
#include <vector>

namespace snf::worker
{
    enum class CloseReason : std::uint8_t;

    enum class ConnectionState : std::uint8_t
    {
        Open = 0,
        Closing = 1,
    };

    enum class SendResult : std::uint8_t
    {
        Accepted = 0,
        SoftLimit = 1, // noncritical frame was not queued
        HardLimit = 2, // frame was not queued
        Closing = 3,
        Stale = 4,
        Rejected = 5, // invalid frame or an unavailable local resource
    };

    struct ConnectionHandle
    {
        ConnectionId id;
        ConnectionGeneration generation;

        [[nodiscard]] constexpr bool isValid() const noexcept
        {
            return generation.isValid();
        }

        [[nodiscard]] bool operator==(const ConnectionHandle&) const noexcept = default;
    };

    struct ConnectionHandleHash
    {
        [[nodiscard]] std::size_t operator()(const ConnectionHandle handle) const noexcept
        {
            std::uint64_t value = static_cast<std::uint64_t>(handle.id.value) + 0x9e3779b97f4a7c15ULL;
            value ^= handle.generation.value + (value << 6U) + (value >> 2U);
            value ^= value >> 30U;
            value *= 0xbf58476d1ce4e5b9ULL;
            value ^= value >> 27U;
            value *= 0x94d049bb133111ebULL;
            value ^= value >> 31U;
            return std::hash<std::uint64_t>{}(value);
        }
    };

    struct ConnectionLimits
    {
        std::size_t max_read_buffer_bytes{512ull * 1024};
        std::size_t write_soft_watermark_bytes{1ull * 1024 * 1024};
        std::size_t write_hard_limit_bytes{4ull * 1024 * 1024};
        std::chrono::milliseconds close_drain_deadline{2000};
    };

    // This is the owner-thread-only write storage for one connection. It
    // stores encoded bytes, not Frames, so partial sends never re-encode or
    // reorder application messages.
    class WriteBuffer final
    {
    public:
        explicit WriteBuffer(std::size_t soft_watermark, std::size_t hard_limit);

        [[nodiscard]] SendResult append(const snf::protocol::Frame& frame, bool critical = false);
        [[nodiscard]] bool empty() const noexcept;
        [[nodiscard]] std::size_t queuedByteCount() const noexcept;
        [[nodiscard]] std::span<const std::byte> frontBytes() const noexcept;
        [[nodiscard]] bool consume(std::size_t byte_count);
        void clear() noexcept;

        [[nodiscard]] std::size_t softWatermark() const noexcept;
        [[nodiscard]] std::size_t hardLimit() const noexcept;

    private:
        struct PendingFrame
        {
            std::vector<std::byte> bytes;
            std::size_t offset{0};
        };

        const std::size_t _soft_watermark;
        const std::size_t _hard_limit;
        std::deque<PendingFrame> _frames;
        std::size_t _queued_bytes{0};
    };

    class ConnectionSlot final
    {
    public:
        ConnectionSlot(snf::net::UniqueFileDescriptor socket, ConnectionRef reference, const ConnectionLimits& limits);

        ConnectionSlot(const ConnectionSlot&) = delete;
        ConnectionSlot& operator=(const ConnectionSlot&) = delete;
        ConnectionSlot(ConnectionSlot&&) noexcept = delete;
        ConnectionSlot& operator=(ConnectionSlot&&) noexcept = delete;

        [[nodiscard]] int descriptor() const noexcept;
        [[nodiscard]] const ConnectionRef& reference() const noexcept;
        [[nodiscard]] ConnectionHandle handle() const noexcept;

        [[nodiscard]] ConnectionState state() const noexcept;
        [[nodiscard]] bool isOpen() const noexcept;
        [[nodiscard]] bool isClosing() const noexcept;
        void beginClosing(std::chrono::steady_clock::time_point deadline, CloseReason reason) noexcept;
        [[nodiscard]] std::chrono::steady_clock::time_point closeDeadline() const noexcept;
        [[nodiscard]] CloseReason closeReason() const noexcept;

        [[nodiscard]] bool readQueued() const noexcept;
        void setReadQueued(bool value) noexcept;
        [[nodiscard]] bool writeQueued() const noexcept;
        void setWriteQueued(bool value) noexcept;
        [[nodiscard]] bool waitingEpollout() const noexcept;
        void setWaitingEpollout(bool value) noexcept;

        [[nodiscard]] snf::protocol::FrameDecoder& decoder() noexcept;
        [[nodiscard]] const snf::protocol::FrameDecoder& decoder() const noexcept;
        [[nodiscard]] std::size_t bufferedByteCount() const noexcept;
        [[nodiscard]] std::size_t maxReadBufferBytes() const noexcept;
        [[nodiscard]] bool canBuffer(std::size_t byte_count) const noexcept;
        void resetReadBuffer() noexcept;

        // The caller transfers the frame to the owner connection. The write
        // buffer then encodes it into its byte-owned queue.
        [[nodiscard]] SendResult appendFrame(snf::protocol::Frame&& frame, bool critical = false);
        [[nodiscard]] bool hasPendingWrite() const noexcept;
        [[nodiscard]] std::size_t queuedWriteBytes() const noexcept;
        [[nodiscard]] std::span<const std::byte> pendingWriteBytes() const noexcept;
        [[nodiscard]] bool consumeWrittenBytes(std::size_t byte_count);
        void clearWriteBuffer() noexcept;

        [[nodiscard]] std::size_t writeSoftWatermarkBytes() const noexcept;
        [[nodiscard]] std::size_t writeHardLimitBytes() const noexcept;

    private:
        snf::net::UniqueFileDescriptor _socket;
        ConnectionRef _reference;
        ConnectionState _state{ConnectionState::Open};
        std::chrono::steady_clock::time_point _close_deadline{};
        CloseReason _close_reason{};
        bool _read_queued{false};
        bool _write_queued{false};
        bool _waiting_epollout{false};
        snf::protocol::FrameDecoder _decoder;
        WriteBuffer _write_buffer;
        std::size_t _max_read_buffer_bytes;
    };

    [[nodiscard]] constexpr std::uint32_t remoteSendCharge(const snf::protocol::Frame& frame) noexcept
    {
        const auto wire_size =
            static_cast<std::uint64_t>(snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE) + frame.payload.size();
        return static_cast<std::uint32_t>(wire_size);
    }
}
