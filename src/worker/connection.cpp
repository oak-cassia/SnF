#include "snf/worker/connection.hpp"

#include <limits>
#include <stdexcept>
#include <utility>

namespace snf::worker
{
    WriteBuffer::WriteBuffer(const std::size_t soft_watermark, const std::size_t hard_limit)
        : _soft_watermark(soft_watermark)
        , _hard_limit(hard_limit)
    {
        if (_soft_watermark == 0 || _hard_limit == 0 || _soft_watermark > _hard_limit)
        {
            throw std::invalid_argument{"Invalid connection write watermarks"};
        }
    }

    SendResult WriteBuffer::append(const snf::protocol::Frame& frame, const bool critical)
    {
        std::vector<std::byte> encoded;
        try
        {
            encoded = snf::protocol::encode_frame(frame);
        }
        catch (const std::invalid_argument&)
        {
            return SendResult::Rejected;
        }
        catch (const std::length_error&)
        {
            return SendResult::Rejected;
        }

        if (encoded.size() > _hard_limit || _queued_bytes > _hard_limit - encoded.size())
        {
            return SendResult::HardLimit;
        }

        if (!critical && (encoded.size() > _soft_watermark || _queued_bytes > _soft_watermark - encoded.size()))
        {
            return SendResult::SoftLimit;
        }

        _frames.push_back(PendingFrame{.bytes = std::move(encoded)});
        _queued_bytes += _frames.back().bytes.size();

        return SendResult::Accepted;
    }

    bool WriteBuffer::empty() const noexcept
    {
        return _frames.empty();
    }

    std::size_t WriteBuffer::queuedByteCount() const noexcept
    {
        return _queued_bytes;
    }

    std::span<const std::byte> WriteBuffer::frontBytes() const noexcept
    {
        if (_frames.empty())
        {
            return {};
        }

        const PendingFrame& frame = _frames.front();
        return std::span<const std::byte>{frame.bytes}.subspan(frame.offset);
    }

    bool WriteBuffer::consume(const std::size_t byte_count)
    {
        if (_frames.empty() || byte_count > frontBytes().size())
        {
            throw std::out_of_range{"Written byte count exceeds the pending frame"};
        }

        PendingFrame& frame = _frames.front();
        frame.offset += byte_count;
        _queued_bytes -= byte_count;

        if (frame.offset == frame.bytes.size())
        {
            _frames.pop_front();
            return true;
        }

        return false;
    }

    void WriteBuffer::clear() noexcept
    {
        _frames.clear();
        _queued_bytes = 0;
    }

    std::size_t WriteBuffer::softWatermark() const noexcept
    {
        return _soft_watermark;
    }

    std::size_t WriteBuffer::hardLimit() const noexcept
    {
        return _hard_limit;
    }

    ConnectionSlot::ConnectionSlot(snf::net::UniqueFileDescriptor socket, const ConnectionRef reference, const ConnectionLimits& limits)
        : _socket(std::move(socket))
        , _reference(reference)
        , _write_buffer(limits.write_soft_watermark_bytes, limits.write_hard_limit_bytes)
        , _max_read_buffer_bytes(limits.max_read_buffer_bytes)
    {
        if (!_socket.isValid() || !_reference.generation.isValid() || limits.max_read_buffer_bytes == 0 ||
            limits.close_drain_deadline < std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Invalid connection slot configuration"};
        }
    }

    int ConnectionSlot::descriptor() const noexcept
    {
        return _socket.getDescriptor();
    }

    const ConnectionRef& ConnectionSlot::reference() const noexcept
    {
        return _reference;
    }

    ConnectionHandle ConnectionSlot::handle() const noexcept
    {
        return ConnectionHandle{.id = _reference.id, .generation = _reference.generation};
    }

    ConnectionState ConnectionSlot::state() const noexcept
    {
        return _state;
    }

    bool ConnectionSlot::isOpen() const noexcept
    {
        return _state == ConnectionState::Open;
    }

    bool ConnectionSlot::isClosing() const noexcept
    {
        return _state == ConnectionState::Closing;
    }

    void ConnectionSlot::beginClosing(const std::chrono::steady_clock::time_point deadline, const CloseReason reason) noexcept
    {
        _state = ConnectionState::Closing;
        _close_deadline = deadline;
        _close_reason = reason;
        _decoder.reset();
    }

    std::chrono::steady_clock::time_point ConnectionSlot::closeDeadline() const noexcept
    {
        return _close_deadline;
    }

    CloseReason ConnectionSlot::closeReason() const noexcept
    {
        return _close_reason;
    }

    bool ConnectionSlot::readQueued() const noexcept
    {
        return _read_queued;
    }

    void ConnectionSlot::setReadQueued(const bool value) noexcept
    {
        _read_queued = value;
    }

    bool ConnectionSlot::writeQueued() const noexcept
    {
        return _write_queued;
    }

    void ConnectionSlot::setWriteQueued(const bool value) noexcept
    {
        _write_queued = value;
    }

    bool ConnectionSlot::waitingEpollout() const noexcept
    {
        return _waiting_epollout;
    }

    void ConnectionSlot::setWaitingEpollout(const bool value) noexcept
    {
        _waiting_epollout = value;
    }

    snf::protocol::FrameDecoder& ConnectionSlot::decoder() noexcept
    {
        return _decoder;
    }

    const snf::protocol::FrameDecoder& ConnectionSlot::decoder() const noexcept
    {
        return _decoder;
    }

    std::size_t ConnectionSlot::bufferedByteCount() const noexcept
    {
        return _decoder.bufferedByteCount();
    }

    std::size_t ConnectionSlot::maxReadBufferBytes() const noexcept
    {
        return _max_read_buffer_bytes;
    }

    bool ConnectionSlot::canBuffer(const std::size_t byte_count) const noexcept
    {
        return byte_count <= _max_read_buffer_bytes && bufferedByteCount() <= _max_read_buffer_bytes - byte_count;
    }

    void ConnectionSlot::resetReadBuffer() noexcept
    {
        _decoder.reset();
    }

    SendResult ConnectionSlot::appendFrame(snf::protocol::Frame&& frame, const bool critical)
    {
        if (isClosing())
        {
            return SendResult::Closing;
        }
        return _write_buffer.append(frame, critical);
    }

    bool ConnectionSlot::hasPendingWrite() const noexcept
    {
        return !_write_buffer.empty();
    }

    std::size_t ConnectionSlot::queuedWriteBytes() const noexcept
    {
        return _write_buffer.queuedByteCount();
    }

    std::span<const std::byte> ConnectionSlot::pendingWriteBytes() const noexcept
    {
        return _write_buffer.frontBytes();
    }

    bool ConnectionSlot::consumeWrittenBytes(const std::size_t byte_count)
    {
        return _write_buffer.consume(byte_count);
    }

    void ConnectionSlot::clearWriteBuffer() noexcept
    {
        _write_buffer.clear();
    }

    std::size_t ConnectionSlot::writeSoftWatermarkBytes() const noexcept
    {
        return _write_buffer.softWatermark();
    }

    std::size_t ConnectionSlot::writeHardLimitBytes() const noexcept
    {
        return _write_buffer.hardLimit();
    }
}
