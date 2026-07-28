#include "snf/net/session.hpp"

#include <stdexcept>
#include <utility>

namespace snf::net
{
    Session::Session(UniqueFileDescriptor socket, const std::size_t max_pending_send_bytes) noexcept
        : _socket(std::move(socket))
        , _max_pending_send_bytes(max_pending_send_bytes)
    {
    }

    Session::Session(UniqueFileDescriptor socket,
                     const snf::server::ConnectionId connection_id,
                     const std::size_t max_pending_send_bytes) noexcept
        : _socket(std::move(socket))
        , _connection_id(connection_id)
        , _max_pending_send_bytes(max_pending_send_bytes)
    {
    }

    int Session::getDescriptor() const noexcept
    {
        return _socket.getDescriptor();
    }

    const snf::server::ConnectionId& Session::getConnectionId() const noexcept
    {
        return _connection_id;
    }

    protocol::DecodeResult Session::appendReceivedBytes(std::span<const std::byte> bytes)
    {
        return _frame_decoder.append(bytes);
    }

    bool Session::enqueueFrame(const protocol::Frame& frame)
    {
        auto encoded_frame = protocol::encode_frame(frame);

        if (_pending_send_byte_count > _max_pending_send_bytes ||
            encoded_frame.size() > _max_pending_send_bytes - _pending_send_byte_count)
        {
            return false;
        }

        _pending_send_byte_count += encoded_frame.size();
        _send_queue.push_back(PendingSend{.bytes = std::move(encoded_frame)});
        return true;
    }

    bool Session::hasPendingSend() const noexcept
    {
        return !_send_queue.empty();
    }

    std::size_t Session::getPendingSendByteCount() const noexcept
    {
        return _pending_send_byte_count;
    }

    std::span<const std::byte> Session::getPendingSendBytes() const noexcept
    {
        if (_send_queue.empty())
        {
            return {};
        }

        const PendingSend& pending_send = _send_queue.front();
        return std::span<const std::byte>{pending_send.bytes}.subspan(pending_send.offset);
    }

    bool Session::consumeSentBytes(const std::size_t byte_count)
    {
        if (_send_queue.empty() || byte_count > getPendingSendBytes().size())
        {
            throw std::out_of_range{"Sent byte count exceeds the pending send"};
        }

        PendingSend& pending_send = _send_queue.front();
        pending_send.offset += byte_count;
        _pending_send_byte_count -= byte_count;

        if (pending_send.offset == pending_send.bytes.size())
        {
            _send_queue.pop_front();
            return true;
        }

        return false;
    }
}
