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

    Session::Session(UniqueFileDescriptor socket, const ConnectionId connection_id, const std::size_t max_pending_send_bytes) noexcept
        : _socket(std::move(socket))
        , _connection_id(connection_id)
        , _max_pending_send_bytes(max_pending_send_bytes)
    {
    }

    int Session::getDescriptor() const noexcept
    {
        return _socket.getDescriptor();
    }

    const ConnectionId& Session::getConnectionId() const noexcept
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

        if (_pending_send_byte_count > _max_pending_send_bytes || encoded_frame.size() > _max_pending_send_bytes - _pending_send_byte_count)
        {
            return false;
        }

        _pending_send_byte_count += encoded_frame.size();
        // 인코딩된 vector의 소유권을 큐 뒤의 새 PendingSend로 옮긴다. offset은 0으로 시작한다.
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

        // front()는 첫 프레임을 참조로 반환한다.
        // 큐의 첫 프레임이라는 것과 프레임 내부 offset이 0이라는 것은 별개다.
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
        // 요청한 양이 아니라 send()가 실제로 받아들인 양만 반영한다.
        // 보낸 바이트를 vector에서 지우거나 남은 바이트를 앞으로 당기지 않는다.
        pending_send.offset += byte_count;
        _pending_send_byte_count -= byte_count;

        // 프레임 하나를 전부 보낸 경우
        if (pending_send.offset == pending_send.bytes.size())
        {
            _send_queue.pop_front();
            return true;
        }

        return false;
    }
}
