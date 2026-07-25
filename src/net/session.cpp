#include "snf/net/session.hpp"

#include <utility>

namespace snf::net
{
    Session::Session(UniqueFileDescriptor socket) noexcept
        : _socket(std::move(socket))
    {
    }

    int Session::getDescriptor() const noexcept
    {
        return _socket.getDescriptor();
    }

    protocol::DecodeResult Session::appendReceivedBytes(std::span<const std::byte> bytes)
    {
        return _frame_decoder.append(bytes);
    }
}
