#include "snf/load/client_connection.hpp"

#include "snf/net/socket_options.hpp"
#include "snf/net/system_error.hpp"

#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <cstddef>
#include <netinet/in.h>
#include <span>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <system_error>

namespace
{
    constexpr std::size_t RECEIVE_BUFFER_SIZE = 4096;

    std::string describe_error(const std::string_view operation, const int error_number)
    {
        return std::string{operation} + ": " + std::generic_category().message(error_number);
    }

    snf::net::UniqueFileDescriptor create_client_socket()
    {
        const int socket_descriptor =
            ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (socket_descriptor == -1)
        {
            snf::net::throw_system_error("socket");
        }

        return snf::net::UniqueFileDescriptor{socket_descriptor};
    }
}

namespace snf::load
{
    ClientConnection::ClientConnection(const std::string_view host, const std::uint16_t port)
        : _socket(create_client_socket())
        , _request_payload{std::byte{0x53}, std::byte{0x6E}, std::byte{0x46}}
    {
        snf::net::enable_tcp_no_delay(_socket.getDescriptor());

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(port);

        const std::string host_text{host};
        const int conversion_result =
            ::inet_pton(AF_INET, host_text.c_str(), &server_address.sin_addr);
        if (conversion_result == 0)
        {
            throw std::invalid_argument{"Host must be a numeric IPv4 address"};
        }
        if (conversion_result == -1)
        {
            snf::net::throw_system_error("inet_pton");
        }

        const int connect_result = ::connect(_socket.getDescriptor(),
                                             reinterpret_cast<const sockaddr*>(&server_address),
                                             sizeof(server_address));

        if (connect_result == 0)
        {
            beginRequest();
            return;
        }

        if (errno != EINPROGRESS)
        {
            snf::net::throw_system_error("connect");
        }
    }

    int ClientConnection::getDescriptor() const noexcept
    {
        return _socket.getDescriptor();
    }

    std::uint32_t ClientConnection::getDesiredEvents() const noexcept
    {
        std::uint32_t events = EPOLLRDHUP;

        if (_state == State::Connecting || _state == State::SendingRequest)
        {
            events |= EPOLLOUT;
        }

        if (_state == State::SendingRequest || _state == State::AwaitingResponse)
        {
            events |= EPOLLIN;
        }

        return events;
    }

    bool ClientConnection::isConnecting() const noexcept
    {
        return _state == State::Connecting;
    }

    bool ClientConnection::isComplete() const noexcept
    {
        return _state == State::Complete;
    }

    std::chrono::steady_clock::duration ClientConnection::getRoundTripTime() const noexcept
    {
        return _response_received_at - _request_started_at;
    }

    std::optional<std::string> ClientConnection::handleWritable()
    {
        if (_state == State::Connecting)
        {
            if (const auto socket_error = getSocketError())
            {
                return socket_error;
            }

            beginRequest();
        }

        if (_state != State::SendingRequest)
        {
            return std::nullopt;
        }

        // PING Frame 전체를 보내거나 socket이 EAGAIN을 반환할 때까지 전송한다.
        while (_send_offset < _pending_send_bytes.size())
        {
            const auto sent_byte_count = ::send(_socket.getDescriptor(),
                                                _pending_send_bytes.data() + _send_offset,
                                                _pending_send_bytes.size() - _send_offset,
                                                MSG_NOSIGNAL);

            if (sent_byte_count > 0)
            {
                _send_offset += static_cast<std::size_t>(sent_byte_count);
                continue;
            }

            if (sent_byte_count == -1 && errno == EINTR)
            {
                continue;
            }

            if (sent_byte_count == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
            {
                return std::nullopt;
            }

            const int error_number = errno;
            return describe_error("send", error_number);
        }

        _pending_send_bytes.clear();
        _send_offset = 0;
        _state = State::AwaitingResponse;
        return std::nullopt;
    }

    std::optional<std::string> ClientConnection::handleReadable()
    {
        std::array<std::byte, RECEIVE_BUFFER_SIZE> receive_buffer{};

        // PONG을 완성하거나 socket이 EAGAIN, EOF, 오류를 반환할 때까지 수신한다.
        while (true)
        {
            const auto received_byte_count =
                ::recv(_socket.getDescriptor(), receive_buffer.data(), receive_buffer.size(), 0);

            if (received_byte_count > 0)
            {
                const std::span<const std::byte> received_bytes{
                    receive_buffer.data(), static_cast<std::size_t>(received_byte_count)};
                const auto decode_result = _frame_decoder.append(received_bytes);

                if (!decode_result.ok())
                {
                    return std::string{"Protocol error while decoding PONG"};
                }

                if (decode_result.frames.size() > 1)
                {
                    return std::string{"Received more than one response for one request"};
                }

                if (!decode_result.frames.empty())
                {
                    if (const auto validation_error =
                            validateResponse(decode_result.frames.front()))
                    {
                        return validation_error;
                    }

                    _response_received_at = std::chrono::steady_clock::now();
                    _state = State::Complete;
                    return std::nullopt;
                }

                continue;
            }

            if (received_byte_count == 0)
            {
                return std::string{"Server closed the connection before PONG"};
            }

            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return std::nullopt;
            }

            const int error_number = errno;
            return describe_error("recv", error_number);
        }
    }

    std::optional<std::string> ClientConnection::getSocketError() const
    {
        int socket_error = 0;
        auto option_size = static_cast<socklen_t>(sizeof(socket_error));

        if (::getsockopt(
                _socket.getDescriptor(), SOL_SOCKET, SO_ERROR, &socket_error, &option_size) == -1)
        {
            const int error_number = errno;
            return describe_error("getsockopt(SO_ERROR)", error_number);
        }

        if (socket_error != 0)
        {
            return describe_error("connect", socket_error);
        }

        return std::nullopt;
    }

    void ClientConnection::beginRequest()
    {
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = _request_id,
            .payload = _request_payload,
        };

        _pending_send_bytes = snf::protocol::encode_frame(request);
        _request_started_at = std::chrono::steady_clock::now();
        _state = State::SendingRequest;
    }

    std::optional<std::string>
    ClientConnection::validateResponse(const snf::protocol::Frame& response)
    {
        if (response.type != snf::protocol::MessageType::Pong)
        {
            return std::string{"Response type is not PONG"};
        }

        if (response.request_id != _request_id)
        {
            return std::string{"PONG request ID does not match PING"};
        }

        if (response.payload != _request_payload)
        {
            return std::string{"PONG payload does not match PING"};
        }

        return std::nullopt;
    }
}
