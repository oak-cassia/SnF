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
#include <utility>

namespace
{
    constexpr std::size_t RECEIVE_BUFFER_SIZE = 4096;

    std::string describe_error(const std::string_view operation, const int error_number)
    {
        return std::string{operation} + ": " + std::generic_category().message(error_number);
    }

    snf::load::ClientError socket_error(const std::string_view operation, const int error_number)
    {
        return snf::load::ClientError{
            .kind = snf::load::ClientErrorKind::Socket,
            .message = describe_error(operation, error_number),
        };
    }

    snf::load::ClientError protocol_error(std::string message)
    {
        return snf::load::ClientError{
            .kind = snf::load::ClientErrorKind::Protocol,
            .message = std::move(message),
        };
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

    std::vector<std::byte> encode_timestamp(const std::uint64_t timestamp)
    {
        std::vector<std::byte> payload;
        payload.reserve(sizeof(timestamp));

        for (int shift = 56; shift >= 0; shift -= 8)
        {
            payload.push_back(static_cast<std::byte>((timestamp >> shift) & 0xFFU));
        }

        return payload;
    }
}

namespace snf::load
{
    ClientConnection::ClientConnection(const std::string_view host,
                                       const std::uint16_t port,
                                       const std::chrono::milliseconds connect_timeout)
        : _socket(create_client_socket())
        , _connect_deadline(std::chrono::steady_clock::now() + connect_timeout)
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
            _state = State::Connected;
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

        if (_state == State::Connected)
        {
            events |= EPOLLIN;
        }

        if (_state == State::Connecting || !_pending_send_bytes.empty())
        {
            events |= EPOLLOUT;
        }

        return events;
    }

    bool ClientConnection::isConnecting() const noexcept
    {
        return _state == State::Connecting;
    }

    bool ClientConnection::isConnected() const noexcept
    {
        return _state == State::Connected;
    }

    bool ClientConnection::canStartRequest() const noexcept
    {
        return isConnected() && _pending_send_bytes.empty() && !_outstanding_request.has_value();
    }

    bool ClientConnection::isIdle() const noexcept
    {
        return _pending_send_bytes.empty() && !_outstanding_request.has_value();
    }

    std::chrono::steady_clock::time_point ClientConnection::getDeadline() const noexcept
    {
        if (isConnecting())
        {
            return _connect_deadline;
        }

        if (_outstanding_request)
        {
            return _outstanding_request->deadline;
        }

        return std::chrono::steady_clock::time_point::max();
    }

    void ClientConnection::enqueuePing(const std::chrono::milliseconds request_timeout)
    {
        if (!canStartRequest())
        {
            throw std::logic_error{"Connection already has an outstanding request"};
        }

        const auto started_at = std::chrono::steady_clock::now();
        const auto timestamp = static_cast<std::uint64_t>(started_at.time_since_epoch().count());
        auto payload = encode_timestamp(timestamp);

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = _next_request_id,
            .payload = payload,
        };

        _pending_send_bytes = snf::protocol::encode_frame(request);
        _send_offset = 0;
        _outstanding_request = OutstandingRequest{
            .request_id = _next_request_id,
            .payload = std::move(payload),
            .started_at = started_at,
            .deadline = started_at + request_timeout,
        };

        ++_next_request_id;
        if (_next_request_id == 0)
        {
            ++_next_request_id;
        }
    }

    WriteResult ClientConnection::handleWritable()
    {
        WriteResult result{};

        if (_state == State::Connecting)
        {
            if (const auto pending_socket_error = getSocketError())
            {
                result.error = pending_socket_error;
                return result;
            }

            _state = State::Connected;
            result.connected = true;
        }

        // 현재 PING을 모두 보내거나 socket이 EAGAIN을 반환할 때까지 전송한다.
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
                return result;
            }

            result.error = socket_error("send", errno);
            return result;
        }

        if (!_pending_send_bytes.empty())
        {
            _pending_send_bytes.clear();
            _send_offset = 0;
            result.sent_requests = 1;
        }

        return result;
    }

    ReadResult ClientConnection::handleReadable()
    {
        ReadResult result{};
        std::array<std::byte, RECEIVE_BUFFER_SIZE> receive_buffer{};

        // 수신 가능한 모든 PONG을 읽거나 EAGAIN, EOF, 오류가 발생할 때까지 반복한다.
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
                    result.error = protocol_error("Protocol error while decoding PONG");
                    return result;
                }

                for (const auto& response : decode_result.frames)
                {
                    if (const auto validation_error = validateResponse(response))
                    {
                        result.error = validation_error;
                        return result;
                    }

                    result.round_trip_times.push_back(std::chrono::steady_clock::now() -
                                                      _outstanding_request->started_at);
                    _outstanding_request.reset();
                }

                continue;
            }

            if (received_byte_count == 0)
            {
                result.error = socket_error("recv", ECONNRESET);
                return result;
            }

            if (errno == EINTR)
            {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return result;
            }

            result.error = socket_error("recv", errno);
            return result;
        }
    }

    std::optional<ClientError> ClientConnection::getSocketError() const
    {
        int pending_error = 0;
        auto option_size = static_cast<socklen_t>(sizeof(pending_error));

        if (::getsockopt(
                _socket.getDescriptor(), SOL_SOCKET, SO_ERROR, &pending_error, &option_size) == -1)
        {
            return socket_error("getsockopt(SO_ERROR)", errno);
        }

        if (pending_error != 0)
        {
            return socket_error("connect", pending_error);
        }

        return std::nullopt;
    }

    std::optional<ClientError>
    ClientConnection::validateResponse(const snf::protocol::Frame& response) const
    {
        if (!_outstanding_request)
        {
            return protocol_error("Received PONG without an outstanding PING");
        }

        if (response.type != snf::protocol::MessageType::Pong)
        {
            return protocol_error("Response type is not PONG");
        }

        if (response.request_id != _outstanding_request->request_id)
        {
            return protocol_error("PONG request ID does not match PING");
        }

        if (response.payload != _outstanding_request->payload)
        {
            return protocol_error("PONG payload does not match PING");
        }

        return std::nullopt;
    }
}
