#include "snf/load/client_connection.hpp"

#include "snf/net/socket_options.hpp"
#include "snf/net/system_error.hpp"

#include <arpa/inet.h>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <netinet/in.h>
#include <span>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <system_error>
#include <type_traits>
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

    template <typename Integer>
    void append_big_endian(std::vector<std::byte>& payload, const Integer value)
    {
        for (std::size_t remaining = sizeof(Integer); remaining > 0; --remaining)
        {
            const std::size_t shift = (remaining - 1) * 8;
            payload.push_back(static_cast<std::byte>(
                (static_cast<std::make_unsigned_t<Integer>>(value) >> shift) & 0xFFU));
        }
    }

    template <typename Integer>
    Integer read_big_endian(const std::vector<std::byte>& payload, const std::size_t offset)
    {
        using Unsigned = std::make_unsigned_t<Integer>;
        Unsigned value = 0;
        for (std::size_t index = 0; index < sizeof(Integer); ++index)
        {
            value = static_cast<Unsigned>((value << 8U) |
                                          std::to_integer<unsigned int>(payload[offset + index]));
        }
        return static_cast<Integer>(value);
    }
}

namespace snf::load
{
    ClientConnection::ClientConnection(const std::string_view host,
                                       const std::uint16_t port,
                                       const std::chrono::milliseconds connect_timeout,
                                       const ClientWorkload workload)
        : _socket(create_client_socket())
        , _connect_deadline(std::chrono::steady_clock::now() + connect_timeout)
        , _workload(workload)
        , _workload_stage(workload.scenario == LoadScenario::Ping ? WorkloadStage::Ping
                                                                  : WorkloadStage::Authenticate)
    {
        if (_workload.scenario == LoadScenario::Zone &&
            (_workload.player_id == 0 || _workload.zone_id == 0))
        {
            throw std::invalid_argument{"Zone workload identities must be non-zero"};
        }
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

    bool ClientConnection::hasCompletedBootstrap() const noexcept
    {
        return _workload_stage == WorkloadStage::Ping || _workload_stage == WorkloadStage::Move;
    }

    void ClientConnection::enqueueNextRequest(const std::chrono::milliseconds request_timeout)
    {
        if (!canStartRequest())
        {
            throw std::logic_error{"Connection already has an outstanding request"};
        }

        const auto started_at = std::chrono::steady_clock::now();
        snf::protocol::MessageType request_type = snf::protocol::MessageType::Ping;
        std::vector<std::byte> payload;
        std::int32_t expected_x = 0;
        std::int32_t expected_y = 0;
        bool bootstrap = false;

        switch (_workload_stage)
        {
        case WorkloadStage::Ping:
            payload =
                encode_timestamp(static_cast<std::uint64_t>(started_at.time_since_epoch().count()));
            break;
        case WorkloadStage::Authenticate:
            request_type = snf::protocol::MessageType::Authenticate;
            append_big_endian(payload, _workload.player_id);
            bootstrap = true;
            break;
        case WorkloadStage::EnterZone:
            request_type = snf::protocol::MessageType::EnterZone;
            append_big_endian(payload, _workload.zone_id);
            append_big_endian(payload, expected_x);
            append_big_endian(payload, expected_y);
            bootstrap = true;
            break;
        case WorkloadStage::Move:
        {
            request_type = snf::protocol::MessageType::Move;
            ++_move_sequence;
            constexpr std::uint64_t POSITION_SPAN = 2001;
            expected_x = static_cast<std::int32_t>(_move_sequence % POSITION_SPAN) - 1000;
            expected_y = static_cast<std::int32_t>((_move_sequence * 17) % POSITION_SPAN) - 1000;
            append_big_endian(payload, expected_x);
            append_big_endian(payload, expected_y);
            break;
        }
        }

        const snf::protocol::Frame request{
            .type = request_type,
            .request_id = _next_request_id,
            .payload = payload,
        };

        _pending_send_bytes = snf::protocol::encode_frame(request);
        _send_offset = 0;
        _outstanding_request = OutstandingRequest{
            .request_id = _next_request_id,
            .request_type = request_type,
            .payload = std::move(payload),
            .expected_x = expected_x,
            .expected_y = expected_y,
            .bootstrap = bootstrap,
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

        // 현재 request를 모두 보내거나 socket이 EAGAIN을 반환할 때까지 전송한다.
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
            if (_outstanding_request && _outstanding_request->bootstrap)
            {
                result.sent_bootstrap_requests = 1;
            }
            else
            {
                result.sent_gameplay_requests = 1;
            }
        }

        return result;
    }

    ReadResult ClientConnection::handleReadable()
    {
        ReadResult result{};
        std::array<std::byte, RECEIVE_BUFFER_SIZE> receive_buffer{};

        // 수신 가능한 response를 읽거나 EAGAIN, EOF, 오류가 발생할 때까지 반복한다.
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
                    result.error = protocol_error("Protocol error while decoding response");
                    return result;
                }

                for (const auto& response : decode_result.frames)
                {
                    if (const auto validation_error = validateResponse(response))
                    {
                        result.error = validation_error;
                        return result;
                    }

                    const auto round_trip_time =
                        std::chrono::steady_clock::now() - _outstanding_request->started_at;
                    result.round_trip_times.push_back(round_trip_time);
                    if (_outstanding_request->bootstrap)
                    {
                        ++result.bootstrap_responses;
                    }
                    else
                    {
                        ++result.gameplay_responses;
                        result.gameplay_round_trip_times.push_back(round_trip_time);
                    }
                    const auto completed_type = _outstanding_request->request_type;
                    _outstanding_request.reset();
                    completeRequest(completed_type);
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
            return protocol_error("Received response without an outstanding request");
        }

        snf::protocol::MessageType expected_type = snf::protocol::MessageType::Pong;
        switch (_outstanding_request->request_type)
        {
        case snf::protocol::MessageType::Ping:
            expected_type = snf::protocol::MessageType::Pong;
            break;
        case snf::protocol::MessageType::Authenticate:
            expected_type = snf::protocol::MessageType::Authenticated;
            break;
        case snf::protocol::MessageType::EnterZone:
            expected_type = snf::protocol::MessageType::ZoneEntered;
            break;
        case snf::protocol::MessageType::Move:
            expected_type = snf::protocol::MessageType::Moved;
            break;
        default:
            return protocol_error("Unsupported load request type");
        }
        if (response.type != expected_type)
        {
            return protocol_error("Response type does not match the request");
        }

        if (response.request_id != _outstanding_request->request_id)
        {
            return protocol_error("Response request ID does not match the request");
        }

        if (_outstanding_request->request_type == snf::protocol::MessageType::Ping ||
            _outstanding_request->request_type == snf::protocol::MessageType::Authenticate)
        {
            if (response.payload != _outstanding_request->payload)
            {
                return protocol_error("Response payload does not match the request");
            }
            return std::nullopt;
        }

        constexpr std::size_t FIXED_ZONE_RESPONSE_SIZE = 1 + 8 + 8 + 4 + 4 + 2;
        if (response.payload.size() < FIXED_ZONE_RESPONSE_SIZE ||
            std::to_integer<std::uint8_t>(response.payload[0]) != 0 ||
            read_big_endian<std::uint64_t>(response.payload, 1) != _workload.zone_id ||
            read_big_endian<std::uint64_t>(response.payload, 9) == 0)
        {
            return protocol_error("Zone response fields do not match the request");
        }
        if (_outstanding_request->request_type == snf::protocol::MessageType::Move &&
            (read_big_endian<std::int32_t>(response.payload, 17) !=
                 _outstanding_request->expected_x ||
             read_big_endian<std::int32_t>(response.payload, 21) !=
                 _outstanding_request->expected_y))
        {
            return protocol_error("Move response position does not match the request");
        }
        const std::uint16_t visible_count = read_big_endian<std::uint16_t>(response.payload, 25);
        if (response.payload.size() != FIXED_ZONE_RESPONSE_SIZE + visible_count * 8U)
        {
            return protocol_error("Zone response member count does not match its payload");
        }

        std::uint64_t previous_player = 0;
        for (std::size_t index = 0; index < visible_count; ++index)
        {
            const std::uint64_t player = read_big_endian<std::uint64_t>(
                response.payload, FIXED_ZONE_RESPONSE_SIZE + index * 8U);
            if (player == 0 || player == _workload.player_id || player <= previous_player)
            {
                return protocol_error("Zone response players are not a sorted peer set");
            }
            previous_player = player;
        }

        return std::nullopt;
    }

    void ClientConnection::completeRequest(const snf::protocol::MessageType request_type) noexcept
    {
        if (request_type == snf::protocol::MessageType::Authenticate)
        {
            _workload_stage = WorkloadStage::EnterZone;
        }
        else if (request_type == snf::protocol::MessageType::EnterZone)
        {
            _workload_stage = WorkloadStage::Move;
        }
    }
}
