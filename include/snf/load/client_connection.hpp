#pragma once

#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace snf::load
{
    enum class ClientErrorKind
    {
        Socket,
        Protocol,
    };

    struct ClientError
    {
        ClientErrorKind kind;
        std::string message;
    };

    struct WriteResult
    {
        bool connected{false};
        std::size_t sent_requests{0};
        std::optional<ClientError> error;
    };

    struct ReadResult
    {
        std::vector<std::chrono::steady_clock::duration> round_trip_times;
        std::optional<ClientError> error;
    };

    class ClientConnection
    {
    public:
        ClientConnection(std::string_view host,
                         std::uint16_t port,
                         std::chrono::milliseconds connect_timeout);

        ClientConnection(const ClientConnection&) = delete;
        ClientConnection& operator=(const ClientConnection&) = delete;

        ClientConnection(ClientConnection&&) noexcept = default;
        ClientConnection& operator=(ClientConnection&&) noexcept = default;

        [[nodiscard]] int getDescriptor() const noexcept;
        [[nodiscard]] std::uint32_t getDesiredEvents() const noexcept;
        [[nodiscard]] bool isConnecting() const noexcept;
        [[nodiscard]] bool isConnected() const noexcept;
        [[nodiscard]] bool canStartRequest() const noexcept;
        [[nodiscard]] bool isIdle() const noexcept;
        [[nodiscard]] std::chrono::steady_clock::time_point getDeadline() const noexcept;

        void enqueuePing(std::chrono::milliseconds request_timeout);
        [[nodiscard]] WriteResult handleWritable();
        [[nodiscard]] ReadResult handleReadable();
        [[nodiscard]] std::optional<ClientError> getSocketError() const;

    private:
        enum class State
        {
            Connecting,
            Connected,
        };

        struct OutstandingRequest
        {
            std::uint32_t request_id;
            std::vector<std::byte> payload;
            std::chrono::steady_clock::time_point started_at;
            std::chrono::steady_clock::time_point deadline;
        };

        [[nodiscard]] std::optional<ClientError>
        validateResponse(const snf::protocol::Frame& response) const;

        snf::net::UniqueFileDescriptor _socket;
        State _state{State::Connecting};
        snf::protocol::FrameDecoder _frame_decoder;
        std::vector<std::byte> _pending_send_bytes;
        std::size_t _send_offset{0};
        std::uint32_t _next_request_id{1};
        std::chrono::steady_clock::time_point _connect_deadline;
        std::optional<OutstandingRequest> _outstanding_request;
    };
}
