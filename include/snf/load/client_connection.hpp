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
    class ClientConnection
    {
    public:
        ClientConnection(std::string_view host,
                         std::uint16_t port,
                         std::uint32_t request_id,
                         std::chrono::milliseconds connect_timeout,
                         std::chrono::milliseconds request_timeout);

        ClientConnection(const ClientConnection&) = delete;
        ClientConnection& operator=(const ClientConnection&) = delete;

        ClientConnection(ClientConnection&&) noexcept = default;
        ClientConnection& operator=(ClientConnection&&) noexcept = default;

        [[nodiscard]] int getDescriptor() const noexcept;
        [[nodiscard]] std::uint32_t getDesiredEvents() const noexcept;
        [[nodiscard]] bool isConnecting() const noexcept;
        [[nodiscard]] bool isComplete() const noexcept;
        [[nodiscard]] std::chrono::steady_clock::time_point getDeadline() const noexcept;
        [[nodiscard]] std::string getTimeoutError() const;
        [[nodiscard]] std::chrono::steady_clock::duration getRoundTripTime() const noexcept;

        [[nodiscard]] std::optional<std::string> handleWritable();
        [[nodiscard]] std::optional<std::string> handleReadable();
        [[nodiscard]] std::optional<std::string> getSocketError() const;

    private:
        enum class State
        {
            Connecting,
            SendingRequest,
            AwaitingResponse,
            Complete,
        };

        void beginRequest();
        [[nodiscard]] std::optional<std::string>
        validateResponse(const snf::protocol::Frame& response);

        snf::net::UniqueFileDescriptor _socket;
        State _state{State::Connecting};
        snf::protocol::FrameDecoder _frame_decoder;
        std::vector<std::byte> _pending_send_bytes;
        std::size_t _send_offset{0};
        std::uint32_t _request_id{1};
        std::vector<std::byte> _request_payload;
        std::chrono::milliseconds _request_timeout;
        std::chrono::steady_clock::time_point _deadline;
        std::chrono::steady_clock::time_point _request_started_at{};
        std::chrono::steady_clock::time_point _response_received_at{};
    };
}
