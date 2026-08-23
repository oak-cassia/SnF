#pragma once

#include "snf/load/load_scenario.hpp"
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
        std::size_t sent_bootstrap_requests{0};
        std::size_t sent_gameplay_requests{0};
        std::optional<ClientError> error;
    };

    struct ReadResult
    {
        std::vector<std::chrono::steady_clock::duration> round_trip_times;
        std::vector<std::chrono::steady_clock::duration> gameplay_round_trip_times;
        std::vector<std::chrono::steady_clock::duration> battle_digest_intervals;
        std::size_t bootstrap_responses{0};
        std::size_t battle_start_rejections{0};
        std::size_t gameplay_responses{0};
        std::size_t unsolicited_frames{0};
        std::size_t unsolicited_bytes{0};
        std::size_t battle_digest_frames{0};
        std::size_t battle_digest_bytes{0};
        std::size_t battle_cleared_frames{0};
        std::size_t battle_failed_frames{0};
        std::size_t returned_to_zone_frames{0};
        std::optional<ClientError> error;
    };

    class ClientConnection
    {
    public:
        ClientConnection(std::string_view host, std::uint16_t port, std::chrono::milliseconds connect_timeout, ClientWorkload workload = {});

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
        [[nodiscard]] bool hasCompletedBootstrap() const noexcept;
        [[nodiscard]] bool hasJoinedRoom() const noexcept;
        [[nodiscard]] bool needsBattleStart() const noexcept;

        void enqueueNextRequest(std::chrono::milliseconds request_timeout);
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
            snf::protocol::MessageType request_type;
            std::vector<std::byte> payload;
            std::int32_t expected_x{0};
            std::int32_t expected_y{0};
            bool bootstrap{false};
            std::chrono::steady_clock::time_point started_at;
            std::chrono::steady_clock::time_point deadline;
        };

        [[nodiscard]] std::optional<ClientError> validateResponse(const snf::protocol::Frame& response) const;
        [[nodiscard]] std::optional<ClientError>
        recordUnsolicitedFrame(const snf::protocol::Frame& response, std::chrono::steady_clock::time_point received_at, ReadResult& result);
        void completeRequest(snf::protocol::MessageType request_type, bool battle_start_rejected) noexcept;

        enum class WorkloadStage
        {
            Ping,
            Authenticate,
            EnterZone,
            Move,
            RoomJoin,
            BattleStart,
            AwaitBattleStart,
            BattleAction,
            Complete,
        };

        snf::net::UniqueFileDescriptor _socket;
        State _state{State::Connecting};
        snf::protocol::FrameDecoder _frame_decoder;
        std::vector<std::byte> _pending_send_bytes;
        std::size_t _send_offset{0};
        std::uint32_t _next_request_id{1};
        std::chrono::steady_clock::time_point _connect_deadline;
        std::optional<OutstandingRequest> _outstanding_request;
        ClientWorkload _workload;
        WorkloadStage _workload_stage{WorkloadStage::Ping};
        std::uint64_t _move_sequence{0};
        std::uint64_t _skill_sequence{0};
        bool _next_battle_action_is_skill{true};
        std::optional<std::uint64_t> _last_battle_digest_sequence;
        std::optional<std::chrono::steady_clock::time_point> _last_battle_digest_at;
    };
}
