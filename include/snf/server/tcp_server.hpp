#pragma once

#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/server/message_dispatcher.hpp"

#include <chrono>
#include <cstddef>
#include <optional>
#include <unordered_map>

namespace snf::server
{
    struct TcpServerConfig
    {
        std::uint16_t port{7777};
        std::chrono::milliseconds shutdown_grace_period{5000};
        std::size_t max_pending_send_bytes{snf::net::MAX_PENDING_SEND_BYTES};
        std::optional<int> client_send_buffer_size;
    };

    struct TcpServerStats
    {
        std::uint64_t accepted_connections{0};
        std::uint64_t closed_connections{0};
        std::uint64_t received_frames{0};
        std::uint64_t sent_frames{0};
        std::uint64_t protocol_errors{0};
    };

    class TcpServer
    {
    public:
        explicit TcpServer(const TcpServerConfig& config);
        explicit TcpServer(
            std::uint16_t port,
            std::chrono::milliseconds shutdown_grace_period = std::chrono::milliseconds{5000});

        TcpServer(const TcpServer&) = delete;
        TcpServer& operator=(const TcpServer&) = delete;

        TcpServer(TcpServer&&) = delete;
        TcpServer& operator=(TcpServer&&) = delete;

        [[nodiscard]] std::uint16_t getPort() const noexcept;
        [[nodiscard]] const TcpServerStats& getStats() const noexcept;

        void run(int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD);
        void requestStop() const noexcept;

    private:
        void registerListener() const;
        void registerControlDescriptor(int descriptor) const;
        void acceptPendingClients();
        void handleClientEvent(int client_descriptor, std::uint32_t event_flags);
        void handleStopRequest();
        void handleTerminationSignal(int signal_descriptor);
        void beginShutdown();
        [[nodiscard]] bool flushPendingSend(snf::net::Session& session);
        void updateClientEvents(const snf::net::Session& session) const;
        void removeSession(int client_descriptor);
        void closeRemainingSessions();
        [[nodiscard]] int getEpollWaitTimeout() const;

        snf::net::UniqueFileDescriptor _listener;
        snf::net::UniqueFileDescriptor _epoll;
        snf::net::UniqueFileDescriptor _stop_event;
        std::uint16_t _port;
        std::chrono::milliseconds _shutdown_grace_period;
        std::size_t _max_pending_send_bytes;
        std::optional<int> _client_send_buffer_size;
        std::chrono::steady_clock::time_point _shutdown_deadline{};
        bool _is_stopping{false};
        std::unordered_map<int, snf::net::Session> _sessions;
        MessageDispatcher _message_dispatcher;
        TcpServerStats _stats;
    };
}
