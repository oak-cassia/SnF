#pragma once

#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/command_ingress.hpp"
#include "snf/server/runtime_types.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
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
        std::uint64_t actor_queue_overflows{0};
        std::uint64_t stale_network_actions{0};
    };

    // The epoll reactor. GameServer provides an ingress and outbound queue; this component
    // only decodes inbound frames and applies NetworkAction values on the reactor thread.
    class TcpServer
    {
    public:
        TcpServer(const TcpServerConfig& config,
                  CommandIngress& command_ingress,
                  snf::runtime::BoundedQueue<NetworkAction>& network_actions,
                  int outbound_event_descriptor);

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
        void registerControlDescriptor(int descriptor, std::uint64_t event_token) const;
        void acceptPendingClients();
        void handleClientEvent(int client_descriptor, std::uint32_t event_flags);
        void handleOutboundActions();
        void handleNetworkAction(NetworkAction action);
        void handleStopRequest();
        void handleTerminationSignal(int signal_descriptor);
        void beginShutdown();
        void completeShutdownAfterActorRuntimeDrained();
        void abortShutdownAfterActorRuntimeFailure();
        void cancelQueues();
        [[nodiscard]] bool flushPendingSend(snf::net::Session& session);
        void updateClientEvents(const snf::net::Session& session) const;
        void removeSession(int client_descriptor);
        void closeRemainingSessions();
        [[nodiscard]] int getEpollWaitTimeout() const;
        [[nodiscard]] snf::net::Session* findCurrentSession(ConnectionId connection);

        snf::net::UniqueFileDescriptor _listener;
        snf::net::UniqueFileDescriptor _epoll;
        snf::net::UniqueFileDescriptor _stop_event;
        std::uint16_t _port;
        std::chrono::milliseconds _shutdown_grace_period;
        std::size_t _max_pending_send_bytes;
        std::optional<int> _client_send_buffer_size;
        CommandIngress& _command_ingress;
        snf::runtime::BoundedQueue<NetworkAction>& _network_actions;
        int _outbound_event_descriptor;
        std::chrono::steady_clock::time_point _shutdown_deadline{};
        std::uint64_t _next_connection_generation{0};
        bool _is_stopping{false};
        bool _actor_runtime_drained{false};
        std::unordered_map<int, snf::net::Session> _sessions;
        // epoll may return a copied event after its FD has been closed and reused.
        // Generation tokens let the reactor discard that event instead of targeting the new FD.
        std::unordered_map<std::uint64_t, int> _client_descriptors_by_event_token;
        TcpServerStats _stats;
    };
}
