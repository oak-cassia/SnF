#pragma once

#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/frame_ingress.hpp"
#include "snf/server/outbound_action.hpp"
#include "snf/server/runtime_completion.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
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
        // A slot is retained until a disconnected session's lifecycle fact is
        // accepted or the ingress closes. This bounds both active sessions and
        // the reactor-owned retry deque without dropping ConnectionClosed.
        std::size_t connection_lifecycle_capacity{4096};
    };

    struct TcpServerStats
    {
        std::uint64_t accepted_connections{0};
        std::uint64_t closed_connections{0};
        std::uint64_t received_frames{0};
        std::uint64_t sent_frames{0};
        std::uint64_t protocol_errors{0};
        std::uint64_t actor_queue_overflows{0};
        std::uint64_t stale_outbound_actions{0};
        std::uint64_t connection_lifecycle_rejections{0};
        std::size_t pending_connection_closes_high_water_mark{0};
    };

    // The epoll reactor. It decodes frames, submits FrameEnvelope values to the shared
    // protocol gateway, and applies OutboundAction values on the reactor thread.
    class TcpServer
    {
    public:
        TcpServer(const TcpServerConfig& config,
                  FrameIngress& frame_ingress,
                  snf::runtime::BoundedQueue<OutboundAction>& outbound_actions,
                  RuntimeCompletionSource& runtime_completion,
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
        void handleOutboundAction(OutboundAction action);
        void handleRuntimeCompletion();
        void handleStopRequest();
        void handleTerminationSignal(int signal_descriptor);
        void beginShutdown();
        void completeShutdownAfterGameRuntimesDrained();
        void abortShutdownAfterActorRuntimeFailure();
        void cancelQueues();
        [[nodiscard]] bool flushPendingSend(snf::net::Session& session);
        void updateClientEvents(const snf::net::Session& session) const;
        void removeSession(int client_descriptor, ConnectionCloseCause cause);
        void closeRemainingSessions();
        void notifyConnectionClosed(ConnectionClosed closed);
        void retryPendingConnectionCloses();
        [[nodiscard]] bool hasAvailableConnectionLifecycleSlot() const noexcept;
        [[nodiscard]] int getEpollWaitTimeout() const;
        [[nodiscard]] snf::net::Session* findCurrentSession(snf::net::ConnectionId connection);

        snf::net::UniqueFileDescriptor _listener;
        snf::net::UniqueFileDescriptor _epoll;
        snf::net::UniqueFileDescriptor _stop_event;
        std::uint16_t _port;
        std::chrono::milliseconds _shutdown_grace_period;
        std::size_t _max_pending_send_bytes;
        std::optional<int> _client_send_buffer_size;
        std::size_t _connection_lifecycle_capacity;
        FrameIngress& _frame_ingress;
        snf::runtime::BoundedQueue<OutboundAction>& _outbound_actions;
        RuntimeCompletionSource& _runtime_completion;
        int _outbound_event_descriptor;
        std::chrono::steady_clock::time_point _shutdown_deadline{};
        std::uint64_t _next_connection_generation{0};
        bool _is_stopping{false};
        bool _game_runtimes_drained{false};
        std::unordered_map<int, snf::net::Session> _sessions;
        // epoll may return a copied event after its FD has been closed and reused.
        // Generation tokens let the reactor discard that event instead of targeting the new FD.
        std::unordered_map<std::uint64_t, int> _client_descriptors_by_event_token;
        std::deque<ConnectionClosed> _pending_connection_closes;
        TcpServerStats _stats;
    };
}
