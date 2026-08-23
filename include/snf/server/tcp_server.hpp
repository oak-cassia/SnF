#pragma once

#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/distribution.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/frame_ingress.hpp"
#include "snf/server/outbound_action.hpp"
#include "snf/server/outbound_channel.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

namespace snf::server
{
    struct TcpServerConfig
    {
        std::uint16_t port{7777};
        std::chrono::milliseconds shutdown_grace_period{5000};
        std::size_t max_pending_send_bytes{snf::net::MAX_PENDING_SEND_BYTES};
        std::optional<int> client_send_buffer_size{};
        std::size_t connection_lifecycle_capacity{4096};
        std::chrono::milliseconds metrics_report_interval{0};
        std::function<void()> on_metrics_interval{};
        std::function<void()> on_control_wake{};
        std::function<bool()> is_control_drained{};
    };

    struct TcpServerStats
    {
        std::uint64_t accepted_connections{0};
        std::uint64_t closed_connections{0};
        std::uint64_t received_frames{0};
        std::uint64_t sent_frames{0};
        std::uint64_t protocol_errors{0};
        std::uint64_t actor_queue_overflows{0};
        std::uint64_t outbound_admission_failures{0};
        std::uint64_t outbound_admission_failure_fallbacks{0};
        std::uint64_t stale_outbound_actions{0};
        std::uint64_t connection_lifecycle_rejections{0};
        std::size_t pending_connection_closes_high_water_mark{0};
    };

    struct TcpServerMetrics
    {
        snf::runtime::DistributionSnapshot reactor_turn_nanoseconds;
        snf::runtime::DistributionSnapshot session_pending_send_bytes;
        snf::runtime::DistributionSnapshot outbound_queue_depth;
        snf::runtime::DistributionSnapshot outbound_queue_wait_nanoseconds;
        std::size_t session_count{0};
        std::size_t sessions_with_pending_send{0};
        std::size_t total_pending_send_bytes{0};
        std::size_t current_outbound_queue_depth{0};
        std::size_t outbound_queue_high_water_mark{0};
        std::size_t reserved_outbound_slots{0};
        std::size_t pending_outbound_reservations{0};
        std::size_t tracked_outbound_connections{0};
    };

    class TcpServer
    {
    public:
        TcpServer(const TcpServerConfig& config, FrameIngress& frame_ingress, OutboundChannel& outbound, snf::runtime::RuntimeCompletionSource& runtime_completion, int outbound_event_descriptor);

        TcpServer(const TcpServer&) = delete;
        TcpServer& operator=(const TcpServer&) = delete;

        TcpServer(TcpServer&&) = delete;
        TcpServer& operator=(TcpServer&&) = delete;

        [[nodiscard]] std::uint16_t getPort() const noexcept;
        [[nodiscard]] const TcpServerStats& getStats() const noexcept;
        [[nodiscard]] TcpServerMetrics getMetrics() const;

        void run(int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD);
        void requestStop() const noexcept;

    private:
        void registerListener() const;
        void registerControlDescriptor(int descriptor, std::uint64_t event_token) const;
        void acceptPendingClients();
        void handleClientEvent(int client_descriptor, std::uint32_t event_flags);
        void handleOutboundActions();
        void handleOutboundAction(OutboundAction action);
        void closeConnectionsWithFailedOutboundAdmission();
        void handleRuntimeCompletion();
        void handleStopRequest();
        void handleTerminationSignal(int signal_descriptor);
        void beginShutdown();
        void completeShutdownAfterLogicRuntimeDrained();
        void abortShutdownAfterLogicRuntimeFailure();
        void cancelQueues();
        [[nodiscard]] bool flushPendingSend(snf::net::Session& session);
        void updateClientEvents(const snf::net::Session& session) const;
        void removeSession(int client_descriptor, ConnectionCloseCause cause);
        void closeRemainingSessions();
        void notifyConnectionClosed(ConnectionClosed closed);
        void retryPendingConnectionCloses();
        void closeFrameIngressAfterConnectionLifecyclesDrain();
        void reportMetricsIfDue();
        [[nodiscard]] bool hasAvailableConnectionLifecycleSlot() const noexcept;
        [[nodiscard]] bool isControlDrained() const noexcept;
        [[nodiscard]] bool hasMetricsReporting() const noexcept;
        [[nodiscard]] bool hasShutdownDeadlineExpired() const noexcept;
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
        std::chrono::milliseconds _metrics_report_interval;
        std::function<void()> _on_metrics_interval;
        std::function<void()> _on_control_wake;
        std::function<bool()> _is_control_drained;
        FrameIngress& _frame_ingress;
        OutboundChannel& _outbound;
        snf::runtime::RuntimeCompletionSource& _runtime_completion;
        int _outbound_event_descriptor;
        std::chrono::steady_clock::time_point _shutdown_deadline{};
        std::chrono::steady_clock::time_point _next_metrics_report{};
        std::uint64_t _next_connection_generation{0};
        bool _is_stopping{false};
        bool _frame_ingress_closed{false};
        bool _logic_runtime_drained{false};
        std::unordered_map<int, snf::net::Session> _sessions;
        std::unordered_map<std::uint64_t, int> _client_descriptors_by_event_token;
        std::deque<ConnectionClosed> _pending_connection_closes;
        std::vector<PostedOutboundAction> _drained_outbound_actions;
        std::vector<snf::net::ConnectionId> _failed_outbound_admissions;
        TcpServerStats _stats;
        snf::runtime::Distribution _reactor_turn_nanoseconds;
        snf::runtime::Distribution _session_pending_send_bytes;
        snf::runtime::Distribution _outbound_queue_depth;
        snf::runtime::Distribution _outbound_queue_wait_nanoseconds;
    };
}
