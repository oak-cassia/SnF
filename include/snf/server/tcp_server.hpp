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
        // A slot is retained until a disconnected session's lifecycle fact is
        // accepted or the ingress closes. This bounds both active sessions and
        // the reactor-owned retry deque without dropping ConnectionClosed.
        std::size_t connection_lifecycle_capacity{4096};
        // Zero disables periodic reporting. Otherwise the reactor bounds its
        // epoll timeout by this interval so a quiet server still reports.
        std::chrono::milliseconds metrics_report_interval{0};
        // Invoked on the reactor thread when the interval elapses, so it must not
        // block: its cost is reactor downtime for every connection. Shipping to a
        // file or a collector belongs on a separate bounded logger queue that this
        // hook only posts to.
        //
        // It runs after the turn has been measured and is therefore absent from
        // reactor_turn_nanoseconds. That metric is reactor work per turn, not full
        // reactor occupancy.
        std::function<void()> on_metrics_interval{};
    };

    struct TcpServerStats
    {
        std::uint64_t accepted_connections{0};
        std::uint64_t closed_connections{0};
        std::uint64_t received_frames{0};
        std::uint64_t sent_frames{0};
        std::uint64_t protocol_errors{0};
        std::uint64_t actor_queue_overflows{0};
        // Connections closed because a Logic Worker could not even be admitted to
        // wait for outbound capacity. Counted apart from actor_queue_overflows: the
        // saturated resource is the outbound channel, not a command queue.
        std::uint64_t outbound_admission_failures{0};
        // Record-budget/allocation fail-safes. Each occurrence closes every current
        // session rather than losing an admission-failure close or failing a Logic
        // Worker. Non-zero means the configured record bound deserves investigation.
        std::uint64_t outbound_admission_failure_fallbacks{0};
        std::uint64_t stale_outbound_actions{0};
        std::uint64_t connection_lifecycle_rejections{0};
        std::size_t pending_connection_closes_high_water_mark{0};
    };

    // Saturation and latency samples owned by the reactor. Distributions are
    // cumulative for the reactor lifetime so that two runs stay comparable, and
    // the remaining fields are gauges read at snapshot time.
    struct TcpServerMetrics
    {
        // Time from an epoll_wait return to the end of that turn's event
        // handling. A turn without a ready event is not sampled, so the idle
        // wait itself never enters the distribution.
        snf::runtime::DistributionSnapshot reactor_turn_nanoseconds;
        // One connection's pending send bytes, sampled after each enqueue.
        snf::runtime::DistributionSnapshot session_pending_send_bytes;
        // Outbound queue depth observed before each drain.
        snf::runtime::DistributionSnapshot outbound_queue_depth;
        // Nanoseconds from a Logic Worker committing an OutboundAction to the reactor
        // consuming it. Since stage 4.1 a Worker no longer blocks on a full queue, so
        // this covers the hand-off alone; the wait for capacity shows up as the
        // actor's suspension in ActorRuntimeWorkerStats instead. Comparing it against
        // the 3.9 baseline therefore compares two different quantities, and both
        // halves have to be reported together.
        snf::runtime::DistributionSnapshot outbound_queue_wait_nanoseconds;
        std::size_t session_count{0};
        std::size_t sessions_with_pending_send{0};
        std::size_t total_pending_send_bytes{0};
        std::size_t current_outbound_queue_depth{0};
        std::size_t outbound_queue_high_water_mark{0};
        // Capacity granted but not yet emitted, and actors suspended waiting for a
        // grant. Together with the depth above they account for the whole channel.
        std::size_t reserved_outbound_slots{0};
        std::size_t pending_outbound_reservations{0};
        // Connections the channel still accounts for. It must follow the live
        // connection count, not the command rate.
        std::size_t tracked_outbound_connections{0};
    };

    // The epoll reactor. It decodes frames, submits FrameEnvelope values to the shared
    // protocol gateway, and applies OutboundAction values on the reactor thread.
    class TcpServer
    {
    public:
        TcpServer(const TcpServerConfig& config,
                  FrameIngress& frame_ingress,
                  OutboundChannel& outbound,
                  snf::runtime::RuntimeCompletionSource& runtime_completion,
                  int outbound_event_descriptor);

        TcpServer(const TcpServer&) = delete;
        TcpServer& operator=(const TcpServer&) = delete;

        TcpServer(TcpServer&&) = delete;
        TcpServer& operator=(TcpServer&&) = delete;

        [[nodiscard]] std::uint16_t getPort() const noexcept;
        [[nodiscard]] const TcpServerStats& getStats() const noexcept;
        // Reads the session map, so it belongs to the reactor thread: call it
        // from on_metrics_interval or after run() has returned.
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
        // Closing here needs no outbound capacity, which is what makes it usable as
        // the policy for a connection that could not obtain any.
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
        void reportMetricsIfDue();
        [[nodiscard]] bool hasAvailableConnectionLifecycleSlot() const noexcept;
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
        FrameIngress& _frame_ingress;
        OutboundChannel& _outbound;
        snf::runtime::RuntimeCompletionSource& _runtime_completion;
        int _outbound_event_descriptor;
        std::chrono::steady_clock::time_point _shutdown_deadline{};
        std::chrono::steady_clock::time_point _next_metrics_report{};
        std::uint64_t _next_connection_generation{0};
        bool _is_stopping{false};
        bool _logic_runtime_drained{false};
        std::unordered_map<int, snf::net::Session> _sessions;
        // epoll may return a copied event after its FD has been closed and reused.
        // Generation tokens let the reactor discard that event instead of targeting the new FD.
        std::unordered_map<std::uint64_t, int> _client_descriptors_by_event_token;
        std::deque<ConnectionClosed> _pending_connection_closes;
        // Both reused across turns so a drain allocates nothing per turn.
        std::vector<PostedOutboundAction> _drained_outbound_actions;
        std::vector<snf::net::ConnectionId> _failed_outbound_admissions;
        TcpServerStats _stats;
        snf::runtime::Distribution _reactor_turn_nanoseconds;
        snf::runtime::Distribution _session_pending_send_bytes;
        snf::runtime::Distribution _outbound_queue_depth;
        snf::runtime::Distribution _outbound_queue_wait_nanoseconds;
    };
}
