#pragma once

#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/command_router.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/outbound_channel.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_actor_ingress.hpp"
#include "snf/server/protocol_gateway.hpp"
#include "snf/server/protocol_player_effect_sink.hpp"
#include "snf/server/tcp_server.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <optional>
#include <thread>

namespace snf::server
{
    using GameServerStats = TcpServerStats;

    // The baseline observation surface for the backpressure contract: reactor
    // saturation, per-connection send buildup and Logic Worker queue wait in one
    // consistent read.
    struct ServerMetricsSnapshot
    {
        GameServerStats counters;
        TcpServerMetrics network;
        snf::runtime::ActorRuntimeStats actor_runtime;
        // Commands that were admitted and reached a final result, counted once each
        // whether or not they answered. Phase 4.5 replaces the counting consumer with
        // the credit owner.
        std::uint64_t command_terminals{0};
        // Posts the runtime refused. A different fact with a different cause, so it is
        // never folded into the count above.
        std::uint64_t command_admission_rejections{0};
    };

    struct GameServerConfig
    {
        std::uint16_t port{7777};
        std::chrono::milliseconds shutdown_grace_period{5000};
        std::size_t max_pending_send_bytes{snf::net::MAX_PENDING_SEND_BYTES};
        std::optional<int> client_send_buffer_size{};
        std::size_t actor_worker_count{2};
        std::size_t actor_queue_capacity_per_worker{4096};
        // Also the continuation queue capacity per Worker, and the bound the outbound
        // waiter registry is sized against: a Worker holds one of these while it waits
        // for capacity, so registration must never be the tighter limit.
        std::size_t actor_max_in_flight_operations_per_worker{1024};
        std::size_t outbound_queue_capacity{4096};
        // Bounds the shared outbound capacity one connection may hold at once.
        std::size_t max_outbound_slots_per_connection{64};
        // Bounds the waiters a single reactor turn examines and grants.
        std::size_t outbound_grants_per_turn{64};
        std::size_t connection_lifecycle_capacity{4096};
        // Periodic exposure while the server runs. Zero reports nothing, leaving
        // only the caller's own snapshot after run() returns.
        std::chrono::milliseconds metrics_report_interval{0};
        // Called on the reactor thread, so it must not block. Anything slower than
        // a local formatting pass belongs on a separate bounded logger queue that
        // the reporter only posts to. Its cost is not part of
        // TcpServerMetrics::reactor_turn_nanoseconds.
        std::function<void(const ServerMetricsSnapshot&)> metrics_reporter{};
    };

    // Composes the reactor, Logic ActorRuntime, Player binding, outbound hand-off
    // queue and eventfd wake-up.
    class GameServer
    {
    public:
        explicit GameServer(const GameServerConfig& config);
        explicit GameServer(
            std::uint16_t port,
            std::chrono::milliseconds shutdown_grace_period = std::chrono::milliseconds{5000});
        ~GameServer();

        GameServer(const GameServer&) = delete;
        GameServer& operator=(const GameServer&) = delete;

        GameServer(GameServer&&) = delete;
        GameServer& operator=(GameServer&&) = delete;

        [[nodiscard]] std::uint16_t getPort() const noexcept;
        [[nodiscard]] const GameServerStats& getStats() const noexcept;
        [[nodiscard]] snf::runtime::ActorRuntimeStats getActorRuntimeStats() const;
        // Reads reactor state, so it belongs to the reactor thread: call it from
        // metrics_reporter or after run() has returned.
        [[nodiscard]] ServerMetricsSnapshot getMetricsSnapshot() const;

        void run(int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD);
        void requestStop() const noexcept;

    private:
        void startActorRuntime();
        void joinActorRuntime();
        void cancelActorRuntime() noexcept;
        void publishMetrics() const;

        std::function<void(const ServerMetricsSnapshot&)> _metrics_reporter;
        // The channel signals this descriptor, so it has to outlive the channel.
        snf::net::UniqueFileDescriptor _outbound_event;
        // The domain side takes it as an OutboundSink&, so the binding and the effect
        // sink still cannot reach the reactor-only half.
        OutboundChannel _outbound_channel;
        ProtocolPlayerEffectSink _player_effects;
        CountingCommandLifecycleSink _command_lifecycle;
        snf::runtime::RuntimeCompletionCoordinator _runtime_completion;
        // Bindings must outlive the generic runtime: the worker owns the
        // wrapper destruction, while this object owns Player dependencies.
        PlayerActorBinding _player_actor_binding;
        snf::runtime::ActorRuntime _logic_runtime;
        PlayerActorIngress _player_actor_ingress;
        CommandRouter _command_router;
        ProtocolGateway _protocol_gateway;
        TcpServer _tcp_server;
        bool _has_run{false};
    };
}
