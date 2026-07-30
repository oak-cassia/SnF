#pragma once

#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/bounded_queue.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/command_router.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_actor_binding.hpp"
#include "snf/server/player_actor_ingress.hpp"
#include "snf/server/protocol_gateway.hpp"
#include "snf/server/protocol_player_effect_sink.hpp"
#include "snf/server/tcp_server.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <thread>

namespace snf::server
{
    struct GameServerConfig
    {
        std::uint16_t port{7777};
        std::chrono::milliseconds shutdown_grace_period{5000};
        std::size_t max_pending_send_bytes{snf::net::MAX_PENDING_SEND_BYTES};
        std::optional<int> client_send_buffer_size;
        std::size_t actor_worker_count{2};
        std::size_t actor_queue_capacity_per_worker{4096};
        std::size_t outbound_queue_capacity{4096};
        std::size_t connection_lifecycle_capacity{4096};
    };

    using GameServerStats = TcpServerStats;

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

        void run(int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD);
        void requestStop() const noexcept;

    private:
        void startActorRuntime();
        void joinActorRuntime();
        void cancelActorRuntime() noexcept;

        snf::runtime::BoundedQueue<OutboundAction> _outbound_actions;
        snf::net::UniqueFileDescriptor _outbound_event;
        EventFdOutboundSink _outbound_sink;
        ProtocolPlayerEffectSink _player_effects;
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
