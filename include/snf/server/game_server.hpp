#pragma once

#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/game_runtime.hpp"
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
        std::size_t inbound_queue_capacity{4096};
        std::size_t outbound_queue_capacity{4096};
    };

    using GameServerStats = TcpServerStats;

    // Composes the reactor, game worker, bounded hand-off queues and their eventfd wake-up.
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

        void run(int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD);
        void requestStop() const noexcept;

    private:
        void startGameRuntime();
        void joinGameRuntime();
        void cancelGameRuntime() noexcept;

        snf::runtime::BoundedQueue<InboundCommand> _inbound_commands;
        snf::runtime::BoundedQueue<NetworkAction> _network_actions;
        snf::net::UniqueFileDescriptor _outbound_event;
        TcpServer _tcp_server;
        GameRuntime _game_runtime;
        std::jthread _game_worker;
        std::exception_ptr _game_runtime_error;
        bool _has_run{false};
    };
}
