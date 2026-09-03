#pragma once

#include "snf/adapter/game_payloads.hpp"
#include "snf/game/player_command.hpp"
#include "snf/game/player_id.hpp"
#include "snf/worker/request_sink.hpp"
#include "snf/worker/worker.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

namespace snf::adapter
{
    // Frame routing for the Worker runtime. One instance per Worker, called only
    // on that Worker's owner thread, so the session table below is a plain map.
    //
    // It owns one direction of the session identity: connection -> player. The
    // reverse direction lives in the PlayerActor, which holds the ConnectionRef
    // it is bound to. Splitting it this way replaces the legacy
    // PlayerSessionDirectory, whose single mutex-guarded map cannot exist in a
    // runtime where each Worker owns its own connections.
    class GameRequestSink final : public snf::worker::RequestSink
    {
    public:
        GameRequestSink() = default;
        explicit GameRequestSink(snf::worker::Worker& worker) noexcept
            : _worker(&worker)
        {
        }

        void setWorker(snf::worker::Worker& worker) noexcept
        {
            _worker = &worker;
        }

        [[nodiscard]] snf::worker::RequestPostResult tryPost(snf::worker::ConnectionRef connection, snf::protocol::Frame&& frame) override;

        void onConnectionClosed(snf::worker::ConnectionRef connection, snf::worker::CloseReason reason) override;

        // Live session count, bounded by the connection table because an entry
        // only exists while its connection is open. This mirror is atomic so it
        // can be observed while the Worker runs; the map itself stays
        // owner-thread-only and must never be read from another thread.
        [[nodiscard]] std::size_t sessionCount() const noexcept
        {
            return _live_sessions.load(std::memory_order_relaxed);
        }

        // Owner thread only.
        [[nodiscard]] std::optional<snf::server::PlayerId> playerFor(snf::worker::ConnectionRef connection) const;

    private:
        // Pins the owner thread. Sharing one sink between Workers would otherwise
        // corrupt _sessions silently instead of failing.
        void assertOwnerThread() noexcept;

        using PlayerCommandDecoder = std::optional<snf::server::PlayerCommand> (*)(const std::vector<std::byte>&);
        using ZoneRequestDecoder = std::optional<ZoneRequest> (*)(const std::vector<std::byte>&);
        using RoomRequestDecoder = std::optional<RoomRequest> (*)(const std::vector<std::byte>&);

        [[nodiscard]] snf::worker::RequestPostResult postAuthenticate(snf::worker::ConnectionRef connection, const snf::protocol::Frame& frame);
        [[nodiscard]] snf::worker::RequestPostResult postPing(snf::worker::ConnectionRef connection, snf::protocol::Frame&& frame);
        [[nodiscard]] snf::worker::RequestPostResult postPlayerCommand(
            snf::worker::ConnectionRef connection,
            const snf::protocol::Frame& frame,
            PlayerCommandDecoder decoder
        );
        [[nodiscard]] snf::worker::RequestPostResult postZoneRequest(
            snf::worker::ConnectionRef connection,
            const snf::protocol::Frame& frame,
            ZoneRequestDecoder decoder
        );
        [[nodiscard]] snf::worker::RequestPostResult postRoomRequest(
            snf::worker::ConnectionRef connection,
            const snf::protocol::Frame& frame,
            RoomRequestDecoder decoder
        );

        // Keyed by connection id, but the generation is stored and verified on
        // every lookup. A connection slot is reused with a new generation, so a
        // stale entry that outlived its close notification fails closed instead
        // of handing the new connection the previous player's session.
        struct Session
        {
            snf::worker::ConnectionGeneration generation{};
            snf::server::PlayerId player{};
        };

        snf::worker::Worker* _worker{nullptr};
        std::unordered_map<std::uint32_t, Session> _sessions;
        // Updated once per session bind and release, never per frame.
        std::atomic<std::size_t> _live_sessions{0};
        std::thread::id _owner_thread{};
    };
}
