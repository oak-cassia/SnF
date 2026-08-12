#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/player_id.hpp"

#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace snf::server
{
    enum class PlayerAttachResult
    {
        Attached,
        AlreadyAttached,
        ConnectionConflict,
        PlayerConflict,
        ProvisionalActivity,
        Closing,
    };

    // Owns the one-live-session policy for persistent players. Gateway methods
    // normally run on the reactor thread; the mutex is needed because an Actor
    // slot reports actual passivation from its owning Worker.
    class PlayerSessionDirectory
    {
    public:
        [[nodiscard]] PlayerAttachResult tryAttach(snf::net::ConnectionId connection,
                                                   PlayerId player);
        void rollbackAttach(snf::net::ConnectionId connection, PlayerId player) noexcept;

        [[nodiscard]] bool noteProvisionalActivity(snf::net::ConnectionId connection);
        void clearProvisionalActivity(snf::net::ConnectionId connection) noexcept;

        [[nodiscard]] std::optional<PlayerId> playerFor(snf::net::ConnectionId connection) const;

        // Closing retains both indexes until the owning Worker has removed and
        // destroyed the Actor slot. This prevents a reconnect command from being
        // queued behind the old close and discarded by that close's eviction.
        [[nodiscard]] bool beginClose(snf::net::ConnectionId connection) noexcept;
        void rollbackClose(snf::net::ConnectionId connection) noexcept;
        void completePassivation(PlayerId player) noexcept;
        void abandon(snf::net::ConnectionId connection) noexcept;

    private:
        enum class State
        {
            Active,
            Closing,
        };

        struct Session
        {
            PlayerId player;
            State state{State::Active};
        };

        mutable std::mutex _mutex;
        std::unordered_map<snf::net::ConnectionId, Session, snf::net::ConnectionIdHash>
            _sessions_by_connection;
        std::unordered_map<PlayerId, snf::net::ConnectionId, PlayerIdHash> _connections_by_player;
        std::unordered_set<snf::net::ConnectionId, snf::net::ConnectionIdHash>
            _provisional_activity;
    };
}
