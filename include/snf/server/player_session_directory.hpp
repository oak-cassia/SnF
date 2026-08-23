#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/player_location.hpp"
#include "snf/net/connection_id.hpp"

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

    struct PlayerLocationSnapshot
    {
        bool known{false};
        std::optional<PlayerLocation> location;
    };

    class PlayerSessionDirectory
    {
    public:
        [[nodiscard]] PlayerAttachResult tryAttach(snf::net::ConnectionId connection, PlayerId player);
        void rollbackAttach(snf::net::ConnectionId connection, PlayerId player) noexcept;

        [[nodiscard]] bool noteProvisionalActivity(snf::net::ConnectionId connection);
        void clearProvisionalActivity(snf::net::ConnectionId connection) noexcept;

        [[nodiscard]] std::optional<PlayerId> playerFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] std::optional<snf::net::ConnectionId> connectionFor(PlayerId player) const;
        [[nodiscard]] std::optional<PlayerLocation> locationFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] PlayerLocationSnapshot locationSnapshotFor(snf::net::ConnectionId connection) const;
        void noteLocation(snf::net::ConnectionId connection, std::optional<PlayerLocation> location) noexcept;

        [[nodiscard]] bool beginClose(snf::net::ConnectionId connection) noexcept;
        void rollbackClose(snf::net::ConnectionId connection) noexcept;
        void completePassivation(PlayerId player, snf::net::ConnectionId connection) noexcept;
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
            bool location_known{false};
            std::optional<PlayerLocation> last_location;
        };

        mutable std::mutex _mutex;
        std::unordered_map<snf::net::ConnectionId, Session, snf::net::ConnectionIdHash> _sessions_by_connection;
        std::unordered_map<PlayerId, snf::net::ConnectionId, PlayerIdHash> _connections_by_player;
        std::unordered_set<snf::net::ConnectionId, snf::net::ConnectionIdHash> _provisional_activity;
    };
}
