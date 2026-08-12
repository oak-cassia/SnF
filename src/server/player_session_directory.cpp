#include "snf/server/player_session_directory.hpp"

namespace snf::server
{
    PlayerAttachResult PlayerSessionDirectory::tryAttach(const snf::net::ConnectionId connection,
                                                         const PlayerId player)
    {
        std::lock_guard lock{_mutex};

        if (_provisional_activity.contains(connection))
        {
            return PlayerAttachResult::ProvisionalActivity;
        }

        if (const auto connection_iterator = _sessions_by_connection.find(connection);
            connection_iterator != _sessions_by_connection.end())
        {
            if (connection_iterator->second.state == State::Closing)
            {
                return PlayerAttachResult::Closing;
            }

            return connection_iterator->second.player == player
                       ? PlayerAttachResult::AlreadyAttached
                       : PlayerAttachResult::ConnectionConflict;
        }

        if (_connections_by_player.contains(player))
        {
            return PlayerAttachResult::PlayerConflict;
        }

        _sessions_by_connection.emplace(connection,
                                        Session{.player = player, .state = State::Active});
        try
        {
            _connections_by_player.emplace(player, connection);
        }
        catch (...)
        {
            _sessions_by_connection.erase(connection);
            throw;
        }

        return PlayerAttachResult::Attached;
    }

    void PlayerSessionDirectory::rollbackAttach(const snf::net::ConnectionId connection,
                                                const PlayerId player) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto connection_iterator = _sessions_by_connection.find(connection);
        if (connection_iterator == _sessions_by_connection.end() ||
            connection_iterator->second.player != player ||
            connection_iterator->second.state != State::Active)
        {
            return;
        }

        _sessions_by_connection.erase(connection_iterator);
        _connections_by_player.erase(player);
    }

    bool PlayerSessionDirectory::noteProvisionalActivity(const snf::net::ConnectionId connection)
    {
        std::lock_guard lock{_mutex};
        if (_sessions_by_connection.contains(connection))
        {
            return false;
        }

        return _provisional_activity.insert(connection).second;
    }

    void PlayerSessionDirectory::clearProvisionalActivity(
        const snf::net::ConnectionId connection) noexcept
    {
        std::lock_guard lock{_mutex};
        _provisional_activity.erase(connection);
    }

    std::optional<PlayerId>
    PlayerSessionDirectory::playerFor(const snf::net::ConnectionId connection) const
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _sessions_by_connection.find(connection);
        if (iterator == _sessions_by_connection.end())
        {
            return std::nullopt;
        }

        return iterator->second.player;
    }

    bool PlayerSessionDirectory::beginClose(const snf::net::ConnectionId connection) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _sessions_by_connection.find(connection);
        if (iterator == _sessions_by_connection.end())
        {
            return false;
        }

        iterator->second.state = State::Closing;
        return true;
    }

    void PlayerSessionDirectory::rollbackClose(const snf::net::ConnectionId connection) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _sessions_by_connection.find(connection);
        if (iterator != _sessions_by_connection.end())
        {
            iterator->second.state = State::Active;
        }
    }

    void PlayerSessionDirectory::completePassivation(const PlayerId player) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto player_iterator = _connections_by_player.find(player);
        if (player_iterator == _connections_by_player.end())
        {
            return;
        }

        const snf::net::ConnectionId connection = player_iterator->second;
        const auto connection_iterator = _sessions_by_connection.find(connection);
        if (connection_iterator == _sessions_by_connection.end() ||
            connection_iterator->second.state != State::Closing)
        {
            return;
        }

        _sessions_by_connection.erase(connection_iterator);
        _connections_by_player.erase(player_iterator);
    }

    void PlayerSessionDirectory::abandon(const snf::net::ConnectionId connection) noexcept
    {
        std::lock_guard lock{_mutex};
        _provisional_activity.erase(connection);

        const auto connection_iterator = _sessions_by_connection.find(connection);
        if (connection_iterator == _sessions_by_connection.end())
        {
            return;
        }

        _connections_by_player.erase(connection_iterator->second.player);
        _sessions_by_connection.erase(connection_iterator);
    }
}
