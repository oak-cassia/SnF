#include "snf/server/party_coordinator.hpp"

#include <limits>
#include <stdexcept>

namespace snf::server
{
    PartyCoordinator::PartyCoordinator(const std::size_t max_members)
        : _max_members(max_members)
    {
        if (_max_members == 0)
        {
            throw std::invalid_argument{"Party member capacity must be positive"};
        }
    }

    std::optional<PartyAdmission> PartyCoordinator::tryJoin(const snf::net::ConnectionId connection, const PlayerId player, const PartyId party)
    {
        if (player.value == 0 || party.value == 0)
        {
            return std::nullopt;
        }

        std::lock_guard lock{_mutex};
        const auto next_epoch = [this, player]
        {
            std::uint64_t& last_epoch = _last_epoch[player];
            if (last_epoch == std::numeric_limits<std::uint64_t>::max())
            {
                throw std::overflow_error{"Party membership epoch exhausted"};
            }
            return ++last_epoch;
        };
        const auto existing = _routes.find(connection);
        if (existing != _routes.end())
        {
            if (existing->second.leaving || existing->second.player != player || existing->second.party != party)
            {
                return std::nullopt;
            }
            return PartyAdmission{
                .route = existing->second,
                .created = false,
                .capacity_denied = false,
            };
        }
        for (const auto& [active_connection, route] : _routes)
        {
            if (route.player == player && active_connection != connection)
            {
                return std::nullopt;
            }
        }
        if (routeCountForLocked(party) >= _max_members)
        {
            return PartyAdmission{
                .route =
                    PartyRoute{
                        .connection = connection,
                        .player = player,
                        .party = party,
                        .membership_epoch = next_epoch(),
                        .leaving = false,
                    },
                .created = false,
                .capacity_denied = true,
            };
        }

        const PartyRoute route{
            .connection = connection,
            .player = player,
            .party = party,
            .membership_epoch = next_epoch(),
            .leaving = false,
        };
        _routes.emplace(connection, route);
        return PartyAdmission{
            .route = route,
            .created = true,
            .capacity_denied = false,
        };
    }

    void PartyCoordinator::rollbackJoin(const PartyAdmission& admission) noexcept
    {
        if (!admission.created)
        {
            return;
        }

        std::lock_guard lock{_mutex};
        const auto iterator = _routes.find(admission.route.connection);
        if (iterator != _routes.end() && iterator->second == admission.route)
        {
            _routes.erase(iterator);
        }
    }

    std::optional<PartyRoute> PartyCoordinator::routeFor(const snf::net::ConnectionId connection) const
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _routes.find(connection);
        return iterator == _routes.end() ? std::nullopt : std::optional{iterator->second};
    }

    std::size_t PartyCoordinator::routeCountFor(const PartyId party) const
    {
        std::lock_guard lock{_mutex};
        return routeCountForLocked(party);
    }

    std::optional<PartyRoute> PartyCoordinator::beginLeave(const snf::net::ConnectionId connection) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _routes.find(connection);
        if (iterator == _routes.end() || iterator->second.leaving)
        {
            return std::nullopt;
        }
        iterator->second.leaving = true;
        return iterator->second;
    }

    void PartyCoordinator::rollbackLeave(const PartyRoute& route) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _routes.find(route.connection);
        if (iterator != _routes.end() && iterator->second == route)
        {
            iterator->second.leaving = false;
        }
    }

    void PartyCoordinator::completeLeave(const PartyRoute& route) noexcept
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _routes.find(route.connection);
        if (iterator != _routes.end() && iterator->second == route)
        {
            _routes.erase(iterator);
        }
    }

    void PartyCoordinator::abandon(const snf::net::ConnectionId connection) noexcept
    {
        std::lock_guard lock{_mutex};
        _routes.erase(connection);
    }

    std::size_t PartyCoordinator::routeCountForLocked(const PartyId party) const
    {
        std::size_t count = 0;
        for (const auto& [connection, route] : _routes)
        {
            static_cast<void>(connection);
            // A leave has already been accepted ahead of any subsequent join to
            // this Party mailbox, so its slot may be reused immediately without
            // exceeding Party capacity when commands execute in FIFO order.
            if (route.party == party && !route.leaving)
            {
                ++count;
            }
        }
        return count;
    }
}
