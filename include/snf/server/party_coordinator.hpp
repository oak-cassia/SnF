#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/party_id.hpp"
#include "snf/server/player_id.hpp"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace snf::server
{
    struct PartyRoute
    {
        snf::net::ConnectionId connection;
        PlayerId player;
        PartyId party;
        std::uint64_t membership_epoch{0};
        bool leaving{false};

        [[nodiscard]] bool operator==(const PartyRoute&) const noexcept = default;
    };

    struct PartyAdmission
    {
        PartyRoute route;
        bool created{false};
        // The route was not published, but a Join command may still be posted so
        // PartyActor returns a typed PartyFull result instead of a protocol error.
        bool capacity_denied{false};
    };

    // Reactor-facing authoritative membership route. It prevents one session from
    // joining multiple parties and admits no more routes than PartyActor can hold.
    class PartyCoordinator
    {
    public:
        explicit PartyCoordinator(std::size_t max_members = 8);

        [[nodiscard]] std::optional<PartyAdmission> tryJoin(snf::net::ConnectionId connection, PlayerId player, PartyId party);
        void rollbackJoin(const PartyAdmission& admission) noexcept;
        [[nodiscard]] std::optional<PartyRoute> routeFor(snf::net::ConnectionId connection) const;
        [[nodiscard]] std::size_t routeCountFor(PartyId party) const;
        [[nodiscard]] std::optional<PartyRoute> beginLeave(snf::net::ConnectionId connection) noexcept;
        void rollbackLeave(const PartyRoute& route) noexcept;
        void completeLeave(const PartyRoute& route) noexcept;
        void abandon(snf::net::ConnectionId connection) noexcept;

    private:
        std::size_t routeCountForLocked(PartyId party) const;

        const std::size_t _max_members;
        mutable std::mutex _mutex;
        std::unordered_map<snf::net::ConnectionId, PartyRoute, snf::net::ConnectionIdHash> _routes;
        std::unordered_map<PlayerId, std::uint64_t, PlayerIdHash> _last_epoch;
    };
}
