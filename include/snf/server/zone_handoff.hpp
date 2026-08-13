#pragma once

#include "snf/net/connection_id.hpp"
#include "snf/server/player_id.hpp"
#include "snf/server/zone_command.hpp"
#include "snf/server/zone_id.hpp"
#include "snf/server/zone_result.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    struct ZoneHandoffId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const ZoneHandoffId&) const noexcept = default;
    };

    struct ZoneTransitionTicket
    {
        std::uint64_t value{0};

        [[nodiscard]] bool valid() const noexcept
        {
            return value != 0;
        }

        [[nodiscard]] bool operator==(const ZoneTransitionTicket&) const noexcept = default;
    };

    enum class ZoneHandoffStep
    {
        LeaveSource,
        EnterTarget,
        RestoreSource,
        CleanupTarget,
    };

    // Immutable routing identity carried by one internal Zone submission. It is
    // deliberately separate from the gameplay command so ZoneActor never sees
    // connection, ticket, or reactor transition state.
    struct ZoneHandoffContext
    {
        ZoneHandoffId handoff_id;
        ZoneTransitionTicket ticket;
        snf::net::ConnectionId connection;
        PlayerId player;
        ZoneHandoffStep step{ZoneHandoffStep::LeaveSource};
        std::uint64_t route_epoch{0};
    };

    struct ZoneHandoffCompletion
    {
        ZoneHandoffId handoff_id;
        snf::net::ConnectionId connection;
        PlayerId player;
        ZoneId zone;
        std::uint64_t route_epoch{0};
        ZoneHandoffStep step{ZoneHandoffStep::LeaveSource};
        ZoneCommandStatus status{ZoneCommandStatus::Applied};
        std::optional<ZonePosition> position;
    };
}
