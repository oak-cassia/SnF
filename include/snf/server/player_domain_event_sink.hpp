#pragma once

#include "snf/server/player_domain_event.hpp"

namespace snf::server
{
    enum class PlayerEventPublishResult
    {
        Published,
        Duplicate,
        Conflict,
        OutOfOrder,
        Full,
    };

    class PlayerDomainEventSink
    {
    public:
        virtual ~PlayerDomainEventSink() = default;
        [[nodiscard]] virtual PlayerEventPublishResult publish(PlayerDomainEvent event) = 0;
    };
}
