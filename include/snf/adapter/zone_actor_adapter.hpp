#pragma once

#include "snf/game/zone.hpp"
#include "snf/worker/actor.hpp"

namespace snf::adapter
{
    class ZoneActorAdapter final : public snf::worker::ActorInstance
    {
    public:
        explicit ZoneActorAdapter(snf::server::ZoneId zone_id, snf::server::ZoneConfig config = {})
            : _zone(zone_id, config)
        {
        }

        [[nodiscard]] snf::server::Zone& zone() noexcept
        {
            return _zone;
        }

        [[nodiscard]] const snf::server::Zone& zone() const noexcept
        {
            return _zone;
        }

        [[nodiscard]] snf::worker::TurnResult dispatch(
            snf::worker::ActorEnvelope&& envelope,
            const snf::worker::ActorTurnContext& context
        ) override;

    private:
        snf::server::Zone _zone;
    };
}
