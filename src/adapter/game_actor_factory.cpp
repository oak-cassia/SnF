#include "snf/adapter/game_actor_factory.hpp"

#include "snf/adapter/player_actor_adapter.hpp"
#include "snf/adapter/room_actor_adapter.hpp"
#include "snf/adapter/zone_actor_adapter.hpp"

namespace snf::adapter
{
    snf::worker::ActorConstructionResult GameActorFactory::construct(const snf::worker::ActorKey key)
    {
        switch (key.kind)
        {
        case snf::worker::ActorKind::Player:
        {
            auto player_actor = std::make_unique<PlayerActorAdapter>(snf::server::PlayerId{key.entity});
            return snf::worker::ActorConstructionResult::ready(std::move(player_actor));
        }
        case snf::worker::ActorKind::Zone:
        {
            auto zone_actor = std::make_unique<ZoneActorAdapter>(snf::server::ZoneId{key.entity});
            return snf::worker::ActorConstructionResult::ready(std::move(zone_actor));
        }
        case snf::worker::ActorKind::Room:
        {
            auto room_actor = std::make_unique<RoomActorAdapter>(snf::server::RoomId{key.entity}, _timer_admission);
            return snf::worker::ActorConstructionResult::ready(std::move(room_actor));
        }
        default:
            return snf::worker::ActorConstructionResult::rejected();
        }
    }
}
