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
            if (_player_load_enabled)
            {
                // The persisted row decides what this actor is, so it cannot be
                // built before the load completes.
                return snf::worker::ActorConstructionResult::needsLoad();
            }
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

    snf::worker::ActorConstructionResult GameActorFactory::constructLoaded(
        const snf::worker::ActorKey key,
        const snf::worker::LoadPlayerResult& loaded
    )
    {
        if (key.kind != snf::worker::ActorKind::Player)
        {
            return snf::worker::ActorConstructionResult::rejected();
        }

        const snf::server::PlayerId player{key.entity};
        if (!loaded.found)
        {
            // No row is a new player, not a failure. The empty actor is what gets
            // persisted the first time it saves.
            return snf::worker::ActorConstructionResult::ready(std::make_unique<PlayerActorAdapter>(player));
        }

        std::vector<snf::server::SkillId> owned_skill_ids;
        owned_skill_ids.reserve(loaded.owned_skill_ids.size());
        for (const std::uint32_t skill_id : loaded.owned_skill_ids)
        {
            owned_skill_ids.push_back(snf::server::SkillId{.value = skill_id});
        }

        std::optional<snf::server::PlayerLocation> last_location;
        if (loaded.row.has_location)
        {
            last_location = snf::server::PlayerLocation{
                .zone = snf::server::ZoneId{loaded.row.zone_id},
                .position =
                    snf::server::ZonePosition{
                        .x = loaded.row.position_x,
                        .y = loaded.row.position_y,
                    },
            };
        }

        const snf::server::PlayerRecord record{
            .player = player,
            .handled_command_count = loaded.row.handled_command_count,
            .last_location = last_location,
            .currency_balance = loaded.row.currency_balance,
            .purchased_item_count = loaded.row.purchased_item_count,
            .street_experience = loaded.row.street_experience,
            .skill_loadout = snf::server::SkillLoadout{std::move(owned_skill_ids), snf::server::SkillId{.value = loaded.row.equipped_skill_id}},
        };

        return snf::worker::ActorConstructionResult::ready(std::make_unique<PlayerActorAdapter>(player, record));
    }
}
