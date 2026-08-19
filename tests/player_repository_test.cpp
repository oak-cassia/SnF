#include "snf/server/player_repository.hpp"

#include <cassert>
#include <optional>
#include <utility>

namespace
{
    void test_in_memory_repository_loads_missing_and_saved_records()
    {
        snf::server::InMemoryPlayerRepository repository;
        const snf::server::PlayerId player{.value = 77};

        std::optional<snf::server::PlayerLoadResult> missing;
        repository.asyncLoad(player, [&missing](snf::server::PlayerLoadResult result) { missing = std::move(result); });
        assert(missing.has_value());
        assert(!missing->record.has_value());

        std::optional<snf::server::PlayerSaveResult> save;
        repository.asyncSave(
            snf::server::PlayerRecord{
                .player = player,
                .handled_command_count = 42,
                .last_location =
                    snf::server::PlayerLocation{
                        .zone = snf::server::ZoneId{.value = 3},
                        .position = {.x = -4, .y = 5},
                    },
                .currency_balance = 600,
                .purchased_item_count = 4,
            },
            [&save](snf::server::PlayerSaveResult result) { save = result; });
        assert(save.has_value());
        assert(save->saved());

        std::optional<snf::server::PlayerLoadResult> loaded;
        repository.asyncLoad(player, [&loaded](snf::server::PlayerLoadResult result) { loaded = std::move(result); });
        assert(loaded.has_value());
        assert(loaded->record.has_value());
        assert(loaded->record->player == player);
        assert(loaded->record->handled_command_count == 42);
        assert(loaded->record->currency_balance == 600);
        assert(loaded->record->purchased_item_count == 4);
        assert((loaded->record->last_location == snf::server::PlayerLocation{
                                                     .zone = snf::server::ZoneId{.value = 3},
                                                     .position = {.x = -4, .y = 5},
                                                 }));
        assert(repository.find(player)->handled_command_count == 42);

        const auto stats = repository.stats();
        assert(stats.accepted == 3);
        assert(stats.rejected == 0);
        assert(stats.queue_depth == 0);
    }
}

void run_player_repository_tests()
{
    test_in_memory_repository_loads_missing_and_saved_records();
}
