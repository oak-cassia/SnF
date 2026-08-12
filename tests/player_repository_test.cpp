#include "snf/server/player_repository.hpp"

#include <cassert>
#include <chrono>
#include <future>
#include <optional>
#include <thread>
#include <utility>

namespace
{
    using namespace std::chrono_literals;

    void test_in_memory_repository_loads_missing_and_saved_records()
    {
        snf::server::InMemoryPlayerRepository repository;
        const snf::server::PlayerId player{.value = 77};

        std::optional<snf::server::PlayerLoadResult> missing;
        repository.asyncLoad(player,
                             [&missing](snf::server::PlayerLoadResult result)
                             { missing = std::move(result); });
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
            },
            [&save](snf::server::PlayerSaveResult result) { save = result; });
        assert(save.has_value());
        assert(save->saved());

        std::optional<snf::server::PlayerLoadResult> loaded;
        repository.asyncLoad(player,
                             [&loaded](snf::server::PlayerLoadResult result)
                             { loaded = std::move(result); });
        assert(loaded.has_value());
        assert(loaded->record.has_value());
        assert(loaded->record->player == player);
        assert(loaded->record->handled_command_count == 42);
        assert((loaded->record->last_location == snf::server::PlayerLocation{
                                                     .zone = snf::server::ZoneId{.value = 3},
                                                     .position = {.x = -4, .y = 5},
                                                 }));
        assert(repository.find(player)->handled_command_count == 42);
    }

    void test_threaded_repository_completes_on_a_bounded_worker()
    {
        snf::server::ThreadedPlayerRepository repository{
            snf::server::ThreadedPlayerRepositoryConfig{
                .worker_count = 1,
                .queue_capacity = 2,
            }};
        const snf::server::PlayerId player{.value = 88};
        const std::thread::id caller = std::this_thread::get_id();
        std::promise<std::thread::id> completed_on;
        auto completed = completed_on.get_future();

        repository.asyncSave(
            snf::server::PlayerRecord{
                .player = player,
                .handled_command_count = 7,
                .last_location = std::nullopt,
            },
            [&completed_on](snf::server::PlayerSaveResult result)
            {
                assert(result.saved());
                completed_on.set_value(std::this_thread::get_id());
            });

        assert(completed.wait_for(1s) == std::future_status::ready);
        assert(completed.get() != caller);
        assert(repository.find(player)->handled_command_count == 7);
        assert(repository.stats().accepted == 1);

        repository.close();
        std::optional<snf::server::PlayerLoadResult> rejected;
        repository.asyncLoad(player,
                             [&rejected](snf::server::PlayerLoadResult result)
                             { rejected = std::move(result); });
        assert(rejected.has_value());
        assert(rejected->status == snf::server::PlayerRepositoryStatus::Unavailable);
        assert(repository.stats().rejected == 1);
    }

    void test_threaded_repository_rejects_instead_of_blocking_when_full()
    {
        snf::server::ThreadedPlayerRepository repository{
            snf::server::ThreadedPlayerRepositoryConfig{
                .worker_count = 1,
                .queue_capacity = 1,
            }};
        std::promise<void> first_completion_started;
        auto started = first_completion_started.get_future();
        std::promise<void> release_first_completion;
        const auto release = release_first_completion.get_future().share();

        repository.asyncLoad(
            snf::server::PlayerId{.value = 1},
            [&first_completion_started, release](snf::server::PlayerLoadResult result)
            {
                assert(result.status == snf::server::PlayerRepositoryStatus::Success);
                first_completion_started.set_value();
                release.wait();
            });
        assert(started.wait_for(1s) == std::future_status::ready);

        std::promise<void> second_completed;
        auto second = second_completed.get_future();
        repository.asyncLoad(snf::server::PlayerId{.value = 2},
                             [&second_completed](snf::server::PlayerLoadResult result)
                             {
                                 assert(result.status ==
                                        snf::server::PlayerRepositoryStatus::Success);
                                 second_completed.set_value();
                             });

        std::optional<snf::server::PlayerLoadResult> rejected;
        repository.asyncLoad(snf::server::PlayerId{.value = 3},
                             [&rejected](snf::server::PlayerLoadResult result)
                             { rejected = std::move(result); });
        assert(rejected.has_value());
        assert(rejected->status == snf::server::PlayerRepositoryStatus::Unavailable);
        assert(repository.stats().queue_high_water_mark == 1);

        release_first_completion.set_value();
        assert(second.wait_for(1s) == std::future_status::ready);
    }
}

void run_player_repository_tests()
{
    test_in_memory_repository_loads_missing_and_saved_records();
    test_threaded_repository_completes_on_a_bounded_worker();
    test_threaded_repository_rejects_instead_of_blocking_when_full();
}
