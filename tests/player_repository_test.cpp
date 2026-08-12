#include "snf/server/player_repository.hpp"

#include <cassert>
#include <chrono>
#include <future>
#include <limits>
#include <optional>
#include <thread>
#include <utility>

namespace
{
    using namespace std::chrono_literals;

    snf::server::PurchaseTransactionResult
    purchase(snf::server::PlayerRepository& repository,
             const snf::server::PlayerId player,
             const std::uint64_t key,
             const snf::server::ProductId product = snf::server::BASIC_PRODUCT)
    {
        std::optional<snf::server::PurchaseTransactionResult> completed;
        repository.asyncPurchase(
            snf::server::PurchaseRequest{
                .player = player,
                .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = key},
                .product = product,
            },
            [&completed](snf::server::PurchaseTransactionResult result)
            { completed = std::move(result); });
        assert(completed.has_value());
        return *completed;
    }

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
                .currency_balance = 600,
                .purchased_item_count = 4,
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
        assert(loaded->record->currency_balance == 600);
        assert(loaded->record->purchased_item_count == 4);
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
                .max_idempotency_records_per_player = 8,
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
                .currency_balance = snf::server::INITIAL_CURRENCY_BALANCE,
                .purchased_item_count = 0,
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
                .max_idempotency_records_per_player = 8,
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

    void test_purchase_transaction_is_atomic_idempotent_and_bounded()
    {
        snf::server::InMemoryPlayerRepository repository{2};
        const snf::server::PlayerId player{.value = 99};

        const auto first = purchase(repository, player, 1);
        assert(first.status == snf::server::PurchaseStatus::Committed);
        assert(!first.replayed);
        assert(first.currency_balance == 900);
        assert(first.purchased_item_count == 1);

        const auto replay = purchase(repository, player, 1);
        assert(replay.status == snf::server::PurchaseStatus::Committed);
        assert(replay.replayed);
        assert(replay.currency_balance == 900);
        assert(replay.purchased_item_count == 1);

        const auto conflict = purchase(repository, player, 1, snf::server::ProductId{.value = 2});
        assert(conflict.status == snf::server::PurchaseStatus::IdempotencyConflict);
        assert(conflict.currency_balance == 900);
        assert(conflict.purchased_item_count == 1);

        const auto second = purchase(repository, player, 2);
        assert(second.status == snf::server::PurchaseStatus::Committed);
        assert(second.currency_balance == 800);
        assert(second.purchased_item_count == 2);
        const auto capacity = purchase(repository, player, 3);
        assert(capacity.status == snf::server::PurchaseStatus::IdempotencyCapacityExceeded);
        assert(repository.find(player)->currency_balance == 800);
        assert(repository.find(player)->purchased_item_count == 2);

        const auto stats = repository.purchaseStats();
        assert(stats.committed == 2);
        assert(stats.replayed == 1);
        assert(stats.rejected == 2);
    }

    void test_failed_purchase_result_is_stable_across_balance_changes()
    {
        snf::server::InMemoryPlayerRepository repository{2};
        const snf::server::PlayerId player{.value = 100};
        std::optional<snf::server::PlayerSaveResult> saved;
        repository.asyncSave(
            snf::server::PlayerRecord{
                .player = player,
                .handled_command_count = 0,
                .last_location = std::nullopt,
                .currency_balance = 50,
                .purchased_item_count = 0,
            },
            [&saved](snf::server::PlayerSaveResult result) { saved = result; });
        assert(saved && saved->saved());

        const auto insufficient = purchase(repository, player, 10);
        assert(insufficient.status == snf::server::PurchaseStatus::InsufficientFunds);
        assert(insufficient.currency_balance == 50);

        saved.reset();
        repository.asyncSave(
            snf::server::PlayerRecord{
                .player = player,
                .handled_command_count = 0,
                .last_location = std::nullopt,
                .currency_balance = 1000,
                .purchased_item_count = 0,
            },
            [&saved](snf::server::PlayerSaveResult result) { saved = result; });
        assert(saved && saved->saved());
        const auto replay = purchase(repository, player, 10);
        assert(replay.status == snf::server::PurchaseStatus::InsufficientFunds);
        assert(replay.replayed);
        assert(replay.currency_balance == 1000);
        assert(repository.find(player)->currency_balance == 1000);

        const auto missing = purchase(repository, player, 11, snf::server::ProductId{.value = 999});
        assert(missing.status == snf::server::PurchaseStatus::ProductNotFound);
    }

    void test_threaded_purchase_reports_unavailable_after_close()
    {
        snf::server::ThreadedPlayerRepository repository{
            snf::server::ThreadedPlayerRepositoryConfig{
                .worker_count = 1,
                .queue_capacity = 2,
                .max_idempotency_records_per_player = 2,
            }};
        repository.close();
        const auto result = purchase(repository, snf::server::PlayerId{.value = 101}, 1);
        assert(result.status == snf::server::PurchaseStatus::Unavailable);
        assert(repository.stats().rejected == 1);
    }

    void test_purchase_inventory_overflow_rolls_back_currency_debit()
    {
        snf::server::InMemoryPlayerRepository repository{1};
        const snf::server::PlayerId player{.value = 102};
        std::optional<snf::server::PlayerSaveResult> saved;
        repository.asyncSave(
            snf::server::PlayerRecord{
                .player = player,
                .handled_command_count = 0,
                .last_location = std::nullopt,
                .currency_balance = 100,
                .purchased_item_count = std::numeric_limits<std::uint64_t>::max(),
            },
            [&saved](snf::server::PlayerSaveResult result) { saved = result; });
        assert(saved && saved->saved());

        const auto result = purchase(repository, player, 1);
        assert(result.status == snf::server::PurchaseStatus::InventoryCapacityExceeded);
        const auto record = repository.find(player);
        assert(record.has_value());
        assert(record->currency_balance == 100);
        assert(record->purchased_item_count == std::numeric_limits<std::uint64_t>::max());
    }
}

void run_player_repository_tests()
{
    test_in_memory_repository_loads_missing_and_saved_records();
    test_threaded_repository_completes_on_a_bounded_worker();
    test_threaded_repository_rejects_instead_of_blocking_when_full();
    test_purchase_transaction_is_atomic_idempotent_and_bounded();
    test_failed_purchase_result_is_stable_across_balance_changes();
    test_threaded_purchase_reports_unavailable_after_close();
    test_purchase_inventory_overflow_rolls_back_currency_debit();
}
