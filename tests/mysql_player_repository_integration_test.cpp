#include "snf/load/load_client.hpp"
#include "snf/server/game_server.hpp"
#include "snf/server/mysql_player_repository.hpp"
#include "snf/server/repository_ranking_projector.hpp"

#include <mysql/mysql.h>

#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <utility>

namespace
{
    using namespace std::chrono_literals;

    std::optional<std::string> environment(const char* const name)
    {
        const char* const value = std::getenv(name);
        return value == nullptr ? std::nullopt : std::optional<std::string>{value};
    }

    std::uint16_t test_port()
    {
        const auto text = environment("SNF_MYSQL_TEST_PORT");
        if (!text)
        {
            return 3306;
        }
        std::uint32_t value = 0;
        const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), value);
        if (error != std::errc{} || end != text->data() + text->size() || value == 0 ||
            value > 65535)
        {
            throw std::invalid_argument{"SNF_MYSQL_TEST_PORT is invalid"};
        }
        return static_cast<std::uint16_t>(value);
    }

    snf::server::MySqlPlayerRepositoryConfig config()
    {
        return snf::server::MySqlPlayerRepositoryConfig{
            .host = environment("SNF_MYSQL_TEST_HOST").value(),
            .port = test_port(),
            .user = environment("SNF_MYSQL_TEST_USER").value_or("snf"),
            .password = environment("SNF_MYSQL_TEST_PASSWORD").value_or("snf-test"),
            .database = environment("SNF_MYSQL_TEST_DATABASE").value_or("snf_test"),
            .worker_count = 2,
            .queue_capacity = 32,
            .max_idempotency_records_per_player = 4,
            .connect_timeout = 5s,
            .read_timeout = 5s,
            .write_timeout = 5s,
            .purchase_fault_injector = {},
            .ranking_award_fault_injector = {},
            .ranking_checkpoint_fault_injector = {},
        };
    }

    void execute_sql(const snf::server::MySqlPlayerRepositoryConfig& repository_config,
                     const std::string_view sql)
    {
        MYSQL* connection = ::mysql_init(nullptr);
        assert(connection != nullptr);
        if (::mysql_real_connect(connection,
                                 repository_config.host.c_str(),
                                 repository_config.user.c_str(),
                                 repository_config.password.c_str(),
                                 repository_config.database.c_str(),
                                 repository_config.port,
                                 nullptr,
                                 0) == nullptr)
        {
            const std::string error = ::mysql_error(connection);
            ::mysql_close(connection);
            throw std::runtime_error{error};
        }
        if (::mysql_real_query(connection, sql.data(), sql.size()) != 0)
        {
            const std::string error = ::mysql_error(connection);
            ::mysql_close(connection);
            throw std::runtime_error{error};
        }
        ::mysql_close(connection);
    }

    void reset_storage(const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        // Construct once so migrations exist before cleanup.
        {
            snf::server::MySqlPlayerRepository repository{repository_config};
        }
        execute_sql(repository_config, "DELETE FROM snf_player_events");
        execute_sql(repository_config,
                    "UPDATE snf_event_stream SET last_offset=0 WHERE stream_name='ranking'");
        execute_sql(repository_config, "DELETE FROM snf_ranking_checkpoint_entries");
        execute_sql(repository_config,
                    "UPDATE snf_ranking_checkpoint_meta SET global_offset=0 WHERE singleton_id=1");
        execute_sql(repository_config, "DELETE FROM snf_purchase_idempotency");
        execute_sql(repository_config, "DELETE FROM snf_players");
    }

    snf::server::PlayerLoadResult load(snf::server::PlayerRepository& repository,
                                       const snf::server::PlayerId player)
    {
        std::promise<snf::server::PlayerLoadResult> completion;
        auto future = completion.get_future();
        repository.asyncLoad(player,
                             [&completion](snf::server::PlayerLoadResult result)
                             { completion.set_value(std::move(result)); });
        assert(future.wait_for(5s) == std::future_status::ready);
        return future.get();
    }

    snf::server::PlayerSaveResult save(snf::server::PlayerRepository& repository,
                                       snf::server::PlayerRecord record)
    {
        std::promise<snf::server::PlayerSaveResult> completion;
        auto future = completion.get_future();
        repository.asyncSave(std::move(record),
                             [&completion](snf::server::PlayerSaveResult result)
                             { completion.set_value(result); });
        assert(future.wait_for(5s) == std::future_status::ready);
        return future.get();
    }

    snf::server::PurchaseTransactionResult
    purchase(snf::server::PlayerRepository& repository,
             const snf::server::PlayerId player,
             const std::uint64_t key,
             const snf::server::ProductId product = snf::server::BASIC_PRODUCT)
    {
        std::promise<snf::server::PurchaseTransactionResult> completion;
        auto future = completion.get_future();
        repository.asyncPurchase(
            snf::server::PurchaseRequest{
                .player = player,
                .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = key},
                .product = product,
            },
            [&completion](snf::server::PurchaseTransactionResult result)
            { completion.set_value(std::move(result)); });
        assert(future.wait_for(5s) == std::future_status::ready);
        return future.get();
    }

    snf::server::RankingAwardTransactionResult award(snf::server::PlayerRepository& repository,
                                                     const snf::server::PlayerId player,
                                                     const std::uint64_t award_id,
                                                     const std::uint64_t delta)
    {
        std::promise<snf::server::RankingAwardTransactionResult> completion;
        auto future = completion.get_future();
        repository.asyncAwardRankingScore(
            snf::server::RankingAwardRequest{
                .player = player,
                .award_id = snf::server::RankingAwardId{.value = award_id},
                .score_delta = delta,
            },
            [&completion](snf::server::RankingAwardTransactionResult result)
            { completion.set_value(std::move(result)); });
        assert(future.wait_for(5s) == std::future_status::ready);
        return future.get();
    }

    void test_record_survives_repository_restart(
        const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        const snf::server::PlayerId player{.value = 1001};
        {
            snf::server::MySqlPlayerRepository repository{repository_config};
            assert(save(repository,
                        snf::server::PlayerRecord{
                            .player = player,
                            .handled_command_count = 9,
                            .last_location =
                                snf::server::PlayerLocation{
                                    .zone = snf::server::ZoneId{.value = 7},
                                    .position = {.x = -8, .y = 12},
                                },
                            .currency_balance = 700,
                            .purchased_item_count = 3,
                            .ranking_score = 44,
                            .last_domain_event_sequence = 2,
                        })
                       .saved());
        }

        snf::server::MySqlPlayerRepository restarted{repository_config};
        const auto loaded = load(restarted, player);
        assert(loaded.status == snf::server::PlayerRepositoryStatus::Success);
        assert(loaded.record.has_value());
        assert(loaded.record->handled_command_count == 9);
        assert((loaded.record->last_location == snf::server::PlayerLocation{
                                                    .zone = snf::server::ZoneId{.value = 7},
                                                    .position = {.x = -8, .y = 12},
                                                }));
        assert(loaded.record->currency_balance == 700);
        assert(loaded.record->purchased_item_count == 3);
        assert(loaded.record->ranking_score == 44);
        assert(loaded.record->last_domain_event_sequence == 2);
    }

    void test_concurrent_unique_key_commits_once(
        const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        const snf::server::PlayerId player{.value = 1002};
        snf::server::MySqlPlayerRepository first{repository_config};
        snf::server::MySqlPlayerRepository second{repository_config};

        std::promise<snf::server::PurchaseTransactionResult> first_completion;
        std::promise<snf::server::PurchaseTransactionResult> second_completion;
        auto first_result = first_completion.get_future();
        auto second_result = second_completion.get_future();
        const snf::server::PurchaseRequest request{
            .player = player,
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 55},
            .product = snf::server::BASIC_PRODUCT,
        };
        first.asyncPurchase(request,
                            [&first_completion](snf::server::PurchaseTransactionResult result)
                            { first_completion.set_value(std::move(result)); });
        second.asyncPurchase(request,
                             [&second_completion](snf::server::PurchaseTransactionResult result)
                             { second_completion.set_value(std::move(result)); });
        assert(first_result.wait_for(5s) == std::future_status::ready);
        assert(second_result.wait_for(5s) == std::future_status::ready);
        const auto first_value = first_result.get();
        const auto second_value = second_result.get();
        assert(first_value.status == snf::server::PurchaseStatus::Committed);
        assert(second_value.status == snf::server::PurchaseStatus::Committed);
        assert(first_value.replayed != second_value.replayed);
        assert(first.find(player)->currency_balance == 900);
        assert(first.find(player)->purchased_item_count == 1);

        const auto conflict = purchase(first, player, 55, snf::server::ProductId{.value = 2});
        assert(conflict.status == snf::server::PurchaseStatus::IdempotencyConflict);
        assert(!conflict.replayed);
    }

    void crash_at(const snf::server::MySqlPlayerRepositoryConfig& base_config,
                  const snf::server::MySqlPurchaseFaultPoint point,
                  const snf::server::PlayerId player,
                  const std::uint64_t key,
                  const int exit_code)
    {
        const pid_t child = ::fork();
        assert(child != -1);
        if (child == 0)
        {
            auto child_config = base_config;
            child_config.worker_count = 1;
            child_config.purchase_fault_injector =
                [point, exit_code](const snf::server::MySqlPurchaseFaultPoint observed)
            {
                if (observed == point)
                {
                    std::_Exit(exit_code);
                }
            };
            snf::server::MySqlPlayerRepository repository{std::move(child_config)};
            static_cast<void>(purchase(repository, player, key));
            std::_Exit(99);
        }

        int status = 0;
        assert(::waitpid(child, &status, 0) == child);
        assert(WIFEXITED(status));
        assert(WEXITSTATUS(status) == exit_code);
    }

    void test_crash_before_and_after_commit_is_recoverable(
        const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        const snf::server::PlayerId before_player{.value = 1003};
        crash_at(repository_config,
                 snf::server::MySqlPurchaseFaultPoint::BeforeCommit,
                 before_player,
                 61,
                 81);
        {
            snf::server::MySqlPlayerRepository recovered{repository_config};
            const auto retry = purchase(recovered, before_player, 61);
            assert(retry.status == snf::server::PurchaseStatus::Committed);
            assert(!retry.replayed);
            assert(retry.currency_balance == 900);
        }

        const snf::server::PlayerId after_player{.value = 1004};
        crash_at(repository_config,
                 snf::server::MySqlPurchaseFaultPoint::AfterCommitBeforeCompletion,
                 after_player,
                 62,
                 82);
        {
            snf::server::MySqlPlayerRepository recovered{repository_config};
            const auto retry = purchase(recovered, after_player, 62);
            assert(retry.status == snf::server::PurchaseStatus::Committed);
            assert(retry.replayed);
            assert(retry.currency_balance == 900);
            assert(retry.purchased_item_count == 1);
        }
    }

    void test_ranking_award_is_durable_ordered_and_idempotent(
        const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        const snf::server::PlayerId first_player{.value = 1101};
        const snf::server::PlayerId second_player{.value = 1102};
        snf::server::MySqlPlayerRepository first{repository_config};
        snf::server::MySqlPlayerRepository second{repository_config};

        std::promise<snf::server::RankingAwardTransactionResult> first_completion;
        std::promise<snf::server::RankingAwardTransactionResult> second_completion;
        auto first_result = first_completion.get_future();
        auto second_result = second_completion.get_future();
        first.asyncAwardRankingScore(
            snf::server::RankingAwardRequest{
                .player = first_player,
                .award_id = snf::server::RankingAwardId{.value = 1},
                .score_delta = 25,
            },
            [&first_completion](snf::server::RankingAwardTransactionResult result)
            { first_completion.set_value(std::move(result)); });
        second.asyncAwardRankingScore(
            snf::server::RankingAwardRequest{
                .player = first_player,
                .award_id = snf::server::RankingAwardId{.value = 1},
                .score_delta = 25,
            },
            [&second_completion](snf::server::RankingAwardTransactionResult result)
            { second_completion.set_value(std::move(result)); });
        assert(first_result.wait_for(5s) == std::future_status::ready);
        assert(second_result.wait_for(5s) == std::future_status::ready);
        const auto first_value = first_result.get();
        const auto second_value = second_result.get();
        assert(first_value.status == snf::server::RankingAwardStatus::Committed);
        assert(second_value.status == snf::server::RankingAwardStatus::Committed);
        assert(first_value.replayed != second_value.replayed);
        assert(first_value.global_offset == second_value.global_offset);

        const auto other_player = award(second, second_player, 1, 40);
        assert(other_player.status == snf::server::RankingAwardStatus::Committed);
        assert(!other_player.replayed);

        const auto next = award(first, first_player, 2, 15);
        assert(next.status == snf::server::RankingAwardStatus::Committed);
        assert(next.event_sequence == 2);
        assert(next.event_score == 40);

        const auto replay = award(first, first_player, 1, 25);
        assert(replay.status == snf::server::RankingAwardStatus::Committed);
        assert(replay.replayed);
        assert(replay.event_sequence == 1);
        assert(replay.event_score == 25);
        assert(replay.authoritative_sequence == 2);
        assert(replay.authoritative_score == 40);

        const auto conflict = award(first, first_player, 1, 26);
        assert(conflict.status == snf::server::RankingAwardStatus::IdempotencyConflict);
        assert(!conflict.replayed);
        const auto events = first.rankingEventsAfter(0);
        assert(events.size() == 3);
        for (std::size_t index = 0; index < events.size(); ++index)
        {
            assert(events[index].offset == index + 1);
        }
        assert(first.find(first_player)->ranking_score == 40);
        assert(first.find(first_player)->last_domain_event_sequence == 2);
    }

    void crash_ranking_at(const snf::server::MySqlPlayerRepositoryConfig& base_config,
                          const snf::server::MySqlRankingAwardFaultPoint point,
                          const snf::server::PlayerId player,
                          const std::uint64_t award_id,
                          const int exit_code)
    {
        const pid_t child = ::fork();
        assert(child != -1);
        if (child == 0)
        {
            auto child_config = base_config;
            child_config.worker_count = 1;
            child_config.ranking_award_fault_injector =
                [point, exit_code](const snf::server::MySqlRankingAwardFaultPoint observed)
            {
                if (observed == point)
                {
                    std::_Exit(exit_code);
                }
            };
            snf::server::MySqlPlayerRepository repository{std::move(child_config)};
            static_cast<void>(award(repository, player, award_id, 30));
            std::_Exit(99);
        }

        int status = 0;
        assert(::waitpid(child, &status, 0) == child);
        assert(WIFEXITED(status));
        assert(WEXITSTATUS(status) == exit_code);
    }

    void test_ranking_award_crash_boundary_recovers_from_outbox(
        const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        const snf::server::PlayerId before_player{.value = 1103};
        crash_ranking_at(repository_config,
                         snf::server::MySqlRankingAwardFaultPoint::BeforeCommit,
                         before_player,
                         71,
                         83);
        {
            snf::server::MySqlPlayerRepository recovered{repository_config};
            const auto retry = award(recovered, before_player, 71, 30);
            assert(retry.status == snf::server::RankingAwardStatus::Committed);
            assert(!retry.replayed);
            assert(retry.event_score == 30);
        }

        const snf::server::PlayerId after_player{.value = 1104};
        crash_ranking_at(repository_config,
                         snf::server::MySqlRankingAwardFaultPoint::AfterCommitBeforeCompletion,
                         after_player,
                         72,
                         84);
        {
            snf::server::MySqlPlayerRepository recovered{repository_config};
            const auto retry = award(recovered, after_player, 72, 30);
            assert(retry.status == snf::server::RankingAwardStatus::Committed);
            assert(retry.replayed);
            assert(retry.event_score == 30);
            const auto record = recovered.find(after_player);
            assert(record && record->ranking_score == 30);
            assert(record->last_domain_event_sequence == 1);
        }

        snf::server::MySqlPlayerRepository verified{repository_config};
        const auto events = verified.rankingEventsAfter(0);
        for (std::size_t index = 0; index < events.size(); ++index)
        {
            assert(events[index].offset == index + 1);
        }
    }

    void test_ranking_projector_restores_live_tail_and_checkpoints(
        const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        const snf::server::RepositoryRankingProjectorConfig projector_config{
            .batch_size = 2,
            .checkpoint_every_events = 1,
            .poll_interval = 5ms,
        };
        std::uint64_t checkpoint_offset = 0;
        {
            snf::server::MySqlPlayerRepository repository{repository_config};
            snf::server::RepositoryRankingProjector projector{repository, projector_config};
            const auto before = projector.stats();
            assert(before.projection_offset == repository.rankingEventsAfter(0).size());
            assert(before.poll_failures == 0);
            assert(before.checkpoint_failures == 0);

            const auto applied = award(repository, snf::server::PlayerId{.value = 1101}, 3, 5);
            assert(applied.status == snf::server::RankingAwardStatus::Committed);
            const auto deadline = std::chrono::steady_clock::now() + 2s;
            while (projector.stats().projection_offset != applied.global_offset &&
                   std::chrono::steady_clock::now() < deadline)
            {
                std::this_thread::sleep_for(1ms);
            }
            assert(projector.stats().projection_offset == applied.global_offset);
            assert(projector.stats().committed_tail_offset == applied.global_offset);
            assert(projector.stats().projection_lag == 0);
            assert(!projector.standings().empty());
            assert(projector.standings().front().player == snf::server::PlayerId{.value = 1101});
            assert(projector.standings().front().score == 45);
            checkpoint_offset = applied.global_offset;
        }

        snf::server::MySqlPlayerRepository restarted{repository_config};
        snf::server::RepositoryRankingProjector restored{restarted, projector_config};
        assert(restored.stats().projection_offset == checkpoint_offset);
        assert(restored.stats().checkpoint_offset == checkpoint_offset);
        assert(restored.standings().front().score == 45);
    }

    void crash_checkpoint_at(const snf::server::MySqlPlayerRepositoryConfig& base_config,
                             const snf::server::MySqlRankingCheckpointFaultPoint point,
                             const snf::server::RankingCheckpoint& checkpoint,
                             const int exit_code)
    {
        const pid_t child = ::fork();
        assert(child != -1);
        if (child == 0)
        {
            auto child_config = base_config;
            child_config.ranking_checkpoint_fault_injector =
                [point, exit_code](const snf::server::MySqlRankingCheckpointFaultPoint observed)
            {
                if (observed == point)
                {
                    std::_Exit(exit_code);
                }
            };
            snf::server::MySqlPlayerRepository repository{std::move(child_config)};
            repository.saveRankingCheckpoint(checkpoint);
            std::_Exit(99);
        }

        int status = 0;
        assert(::waitpid(child, &status, 0) == child);
        assert(WIFEXITED(status));
        assert(WEXITSTATUS(status) == exit_code);
    }

    void test_ranking_checkpoint_commit_is_atomic(
        const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        snf::server::RankingCheckpoint before;
        snf::server::RankingCheckpoint next;
        {
            snf::server::MySqlPlayerRepository repository{repository_config};
            before = repository.loadRankingCheckpoint();
            const auto applied = award(repository, snf::server::PlayerId{.value = 1102}, 2, 5);
            assert(applied.status == snf::server::RankingAwardStatus::Committed);

            snf::server::RankingProjection projection;
            projection.restore(before);
            assert(projection.replay(repository.rankingEventsAfter(before.offset)) ==
                   snf::server::ProjectionApplyResult::Applied);
            next = projection.checkpoint();
            assert(next.offset == applied.global_offset);
        }

        {
            crash_checkpoint_at(repository_config,
                                snf::server::MySqlRankingCheckpointFaultPoint::BeforeCommit,
                                next,
                                85);
            snf::server::MySqlPlayerRepository recovered{repository_config};
            assert(recovered.loadRankingCheckpoint().offset == before.offset);
        }

        {
            crash_checkpoint_at(repository_config,
                                snf::server::MySqlRankingCheckpointFaultPoint::AfterCommit,
                                next,
                                86);
            snf::server::MySqlPlayerRepository recovered{repository_config};
            assert(recovered.loadRankingCheckpoint().offset == next.offset);
            assert(recovered.loadRankingCheckpoint().entries == next.entries);
        }
    }

    void test_failed_outcome_and_capacity_are_durable(
        const snf::server::MySqlPlayerRepositoryConfig& base_config)
    {
        auto repository_config = base_config;
        repository_config.max_idempotency_records_per_player = 2;
        const snf::server::PlayerId player{.value = 1005};
        snf::server::MySqlPlayerRepository repository{repository_config};
        assert(save(repository,
                    snf::server::PlayerRecord{
                        .player = player,
                        .handled_command_count = 0,
                        .last_location = std::nullopt,
                        .currency_balance = 50,
                        .purchased_item_count = 0,
                        .ranking_score = 0,
                        .last_domain_event_sequence = 0,
                    })
                   .saved());
        const auto insufficient = purchase(repository, player, 71);
        assert(insufficient.status == snf::server::PurchaseStatus::InsufficientFunds);
        assert(!insufficient.replayed);

        assert(save(repository,
                    snf::server::PlayerRecord{
                        .player = player,
                        .handled_command_count = 0,
                        .last_location = std::nullopt,
                        .currency_balance = 1000,
                        .purchased_item_count = 0,
                        .ranking_score = 0,
                        .last_domain_event_sequence = 0,
                    })
                   .saved());
        const auto replay = purchase(repository, player, 71);
        assert(replay.status == snf::server::PurchaseStatus::InsufficientFunds);
        assert(replay.replayed);
        assert(replay.currency_balance == 1000);

        const auto missing = purchase(repository, player, 72, snf::server::ProductId{.value = 999});
        assert(missing.status == snf::server::PurchaseStatus::ProductNotFound);
        assert(purchase(repository, player, 72).status == snf::server::PurchaseStatus::Committed);
        assert(purchase(repository, player, 73).status ==
               snf::server::PurchaseStatus::IdempotencyCapacityExceeded);
    }

    void test_mysql_queue_is_bounded_without_blocking_the_caller(
        const snf::server::MySqlPlayerRepositoryConfig& base_config)
    {
        auto repository_config = base_config;
        repository_config.worker_count = 1;
        repository_config.queue_capacity = 1;
        std::promise<void> transaction_blocked;
        auto blocked = transaction_blocked.get_future();
        std::promise<void> release_transaction;
        const auto release = release_transaction.get_future().share();
        repository_config.purchase_fault_injector =
            [&transaction_blocked, release](const snf::server::MySqlPurchaseFaultPoint point)
        {
            if (point == snf::server::MySqlPurchaseFaultPoint::BeforeCommit)
            {
                transaction_blocked.set_value();
                release.wait();
            }
        };

        snf::server::MySqlPlayerRepository repository{std::move(repository_config)};
        std::promise<snf::server::PurchaseTransactionResult> first_completion;
        auto first = first_completion.get_future();
        repository.asyncPurchase(
            snf::server::PurchaseRequest{
                .player = snf::server::PlayerId{.value = 1006},
                .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 81},
                .product = snf::server::BASIC_PRODUCT,
            },
            [&first_completion](snf::server::PurchaseTransactionResult result)
            { first_completion.set_value(std::move(result)); });
        assert(blocked.wait_for(5s) == std::future_status::ready);

        std::promise<snf::server::PlayerLoadResult> queued_completion;
        auto queued = queued_completion.get_future();
        repository.asyncLoad(snf::server::PlayerId{.value = 1007},
                             [&queued_completion](snf::server::PlayerLoadResult result)
                             { queued_completion.set_value(std::move(result)); });

        std::optional<snf::server::PlayerLoadResult> rejected;
        repository.asyncLoad(snf::server::PlayerId{.value = 1008},
                             [&rejected](snf::server::PlayerLoadResult result)
                             { rejected = std::move(result); });
        assert(rejected.has_value());
        assert(rejected->status == snf::server::PlayerRepositoryStatus::Unavailable);
        assert(repository.stats().rejected == 1);
        assert(repository.stats().queue_high_water_mark == 1);

        release_transaction.set_value();
        assert(first.wait_for(5s) == std::future_status::ready);
        assert(first.get().status == snf::server::PurchaseStatus::Committed);
        assert(queued.wait_for(5s) == std::future_status::ready);
        assert(queued.get().status == snf::server::PlayerRepositoryStatus::Success);
    }

    class RunningMySqlServer final
    {
    public:
        explicit RunningMySqlServer(
            const snf::server::MySqlPlayerRepositoryConfig& repository_config)
            : _server(
                  [&repository_config]
                  {
                      snf::server::GameServerConfig server_config;
                      server_config.port = 0;
                      server_config.shutdown_grace_period = 500ms;
                      server_config.mysql_player_repository = repository_config;
                      return server_config;
                  }())
            , _thread(
                  [this]
                  {
                      try
                      {
                          _server.run();
                      }
                      catch (...)
                      {
                          _failure = std::current_exception();
                      }
                  })
        {
        }

        ~RunningMySqlServer()
        {
            if (_thread.joinable())
            {
                _server.requestStop();
                _thread.join();
            }
        }

        [[nodiscard]] std::uint16_t port() const noexcept
        {
            return _server.getPort();
        }

        [[nodiscard]] std::vector<snf::server::RankingEntry> standings() const
        {
            return _server.getRankingStandings();
        }

        void stop()
        {
            _server.requestStop();
            _thread.join();
            if (_failure)
            {
                std::rethrow_exception(_failure);
            }
        }

    private:
        snf::server::GameServer _server;
        std::exception_ptr _failure;
        std::thread _thread;
    };

    void run_zone_cycle(const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        RunningMySqlServer server{repository_config};
        const auto standings = server.standings();
        assert(!standings.empty());
        assert(standings.front().player == snf::server::PlayerId{.value = 1101});
        assert(standings.front().score == 45);
        const snf::load::LoadClient client{snf::load::LoadClientConfig{
            .host = "127.0.0.1",
            .port = server.port(),
            .connections = 6,
            .duration = 400ms,
            .requests_per_second = 20,
            .scenario = snf::load::LoadScenario::Zone,
            .players_per_zone = 3,
            .connect_timeout = 2s,
            .request_timeout = 2s,
        }};
        const auto result = client.run();
        assert(result.success);
        assert(result.sent_bootstrap_requests == 12);
        assert(result.received_gameplay_responses > 0);
        server.stop();
    }

    void test_game_server_restores_mysql_players_across_restart(
        const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        run_zone_cycle(repository_config);
        run_zone_cycle(repository_config);
    }
}

int main()
{
    if (!environment("SNF_MYSQL_TEST_HOST"))
    {
        return 77;
    }

    const auto repository_config = config();
    reset_storage(repository_config);
    test_record_survives_repository_restart(repository_config);
    test_concurrent_unique_key_commits_once(repository_config);
    test_crash_before_and_after_commit_is_recoverable(repository_config);
    test_ranking_award_is_durable_ordered_and_idempotent(repository_config);
    test_ranking_award_crash_boundary_recovers_from_outbox(repository_config);
    test_ranking_projector_restores_live_tail_and_checkpoints(repository_config);
    test_ranking_checkpoint_commit_is_atomic(repository_config);
    test_failed_outcome_and_capacity_are_durable(repository_config);
    test_mysql_queue_is_bounded_without_blocking_the_caller(repository_config);
    test_game_server_restores_mysql_players_across_restart(repository_config);
}
