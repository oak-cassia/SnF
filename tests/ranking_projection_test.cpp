#include "snf/server/player_repository.hpp"
#include "snf/server/ranking_projection.hpp"
#include "snf/server/repository_ranking_projector.hpp"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

namespace
{
    using namespace std::chrono_literals;

    snf::server::PlayerDomainEvent
    score_event(const std::uint64_t player, const std::uint64_t sequence, const std::uint64_t score)
    {
        return snf::server::PlayerScoreChanged{
            .player = snf::server::PlayerId{.value = player},
            .sequence = sequence,
            .score = score,
        };
    }

    void test_event_log_enforces_identity_order_and_capacity()
    {
        snf::server::InMemoryPlayerEventLog log{2};

        const auto first = log.append(score_event(1, 1, 10));
        assert(first.status == snf::server::PlayerEventPublishResult::Published);
        assert(first.record && first.record->offset == 1);

        const auto duplicate = log.append(score_event(1, 1, 10));
        assert(duplicate.status == snf::server::PlayerEventPublishResult::Duplicate);
        assert(duplicate.record && duplicate.record->offset == 1);
        assert(log.size() == 1);

        assert(log.append(score_event(1, 1, 11)).status ==
               snf::server::PlayerEventPublishResult::Conflict);
        assert(log.append(score_event(99, 2, 1)).status ==
               snf::server::PlayerEventPublishResult::OutOfOrder);
        assert(log.size() == 1);

        assert(log.append(score_event(2, 1, 10)).status ==
               snf::server::PlayerEventPublishResult::Published);
        assert(log.append(score_event(1, 2, 20)).status ==
               snf::server::PlayerEventPublishResult::Full);
        assert(log.size() == 2);
        assert(log.recordsAfter(1).size() == 1);
    }

    void test_projection_orders_deterministically_and_rejects_without_mutation()
    {
        snf::server::RankingProjection projection;
        assert(projection.apply({.offset = 1, .event = score_event(2, 1, 50)}) ==
               snf::server::ProjectionApplyResult::Applied);
        assert(projection.apply({.offset = 2, .event = score_event(1, 1, 50)}) ==
               snf::server::ProjectionApplyResult::Applied);
        assert((projection.standings() ==
                std::vector<snf::server::RankingEntry>{
                    {.player = snf::server::PlayerId{.value = 1}, .score = 50, .last_sequence = 1},
                    {.player = snf::server::PlayerId{.value = 2}, .score = 50, .last_sequence = 1},
                }));

        const auto before = projection.checkpoint();
        assert(projection.apply({.offset = 3, .event = score_event(3, 2, 60)}) ==
               snf::server::ProjectionApplyResult::OutOfOrder);
        assert(projection.checkpoint().offset == before.offset);
        assert(projection.checkpoint().entries == before.entries);
    }

    void test_checkpoint_restore_and_tail_replay_match_live_projection()
    {
        snf::server::InMemoryRankingEventPipeline pipeline{8};
        assert(pipeline.publish(score_event(1, 1, 10)) ==
               snf::server::PlayerEventPublishResult::Published);
        assert(pipeline.publish(score_event(2, 1, 20)) ==
               snf::server::PlayerEventPublishResult::Published);
        const snf::server::RankingCheckpoint checkpoint = pipeline.checkpoint();

        assert(pipeline.publish(score_event(1, 2, 40)) ==
               snf::server::PlayerEventPublishResult::Published);
        assert(pipeline.publish(score_event(2, 2, 30)) ==
               snf::server::PlayerEventPublishResult::Published);
        assert(pipeline.publish(score_event(2, 2, 30)) ==
               snf::server::PlayerEventPublishResult::Duplicate);

        const auto tail = pipeline.recordsAfter(checkpoint.offset);
        snf::server::RankingProjection restored;
        restored.restore(checkpoint);
        assert(restored.replay(tail) == snf::server::ProjectionApplyResult::Applied);
        assert(restored.standings() == pipeline.standings());
        assert(restored.replay(tail) == snf::server::ProjectionApplyResult::Duplicate);
        assert(restored.standings() == pipeline.standings());

        const auto stats = pipeline.stats();
        assert(stats.published == 4);
        assert(stats.duplicates == 1);
        assert(stats.rejected == 0);
        assert(stats.event_count == 4);
        assert(stats.projection_offset == 4);
    }

    void test_invalid_checkpoint_is_rejected()
    {
        snf::server::RankingProjection projection;
        bool rejected = false;
        try
        {
            projection.restore(snf::server::RankingCheckpoint{
                .offset = 1,
                .entries =
                    {
                        {.player = snf::server::PlayerId{.value = 4},
                         .score = 20,
                         .last_sequence = 2},
                    },
            });
        }
        catch (const std::invalid_argument&)
        {
            rejected = true;
        }
        assert(rejected);
        assert(projection.offset() == 0);
        assert(projection.standings().empty());
    }

    void award(snf::server::PlayerRepository& repository,
               const std::uint64_t player,
               const std::uint64_t award_id,
               const std::uint64_t delta)
    {
        std::optional<snf::server::RankingAwardTransactionResult> result;
        repository.asyncAwardRankingScore(
            snf::server::RankingAwardRequest{
                .player = snf::server::PlayerId{.value = player},
                .award_id = snf::server::RankingAwardId{.value = award_id},
                .score_delta = delta,
            },
            [&result](snf::server::RankingAwardTransactionResult completed)
            { result = completed; });
        assert(result && result->status == snf::server::RankingAwardStatus::Committed);
    }

    class FlakyRankingStore final : public snf::server::RankingStore
    {
    public:
        [[nodiscard]] snf::server::RankingCheckpoint loadRankingCheckpoint() const override
        {
            return repository.loadRankingCheckpoint();
        }

        void saveRankingCheckpoint(const snf::server::RankingCheckpoint& checkpoint) override
        {
            if (fail_checkpoint.load())
            {
                throw std::runtime_error{"Injected checkpoint failure"};
            }
            repository.saveRankingCheckpoint(checkpoint);
        }

        [[nodiscard]] std::uint64_t rankingTailOffset() const override
        {
            failReadIfRequested();
            return repository.rankingTailOffset();
        }

        [[nodiscard]] std::vector<snf::server::PlayerEventRecord>
        rankingEventsAfter(const std::uint64_t offset, const std::size_t limit) const override
        {
            failReadIfRequested();
            return repository.rankingEventsAfter(offset, limit);
        }

        snf::server::InMemoryPlayerRepository repository;
        std::atomic_bool fail_reads{false};
        std::atomic_bool fail_checkpoint{false};

    private:
        void failReadIfRequested() const
        {
            if (fail_reads.load())
            {
                throw std::runtime_error{"Injected ranking read failure"};
            }
        }
    };

    template <typename Predicate> void wait_until(Predicate predicate)
    {
        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (!predicate() && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(predicate());
    }

    void test_repository_projector_restores_polls_and_checkpoints()
    {
        snf::server::InMemoryPlayerRepository store;
        award(store, 1, 1, 10);
        award(store, 2, 1, 20);

        const snf::server::RepositoryRankingProjectorConfig config{
            .batch_size = 1,
            .checkpoint_every_events = 1,
            .poll_interval = 5ms,
        };
        {
            snf::server::RepositoryRankingProjector projector{store, config};
            assert(
                (projector.standings() ==
                 std::vector<snf::server::RankingEntry>{
                     {.player = snf::server::PlayerId{.value = 2}, .score = 20, .last_sequence = 1},
                     {.player = snf::server::PlayerId{.value = 1}, .score = 10, .last_sequence = 1},
                 }));

            award(store, 1, 2, 30);
            const auto deadline = std::chrono::steady_clock::now() + 1s;
            while (projector.stats().projection_offset != 3 &&
                   std::chrono::steady_clock::now() < deadline)
            {
                std::this_thread::sleep_for(1ms);
            }
            assert(projector.stats().projection_offset == 3);
            assert(projector.stats().committed_tail_offset == 3);
            assert(projector.stats().projection_lag == 0);
            assert(projector.stats().poll_failures == 0);
            assert(projector.stats().checkpoint_failures == 0);
        }

        assert(store.loadRankingCheckpoint().offset == 3);
        snf::server::RepositoryRankingProjector restored{store, config};
        assert(restored.stats().projection_offset == 3);
        assert((restored.standings() ==
                std::vector<snf::server::RankingEntry>{
                    {.player = snf::server::PlayerId{.value = 1}, .score = 40, .last_sequence = 2},
                    {.player = snf::server::PlayerId{.value = 2}, .score = 20, .last_sequence = 1},
                }));
    }

    void test_repository_projector_retries_failures_and_drains_on_stop()
    {
        FlakyRankingStore store;
        const snf::server::RepositoryRankingProjectorConfig config{
            .batch_size = 1,
            .checkpoint_every_events = 1,
            .poll_interval = 5ms,
        };
        snf::server::RepositoryRankingProjector projector{store, config};

        store.fail_reads = true;
        award(store.repository, 1, 1, 10);
        wait_until([&projector] { return projector.stats().poll_failures != 0; });

        store.fail_checkpoint = true;
        store.fail_reads = false;
        wait_until([&projector] { return projector.stats().projection_offset == 1; });
        wait_until([&projector] { return projector.stats().checkpoint_failures != 0; });
        assert(projector.stats().projection_lag == 0);

        store.fail_checkpoint = false;
        wait_until([&projector] { return projector.stats().checkpoint_offset == 1; });

        award(store.repository, 2, 1, 20);
        projector.stop();
        assert(projector.stats().projection_offset == 2);
        assert(projector.stats().checkpoint_offset == 2);
        assert(store.repository.loadRankingCheckpoint().offset == 2);
    }
}

void run_ranking_projection_tests()
{
    test_event_log_enforces_identity_order_and_capacity();
    test_projection_orders_deterministically_and_rejects_without_mutation();
    test_checkpoint_restore_and_tail_replay_match_live_projection();
    test_invalid_checkpoint_is_rejected();
    test_repository_projector_restores_polls_and_checkpoints();
    test_repository_projector_retries_failures_and_drains_on_stop();
}
