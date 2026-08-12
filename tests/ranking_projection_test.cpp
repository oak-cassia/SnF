#include "snf/server/ranking_projection.hpp"

#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace
{
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
}

void run_ranking_projection_tests()
{
    test_event_log_enforces_identity_order_and_capacity();
    test_projection_orders_deterministically_and_rejects_without_mutation();
    test_checkpoint_restore_and_tail_replay_match_live_projection();
    test_invalid_checkpoint_is_rejected();
}
