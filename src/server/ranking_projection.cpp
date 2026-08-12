#include "snf/server/ranking_projection.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace
{
    const snf::server::PlayerScoreChanged& score_event(const snf::server::PlayerDomainEvent& event)
    {
        return std::get<snf::server::PlayerScoreChanged>(event);
    }
}

namespace snf::server
{
    InMemoryPlayerEventLog::InMemoryPlayerEventLog(const std::size_t capacity)
        : _capacity(capacity)
    {
        if (_capacity == 0)
        {
            throw std::invalid_argument{"Player event log capacity must be positive"};
        }
        if (_capacity > std::numeric_limits<std::uint64_t>::max())
        {
            throw std::invalid_argument{"Player event log capacity exceeds its offset range"};
        }
        _records.reserve(_capacity);
    }

    InMemoryPlayerEventLog::AppendResult InMemoryPlayerEventLog::append(PlayerDomainEvent event)
    {
        const PlayerId player = eventPlayer(event);
        const std::uint64_t sequence = eventSequence(event);
        if (player.value == 0 || sequence == 0)
        {
            throw std::invalid_argument{"Player event identity must be non-zero"};
        }

        const auto player_events = _by_player_sequence.find(player);
        if (player_events != _by_player_sequence.end())
        {
            if (const auto existing = player_events->second.find(sequence);
                existing != player_events->second.end())
            {
                return AppendResult{
                    .status = existing->second.event == event ? PlayerEventPublishResult::Duplicate
                                                              : PlayerEventPublishResult::Conflict,
                    .record =
                        PlayerEventRecord{
                            .offset = existing->second.offset,
                            .event = existing->second.event,
                        },
                };
            }
        }

        const auto last = _last_sequence.find(player);
        const std::uint64_t last_sequence = last == _last_sequence.end() ? 0 : last->second;
        if (last_sequence == std::numeric_limits<std::uint64_t>::max() ||
            sequence != last_sequence + 1)
        {
            return AppendResult{
                .status = PlayerEventPublishResult::OutOfOrder,
                .record = std::nullopt,
            };
        }
        if (_records.size() == _capacity)
        {
            return AppendResult{
                .status = PlayerEventPublishResult::Full,
                .record = std::nullopt,
            };
        }

        const auto offset = static_cast<std::uint64_t>(_records.size()) + 1;
        PlayerEventRecord record{
            .offset = offset,
            .event = std::move(event),
        };
        _records.push_back(record);
        _by_player_sequence[player].emplace(sequence,
                                            StoredEvent{
                                                .event = record.event,
                                                .offset = offset,
                                            });
        _last_sequence.insert_or_assign(player, sequence);
        return AppendResult{
            .status = PlayerEventPublishResult::Published,
            .record = std::move(record),
        };
    }

    std::vector<PlayerEventRecord>
    InMemoryPlayerEventLog::recordsAfter(const std::uint64_t offset) const
    {
        if (offset > _records.size())
        {
            throw std::out_of_range{"Player event log offset is beyond its tail"};
        }
        return std::vector<PlayerEventRecord>{_records.begin() + static_cast<std::size_t>(offset),
                                              _records.end()};
    }

    std::size_t InMemoryPlayerEventLog::size() const noexcept
    {
        return _records.size();
    }

    std::size_t InMemoryPlayerEventLog::capacity() const noexcept
    {
        return _capacity;
    }

    ProjectionApplyResult RankingProjection::apply(const PlayerEventRecord& record)
    {
        if (record.offset <= _offset)
        {
            return ProjectionApplyResult::Duplicate;
        }
        if (_offset == std::numeric_limits<std::uint64_t>::max() || record.offset != _offset + 1)
        {
            return ProjectionApplyResult::OutOfOrder;
        }

        const PlayerScoreChanged& event = score_event(record.event);
        if (event.player.value == 0 || event.sequence == 0)
        {
            return ProjectionApplyResult::Conflict;
        }

        const auto existing = _entries.find(event.player);
        const std::uint64_t last_sequence =
            existing == _entries.end() ? 0 : existing->second.last_sequence;
        const std::uint64_t last_score = existing == _entries.end() ? 0 : existing->second.score;
        if (event.sequence <= last_sequence)
        {
            return event.sequence == last_sequence && event.score != last_score
                       ? ProjectionApplyResult::Conflict
                       : ProjectionApplyResult::OutOfOrder;
        }
        if (last_sequence == std::numeric_limits<std::uint64_t>::max() ||
            event.sequence != last_sequence + 1 || event.score < last_score)
        {
            return ProjectionApplyResult::OutOfOrder;
        }

        _entries.insert_or_assign(event.player,
                                  RankingEntry{
                                      .player = event.player,
                                      .score = event.score,
                                      .last_sequence = event.sequence,
                                  });
        _offset = record.offset;
        return ProjectionApplyResult::Applied;
    }

    ProjectionApplyResult RankingProjection::replay(const std::vector<PlayerEventRecord>& records)
    {
        ProjectionApplyResult result = ProjectionApplyResult::Duplicate;
        for (const PlayerEventRecord& record : records)
        {
            result = apply(record);
            if (result != ProjectionApplyResult::Applied &&
                result != ProjectionApplyResult::Duplicate)
            {
                return result;
            }
        }
        return result;
    }

    RankingCheckpoint RankingProjection::checkpoint() const
    {
        std::vector<RankingEntry> entries;
        entries.reserve(_entries.size());
        for (const auto& [player, entry] : _entries)
        {
            static_cast<void>(player);
            entries.push_back(entry);
        }
        std::sort(entries.begin(),
                  entries.end(),
                  [](const RankingEntry& left, const RankingEntry& right)
                  { return left.player.value < right.player.value; });
        return RankingCheckpoint{
            .offset = _offset,
            .entries = std::move(entries),
        };
    }

    void RankingProjection::restore(const RankingCheckpoint& checkpoint)
    {
        std::unordered_map<PlayerId, RankingEntry, PlayerIdHash> restored;
        restored.reserve(checkpoint.entries.size());
        for (const RankingEntry& entry : checkpoint.entries)
        {
            if (entry.player.value == 0 || entry.last_sequence == 0 ||
                entry.last_sequence > checkpoint.offset ||
                !restored.emplace(entry.player, entry).second)
            {
                throw std::invalid_argument{"Ranking checkpoint contains an invalid entry"};
            }
        }
        _offset = checkpoint.offset;
        _entries = std::move(restored);
    }

    std::optional<std::uint64_t> RankingProjection::scoreFor(const PlayerId player) const
    {
        const auto iterator = _entries.find(player);
        return iterator == _entries.end() ? std::nullopt : std::optional{iterator->second.score};
    }

    std::vector<RankingEntry> RankingProjection::standings() const
    {
        std::vector<RankingEntry> result = checkpoint().entries;
        std::sort(result.begin(),
                  result.end(),
                  [](const RankingEntry& left, const RankingEntry& right)
                  {
                      return left.score != right.score ? left.score > right.score
                                                       : left.player.value < right.player.value;
                  });
        return result;
    }

    std::uint64_t RankingProjection::offset() const noexcept
    {
        return _offset;
    }

    InMemoryRankingEventPipeline::InMemoryRankingEventPipeline(const std::size_t event_capacity)
        : _log(event_capacity)
    {
    }

    PlayerEventPublishResult InMemoryRankingEventPipeline::publish(PlayerDomainEvent event)
    {
        std::lock_guard lock{_mutex};
        InMemoryPlayerEventLog::AppendResult appended = _log.append(std::move(event));
        switch (appended.status)
        {
        case PlayerEventPublishResult::Published:
            if (!appended.record ||
                _projection.apply(*appended.record) != ProjectionApplyResult::Applied)
            {
                throw std::logic_error{"New player event did not advance its projection"};
            }
            ++_published;
            break;
        case PlayerEventPublishResult::Duplicate:
            ++_duplicates;
            break;
        case PlayerEventPublishResult::Conflict:
        case PlayerEventPublishResult::OutOfOrder:
        case PlayerEventPublishResult::Full:
            ++_rejected;
            break;
        }
        return appended.status;
    }

    RankingCheckpoint InMemoryRankingEventPipeline::checkpoint() const
    {
        std::lock_guard lock{_mutex};
        return _projection.checkpoint();
    }

    std::vector<PlayerEventRecord>
    InMemoryRankingEventPipeline::recordsAfter(const std::uint64_t offset) const
    {
        std::lock_guard lock{_mutex};
        return _log.recordsAfter(offset);
    }

    std::vector<RankingEntry> InMemoryRankingEventPipeline::standings() const
    {
        std::lock_guard lock{_mutex};
        return _projection.standings();
    }

    RankingPipelineStats InMemoryRankingEventPipeline::stats() const
    {
        std::lock_guard lock{_mutex};
        return RankingPipelineStats{
            .published = _published,
            .duplicates = _duplicates,
            .rejected = _rejected,
            .event_count = _log.size(),
            .projection_offset = _projection.offset(),
        };
    }
}
