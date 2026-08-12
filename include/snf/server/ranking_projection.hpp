#pragma once

#include "snf/server/player_domain_event.hpp"
#include "snf/server/player_domain_event_sink.hpp"

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

namespace snf::server
{
    struct RankingEntry
    {
        PlayerId player;
        std::uint64_t score{0};
        std::uint64_t last_sequence{0};

        [[nodiscard]] bool operator==(const RankingEntry&) const noexcept = default;
    };

    struct RankingCheckpoint
    {
        std::uint64_t offset{0};
        std::vector<RankingEntry> entries;
    };

    enum class ProjectionApplyResult
    {
        Applied,
        Duplicate,
        Conflict,
        OutOfOrder,
    };

    class InMemoryPlayerEventLog
    {
    public:
        explicit InMemoryPlayerEventLog(std::size_t capacity = 65536);

        struct AppendResult
        {
            PlayerEventPublishResult status{PlayerEventPublishResult::Conflict};
            std::optional<PlayerEventRecord> record;
        };

        [[nodiscard]] AppendResult append(PlayerDomainEvent event);
        [[nodiscard]] std::vector<PlayerEventRecord> recordsAfter(std::uint64_t offset) const;
        [[nodiscard]] std::size_t size() const noexcept;
        [[nodiscard]] std::size_t capacity() const noexcept;

    private:
        struct StoredEvent
        {
            PlayerDomainEvent event;
            std::uint64_t offset{0};
        };

        const std::size_t _capacity;
        std::vector<PlayerEventRecord> _records;
        std::unordered_map<PlayerId, std::unordered_map<std::uint64_t, StoredEvent>, PlayerIdHash>
            _by_player_sequence;
        std::unordered_map<PlayerId, std::uint64_t, PlayerIdHash> _last_sequence;
    };

    class RankingProjection
    {
    public:
        [[nodiscard]] ProjectionApplyResult apply(const PlayerEventRecord& record);
        [[nodiscard]] ProjectionApplyResult replay(const std::vector<PlayerEventRecord>& records);
        [[nodiscard]] RankingCheckpoint checkpoint() const;
        void restore(const RankingCheckpoint& checkpoint);

        [[nodiscard]] std::optional<std::uint64_t> scoreFor(PlayerId player) const;
        [[nodiscard]] std::vector<RankingEntry> standings() const;
        [[nodiscard]] std::uint64_t offset() const noexcept;

    private:
        std::uint64_t _offset{0};
        std::unordered_map<PlayerId, RankingEntry, PlayerIdHash> _entries;
    };

    struct RankingPipelineStats
    {
        std::uint64_t published{0};
        std::uint64_t duplicates{0};
        std::uint64_t rejected{0};
        std::size_t event_count{0};
        std::uint64_t projection_offset{0};
    };

    class InMemoryRankingEventPipeline final : public PlayerDomainEventSink
    {
    public:
        explicit InMemoryRankingEventPipeline(std::size_t event_capacity = 65536);

        [[nodiscard]] PlayerEventPublishResult publish(PlayerDomainEvent event) override;
        [[nodiscard]] RankingCheckpoint checkpoint() const;
        [[nodiscard]] std::vector<PlayerEventRecord> recordsAfter(std::uint64_t offset) const;
        [[nodiscard]] std::vector<RankingEntry> standings() const;
        [[nodiscard]] RankingPipelineStats stats() const;

    private:
        mutable std::mutex _mutex;
        InMemoryPlayerEventLog _log;
        RankingProjection _projection;
        std::uint64_t _published{0};
        std::uint64_t _duplicates{0};
        std::uint64_t _rejected{0};
    };
}
