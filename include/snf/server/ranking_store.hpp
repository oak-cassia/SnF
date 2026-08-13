#pragma once

#include "snf/server/ranking_projection.hpp"

#include <cstddef>
#include <cstdint>
#include <vector>

namespace snf::server
{
    // Blocking storage surface for the dedicated ranking projector. Actor Workers
    // and the network reactor must never call these methods.
    class RankingStore
    {
    public:
        virtual ~RankingStore() = default;

        [[nodiscard]] virtual RankingCheckpoint loadRankingCheckpoint() const = 0;
        virtual void saveRankingCheckpoint(const RankingCheckpoint& checkpoint) = 0;
        [[nodiscard]] virtual std::uint64_t rankingTailOffset() const = 0;
        [[nodiscard]] virtual std::vector<PlayerEventRecord>
        rankingEventsAfter(std::uint64_t offset, std::size_t limit) const = 0;
    };
}
