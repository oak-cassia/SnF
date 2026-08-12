#pragma once

#include "snf/server/player_id.hpp"

#include <cstdint>

namespace snf::server
{
    struct RankingAwardId
    {
        std::uint64_t value{0};

        [[nodiscard]] bool operator==(const RankingAwardId&) const noexcept = default;
    };

    enum class RankingAwardStatus
    {
        Committed,
        IdempotencyConflict,
        ScoreOverflow,
        SequenceOverflow,
        EventOffsetOverflow,
        CapacityExceeded,
        Unavailable,
    };

    struct RankingAwardRequest
    {
        PlayerId player;
        RankingAwardId award_id;
        std::uint64_t score_delta{0};
    };

    struct RankingAwardTransactionResult
    {
        RankingAwardStatus status{RankingAwardStatus::Unavailable};
        PlayerId player;
        RankingAwardId award_id;
        std::uint64_t score_delta{0};
        // Identity and value of the originally committed outbox event. On a
        // conflict these identify the row whose award_id was reused.
        std::uint64_t event_sequence{0};
        std::uint64_t event_score{0};
        std::uint64_t global_offset{0};
        // Current Player row values. A replay of an old award must not roll an
        // already newer Actor back to the event's historical absolute value.
        std::uint64_t authoritative_score{0};
        std::uint64_t authoritative_sequence{0};
        bool replayed{false};
    };
}
