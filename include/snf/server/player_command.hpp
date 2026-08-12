#pragma once

#include "snf/server/player_id.hpp"
#include "snf/server/purchase.hpp"
#include "snf/server/ranking_award.hpp"

#include <cstddef>
#include <cstdint>
#include <variant>
#include <vector>

namespace snf::server
{
    struct PingCommand
    {
        std::uint32_t request_id{0};
        std::vector<std::byte> payload;
    };

    struct AuthenticateCommand
    {
        std::uint32_t request_id{0};
        PlayerId player;
    };

    struct PurchaseCommand
    {
        std::uint32_t request_id{0};
        PurchaseIdempotencyKey idempotency_key;
        ProductId product;
    };

    // Trusted gameplay input. It is deliberately absent from the client protocol:
    // clients may observe ranking, but never award their own score.
    struct AwardRankingScoreCommand
    {
        std::uint32_t request_id{0};
        RankingAwardId award_id;
        std::uint64_t score_delta{0};
    };

    using PlayerCommand =
        std::variant<PingCommand, AuthenticateCommand, PurchaseCommand, AwardRankingScoreCommand>;

    [[nodiscard]] inline std::uint32_t requestId(const PlayerCommand& command) noexcept
    {
        return std::visit([](const auto& value) { return value.request_id; }, command);
    }
}
