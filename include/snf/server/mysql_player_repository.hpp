#pragma once

#include "snf/server/player_repository.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace snf::server
{
    enum class MySqlPurchaseFaultPoint
    {
        BeforeCommit,
        AfterCommitBeforeCompletion,
    };

    enum class MySqlRankingAwardFaultPoint
    {
        BeforeCommit,
        AfterCommitBeforeCompletion,
    };

    enum class MySqlRankingCheckpointFaultPoint
    {
        BeforePointerSwap,
        BeforeCommit,
        AfterCommit,
    };

    struct MySqlPlayerRepositoryConfig
    {
        std::string host{"127.0.0.1"};
        std::uint16_t port{3306};
        std::string user;
        std::string password;
        std::string database{"snf"};
        std::size_t worker_count{2};
        std::size_t queue_capacity{4096};
        std::size_t max_idempotency_records_per_player{1024};
        std::chrono::seconds connect_timeout{5};
        std::chrono::seconds read_timeout{5};
        std::chrono::seconds write_timeout{5};
        // Test-only crash/failure injection. Production leaves this empty.
        std::function<void(MySqlPurchaseFaultPoint)> purchase_fault_injector;
        std::function<void(MySqlRankingAwardFaultPoint)> ranking_award_fault_injector;
        std::function<void(MySqlRankingCheckpointFaultPoint)> ranking_checkpoint_fault_injector;
    };

    // Durable blocking adapter. Its bounded queue is non-blocking at the Actor
    // boundary and each repository Worker owns one MySQL connection at a time.
    class MySqlPlayerRepository final : public PlayerRepository,
                                        public PlayerRepositoryDiagnostics,
                                        public RankingStore
    {
    public:
        explicit MySqlPlayerRepository(MySqlPlayerRepositoryConfig config);
        ~MySqlPlayerRepository();

        MySqlPlayerRepository(const MySqlPlayerRepository&) = delete;
        MySqlPlayerRepository& operator=(const MySqlPlayerRepository&) = delete;
        MySqlPlayerRepository(MySqlPlayerRepository&&) = delete;
        MySqlPlayerRepository& operator=(MySqlPlayerRepository&&) = delete;

        void asyncLoad(PlayerId player, PlayerLoadCompletion completion) override;
        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) override;
        void asyncPurchase(PurchaseRequest request, PurchaseCompletion completion) override;
        void asyncAwardRankingScore(RankingAwardRequest request,
                                    RankingAwardCompletion completion) override;

        void close() noexcept;
        [[nodiscard]] std::optional<PlayerRecord> find(PlayerId player) const override;
        [[nodiscard]] PlayerRepositoryStats stats() const override;
        [[nodiscard]] RankingCheckpoint loadRankingCheckpoint() const override;
        void saveRankingCheckpoint(const RankingCheckpoint& checkpoint) override;
        [[nodiscard]] std::uint64_t rankingTailOffset() const override;
        // Blocking projector reads. Never call these from an Actor Worker or the
        // network reactor.
        [[nodiscard]] std::vector<PlayerEventRecord>
        rankingEventsAfter(std::uint64_t offset, std::size_t limit = 1024) const override;

    private:
        class Impl;
        std::unique_ptr<Impl> _impl;
    };
}
