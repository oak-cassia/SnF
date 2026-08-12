#pragma once

#include "snf/runtime/bounded_queue.hpp"
#include "snf/runtime/distribution.hpp"
#include "snf/server/player_domain_event.hpp"
#include "snf/server/player_id.hpp"
#include "snf/server/player_record.hpp"
#include "snf/server/purchase.hpp"
#include "snf/server/ranking_award.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <variant>
#include <vector>

namespace snf::server
{
    enum class PlayerRepositoryStatus
    {
        Success,
        Unavailable,
    };

    struct PlayerLoadResult
    {
        PlayerRepositoryStatus status{PlayerRepositoryStatus::Success};
        std::optional<PlayerRecord> record;
    };

    struct PlayerSaveResult
    {
        PlayerRepositoryStatus status{PlayerRepositoryStatus::Success};

        [[nodiscard]] bool saved() const noexcept
        {
            return status == PlayerRepositoryStatus::Success;
        }
    };

    using PlayerLoadCompletion = std::function<void(PlayerLoadResult)>;
    using PlayerSaveCompletion = std::function<void(PlayerSaveResult)>;
    using PurchaseCompletion = std::function<void(PurchaseTransactionResult)>;
    using RankingAwardCompletion = std::function<void(RankingAwardTransactionResult)>;

    struct PlayerRepositoryStats
    {
        std::uint64_t accepted{0};
        std::uint64_t rejected{0};
        std::size_t queue_depth{0};
        std::size_t queue_high_water_mark{0};
        std::uint64_t purchase_committed{0};
        std::uint64_t purchase_replayed{0};
        std::uint64_t purchase_rejected{0};
        std::uint64_t ranking_awards_committed{0};
        std::uint64_t ranking_awards_replayed{0};
        std::uint64_t ranking_awards_rejected{0};
        std::uint64_t operation_failures{0};
        snf::runtime::DistributionSnapshot operation_latency_nanoseconds;
    };

    // Read-only diagnostics shared by the in-memory and durable adapters. It is
    // deliberately separate from PlayerRepository so deterministic test fakes do
    // not need to expose storage internals.
    class PlayerRepositoryDiagnostics
    {
    public:
        virtual ~PlayerRepositoryDiagnostics() = default;
        [[nodiscard]] virtual std::optional<PlayerRecord> find(PlayerId player) const = 0;
        [[nodiscard]] virtual PlayerRepositoryStats stats() const = 0;
    };

    // The repository receives values and completion callbacks only. An adapter
    // may run blocking storage work elsewhere, but it never receives an Actor,
    // ActorSlot, coroutine handle, or mutable runtime object.
    class PlayerRepository
    {
    public:
        virtual ~PlayerRepository() = default;

        virtual void asyncLoad(PlayerId player, PlayerLoadCompletion completion) = 0;
        virtual void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) = 0;
        virtual void asyncPurchase(PurchaseRequest request, PurchaseCompletion completion) = 0;
        virtual void asyncAwardRankingScore(RankingAwardRequest request,
                                            RankingAwardCompletion completion) = 0;
    };

    // Deterministic first adapter for the vertical slice. Completion is immediate,
    // but still crosses the Actor continuation queue because the binding wraps it
    // in an async operation. The mutex also makes it safe for integration tests to
    // inspect records while the owning Worker may complete a save.
    class InMemoryPlayerRepository final : public PlayerRepository
    {
    public:
        explicit InMemoryPlayerRepository(std::size_t max_idempotency_records_per_player = 1024,
                                          std::size_t max_ranking_events = 65536);

        void asyncLoad(PlayerId player, PlayerLoadCompletion completion) override;
        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) override;
        void asyncPurchase(PurchaseRequest request, PurchaseCompletion completion) override;
        void asyncAwardRankingScore(RankingAwardRequest request,
                                    RankingAwardCompletion completion) override;

        [[nodiscard]] std::optional<PlayerRecord> find(PlayerId player) const;
        [[nodiscard]] std::vector<PlayerEventRecord> rankingEventsAfter(std::uint64_t offset) const;

        struct PurchaseStats
        {
            std::uint64_t committed{0};
            std::uint64_t replayed{0};
            std::uint64_t rejected{0};
        };
        [[nodiscard]] PurchaseStats purchaseStats() const;

        struct RankingAwardStats
        {
            std::uint64_t committed{0};
            std::uint64_t replayed{0};
            std::uint64_t rejected{0};
        };
        [[nodiscard]] RankingAwardStats rankingAwardStats() const;

    private:
        struct StoredPurchase
        {
            ProductId product;
            PurchaseTransactionResult result;
        };

        struct StoredRankingAward
        {
            std::uint64_t score_delta{0};
            RankingAwardTransactionResult result;
        };

        const std::size_t _max_idempotency_records_per_player;
        const std::size_t _max_ranking_events;
        mutable std::mutex _mutex;
        std::unordered_map<PlayerId, PlayerRecord, PlayerIdHash> _records;
        std::
            unordered_map<PlayerId, std::unordered_map<std::uint64_t, StoredPurchase>, PlayerIdHash>
                _purchases;
        std::unordered_map<PlayerId,
                           std::unordered_map<std::uint64_t, StoredRankingAward>,
                           PlayerIdHash>
            _ranking_awards;
        std::vector<PlayerEventRecord> _ranking_events;
        PurchaseStats _purchase_stats;
        RankingAwardStats _ranking_award_stats;
    };

    struct ThreadedPlayerRepositoryConfig
    {
        std::size_t worker_count{1};
        std::size_t queue_capacity{4096};
        std::size_t max_idempotency_records_per_player{1024};
        std::size_t max_ranking_events{65536};
    };

    using ThreadedPlayerRepositoryStats = PlayerRepositoryStats;

    // Blocking-adapter boundary. Jobs are admitted non-blockingly into a bounded
    // FIFO and executed only by the repository Workers. The in-memory storage is
    // deterministic here; replacing the job body with a DB client does not change
    // the Actor-facing completion contract.
    class ThreadedPlayerRepository final : public PlayerRepository,
                                           public PlayerRepositoryDiagnostics
    {
    public:
        explicit ThreadedPlayerRepository(ThreadedPlayerRepositoryConfig config = {});
        ~ThreadedPlayerRepository();

        ThreadedPlayerRepository(const ThreadedPlayerRepository&) = delete;
        ThreadedPlayerRepository& operator=(const ThreadedPlayerRepository&) = delete;

        void asyncLoad(PlayerId player, PlayerLoadCompletion completion) override;
        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) override;
        void asyncPurchase(PurchaseRequest request, PurchaseCompletion completion) override;
        void asyncAwardRankingScore(RankingAwardRequest request,
                                    RankingAwardCompletion completion) override;

        void close() noexcept;
        [[nodiscard]] std::optional<PlayerRecord> find(PlayerId player) const override;
        [[nodiscard]] PlayerRepositoryStats stats() const override;

    private:
        struct LoadJob
        {
            PlayerId player;
            PlayerLoadCompletion completion;
        };

        struct SaveJob
        {
            PlayerRecord record;
            PlayerSaveCompletion completion;
        };

        struct PurchaseJob
        {
            PurchaseRequest request;
            PurchaseCompletion completion;
        };

        struct RankingAwardJob
        {
            RankingAwardRequest request;
            RankingAwardCompletion completion;
        };

        using Job = std::variant<LoadJob, SaveJob, PurchaseJob, RankingAwardJob>;

        void runWorker();

        InMemoryPlayerRepository _storage;
        snf::runtime::BoundedQueue<Job> _jobs;
        std::vector<std::thread> _workers;
        std::atomic<std::uint64_t> _accepted{0};
        std::atomic<std::uint64_t> _rejected{0};
    };
}
