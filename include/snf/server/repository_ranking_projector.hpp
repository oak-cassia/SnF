#pragma once

#include "snf/server/ranking_projection.hpp"
#include "snf/server/ranking_store.hpp"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

namespace snf::server
{
    struct RepositoryRankingProjectorConfig
    {
        std::size_t batch_size{1024};
        std::size_t max_batches_per_poll{8};
        std::uint64_t checkpoint_every_events{1024};
        std::chrono::milliseconds poll_interval{100};
    };

    // A single blocking projector thread owns ordered application and durable
    // checkpoints. Construction restores and catches up synchronously, before a
    // GameServer can accept gameplay.
    class RepositoryRankingProjector final
    {
    public:
        explicit RepositoryRankingProjector(
            RankingStore& store,
            RepositoryRankingProjectorConfig config = RepositoryRankingProjectorConfig{});
        ~RepositoryRankingProjector();

        RepositoryRankingProjector(const RepositoryRankingProjector&) = delete;
        RepositoryRankingProjector& operator=(const RepositoryRankingProjector&) = delete;

        void stop() noexcept;
        [[nodiscard]] std::vector<RankingEntry> standings() const;
        [[nodiscard]] RankingPipelineStats stats() const;

    private:
        [[nodiscard]] bool catchUpOnce();
        void catchUpAll();
        void pollOnce();
        void saveCheckpoint(bool force);
        void run() noexcept;

        RankingStore& _store;
        RepositoryRankingProjectorConfig _config;
        mutable std::mutex _projection_mutex;
        RankingProjection _projection;
        std::uint64_t _applied{0};
        std::uint64_t _duplicates{0};
        std::uint64_t _rejected{0};
        std::uint64_t _poll_failures{0};
        std::uint64_t _checkpoint_failures{0};
        std::uint64_t _committed_tail_offset{0};
        std::uint64_t _checkpoint_offset{0};
        snf::runtime::Distribution _poll_latency;
        snf::runtime::Distribution _checkpoint_latency;
        std::mutex _control_mutex;
        std::condition_variable _control;
        bool _stopping{false};
        std::thread _thread;
    };
}
