#pragma once

#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/player_id.hpp"
#include "snf/server/player_record.hpp"

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

    // The repository receives values and completion callbacks only. An adapter
    // may run blocking storage work elsewhere, but it never receives an Actor,
    // ActorSlot, coroutine handle, or mutable runtime object.
    class PlayerRepository
    {
    public:
        virtual ~PlayerRepository() = default;

        virtual void asyncLoad(PlayerId player, PlayerLoadCompletion completion) = 0;
        virtual void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) = 0;
    };

    // Deterministic first adapter for the vertical slice. Completion is immediate,
    // but still crosses the Actor continuation queue because the binding wraps it
    // in an async operation. The mutex also makes it safe for integration tests to
    // inspect records while the owning Worker may complete a save.
    class InMemoryPlayerRepository final : public PlayerRepository
    {
    public:
        void asyncLoad(PlayerId player, PlayerLoadCompletion completion) override;
        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) override;

        [[nodiscard]] std::optional<PlayerRecord> find(PlayerId player) const;

    private:
        mutable std::mutex _mutex;
        std::unordered_map<PlayerId, PlayerRecord, PlayerIdHash> _records;
    };

    struct ThreadedPlayerRepositoryConfig
    {
        std::size_t worker_count{1};
        std::size_t queue_capacity{4096};
    };

    struct ThreadedPlayerRepositoryStats
    {
        std::uint64_t accepted{0};
        std::uint64_t rejected{0};
        std::size_t queue_depth{0};
        std::size_t queue_high_water_mark{0};
    };

    // Blocking-adapter boundary. Jobs are admitted non-blockingly into a bounded
    // FIFO and executed only by the repository Workers. The in-memory storage is
    // deterministic here; replacing the job body with a DB client does not change
    // the Actor-facing completion contract.
    class ThreadedPlayerRepository final : public PlayerRepository
    {
    public:
        explicit ThreadedPlayerRepository(ThreadedPlayerRepositoryConfig config = {});
        ~ThreadedPlayerRepository();

        ThreadedPlayerRepository(const ThreadedPlayerRepository&) = delete;
        ThreadedPlayerRepository& operator=(const ThreadedPlayerRepository&) = delete;

        void asyncLoad(PlayerId player, PlayerLoadCompletion completion) override;
        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) override;

        void close() noexcept;
        [[nodiscard]] std::optional<PlayerRecord> find(PlayerId player) const;
        [[nodiscard]] ThreadedPlayerRepositoryStats stats() const;

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

        using Job = std::variant<LoadJob, SaveJob>;

        void runWorker();

        InMemoryPlayerRepository _storage;
        snf::runtime::BoundedQueue<Job> _jobs;
        std::vector<std::thread> _workers;
        std::atomic<std::uint64_t> _accepted{0};
        std::atomic<std::uint64_t> _rejected{0};
    };
}
