#pragma once

#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/player_repository.hpp"

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace snf::server
{
    struct PlayerPersistenceServiceConfig
    {
        std::size_t queue_capacity{4096};
        std::chrono::milliseconds flush_interval{100};
    };

    struct PlayerPersistenceServiceStats
    {
        std::uint64_t snapshots_accepted{0};
        std::uint64_t snapshots_rejected{0};
        std::uint64_t saves_started{0};
        std::uint64_t saves_succeeded{0};
        std::uint64_t saves_failed{0};
        std::uint64_t background_retries{0};
        std::uint64_t final_saves{0};
        std::uint64_t final_failures{0};
        std::size_t queue_depth{0};
        std::size_t queue_high_water_mark{0};
        std::size_t pending_players{0};
        std::size_t in_flight_players{0};
    };

    // Owns the only production path that writes Player snapshots. Actor Workers
    // only submit immutable flat records; this service coalesces them and serializes
    // saves per Player while allowing different Players to share the repository.
    class PlayerPersistenceService final
    {
    public:
        explicit PlayerPersistenceService(
            PlayerRepository& repository,
            PlayerPersistenceServiceConfig config = PlayerPersistenceServiceConfig{});
        ~PlayerPersistenceService();

        PlayerPersistenceService(const PlayerPersistenceService&) = delete;
        PlayerPersistenceService& operator=(const PlayerPersistenceService&) = delete;

        // Non-blocking dirty snapshot admission. A false result preserves the
        // Actor's dirty mask so its next command can retry naturally.
        [[nodiscard]] bool tryEnqueue(PlayerRecord record) noexcept;

        // Final/logout save. It is serialized behind any save already in flight for
        // the same Player. The completion is delivered with repository status and is
        // intentionally awaited by the owning Actor binding.
        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion);

        // Requests an immediate drain and waits until accepted snapshots have been
        // attempted and final requests have reached a terminal repository result.
        // A failed background snapshot remains retryable for the next timer tick;
        // a final save failure is reported to its caller.
        void flush();
        void stop() noexcept;

        [[nodiscard]] PlayerPersistenceServiceStats stats() const;

    private:
        struct SnapshotJob
        {
            PlayerRecord record;
        };

        struct FinalRequest
        {
            PlayerRecord record;
            PlayerSaveCompletion completion;
        };

        struct InFlight
        {
            PlayerRecord record;
            bool final{false};
            PlayerSaveCompletion completion;
        };

        struct StartSave
        {
            PlayerId player;
            PlayerRecord record;
            bool final{false};
        };

        using SnapshotQueue = snf::runtime::BoundedQueue<SnapshotJob>;
        using PendingMap = std::unordered_map<PlayerId, PlayerRecord, PlayerIdHash>;
        using FinalMap = std::unordered_map<PlayerId, std::deque<FinalRequest>, PlayerIdHash>;
        using InFlightMap = std::unordered_map<PlayerId, InFlight, PlayerIdHash>;

        void run() noexcept;
        void processOnce();
        void drainSnapshots();
        [[nodiscard]] std::vector<StartSave> selectSaves();
        void startSave(StartSave save) noexcept;
        void completeSave(PlayerId player, PlayerSaveResult result) noexcept;
        void notifyCompletion(PlayerSaveCompletion& completion, PlayerSaveResult result) noexcept;
        [[nodiscard]] bool isFlushCompleteLocked() const noexcept;

        PlayerRepository& _repository;
        const std::chrono::milliseconds _flush_interval;
        SnapshotQueue _snapshots;

        mutable std::mutex _mutex;
        std::condition_variable _wake;
        std::condition_variable _idle;
        PendingMap _pending;
        FinalMap _final_requests;
        InFlightMap _in_flight;
        std::thread _thread;
        bool _stopping{false};
        bool _stopped{false};
        bool _flush_requested{false};

        std::uint64_t _snapshots_accepted{0};
        std::uint64_t _snapshots_rejected{0};
        std::uint64_t _saves_started{0};
        std::uint64_t _saves_succeeded{0};
        std::uint64_t _saves_failed{0};
        std::uint64_t _background_retries{0};
        std::uint64_t _final_saves{0};
        std::uint64_t _final_failures{0};
    };
}
