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

    class PlayerPersistenceService final
    {
    public:
        explicit PlayerPersistenceService(PlayerRepository& repository, PlayerPersistenceServiceConfig config = PlayerPersistenceServiceConfig{});
        ~PlayerPersistenceService();

        PlayerPersistenceService(const PlayerPersistenceService&) = delete;
        PlayerPersistenceService& operator=(const PlayerPersistenceService&) = delete;

        [[nodiscard]] bool tryEnqueue(PlayerRecord record) noexcept;

        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion);

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
