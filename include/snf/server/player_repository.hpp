#pragma once

#include "snf/game/player_id.hpp"
#include "snf/game/player_record.hpp"
#include "snf/runtime/distribution.hpp"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <optional>
#include <unordered_map>

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

    struct PlayerRepositoryStats
    {
        std::uint64_t accepted{0};
        std::uint64_t rejected{0};
        std::size_t queue_depth{0};
        std::size_t queue_high_water_mark{0};
        std::uint64_t operation_failures{0};
        snf::runtime::DistributionSnapshot operation_latency_nanoseconds;
    };

    class PlayerRepositoryDiagnostics
    {
    public:
        virtual ~PlayerRepositoryDiagnostics() = default;
        [[nodiscard]] virtual std::optional<PlayerRecord> find(PlayerId player) const = 0;
        [[nodiscard]] virtual PlayerRepositoryStats stats() const = 0;
    };

    class PlayerRepository
    {
    public:
        virtual ~PlayerRepository() = default;

        virtual void asyncLoad(PlayerId player, PlayerLoadCompletion completion) = 0;
        virtual void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) = 0;
    };

    class InMemoryPlayerRepository final : public PlayerRepository, public PlayerRepositoryDiagnostics
    {
    public:
        void asyncLoad(PlayerId player, PlayerLoadCompletion completion) override;
        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion) override;

        [[nodiscard]] std::optional<PlayerRecord> find(PlayerId player) const override;
        [[nodiscard]] PlayerRepositoryStats stats() const override;

    private:
        mutable std::mutex _mutex;
        std::unordered_map<PlayerId, PlayerRecord, PlayerIdHash> _records;
        std::uint64_t _accepted{0};
    };
}
