#pragma once

#include "snf/runtime/distribution.hpp"
#include "snf/server/player_id.hpp"
#include "snf/server/player_record.hpp"

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
    };

    // Deterministic first adapter for the vertical slice. Completion is immediate,
    // but still crosses the Actor continuation queue because the binding wraps it
    // in an async operation. The mutex also makes it safe for integration tests to
    // inspect records while the owning Worker may complete a save.
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
