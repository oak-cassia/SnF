#include "snf/server/player_repository.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    void InMemoryPlayerRepository::asyncLoad(const PlayerId player,
                                             PlayerLoadCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Player load completion must be callable"};
        }

        std::optional<PlayerRecord> record;
        {
            std::lock_guard lock{_mutex};
            ++_accepted;
            const auto iterator = _records.find(player);
            if (iterator != _records.end())
            {
                record = iterator->second;
            }
        }

        completion(PlayerLoadResult{
            .status = PlayerRepositoryStatus::Success,
            .record = std::move(record),
        });
    }

    void InMemoryPlayerRepository::asyncSave(PlayerRecord record,
                                             PlayerSaveCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Player save completion must be callable"};
        }

        {
            std::lock_guard lock{_mutex};
            ++_accepted;
            _records.insert_or_assign(record.player, std::move(record));
        }

        completion(PlayerSaveResult{.status = PlayerRepositoryStatus::Success});
    }

    std::optional<PlayerRecord> InMemoryPlayerRepository::find(const PlayerId player) const
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _records.find(player);
        return iterator == _records.end() ? std::nullopt : std::optional{iterator->second};
    }

    PlayerRepositoryStats InMemoryPlayerRepository::stats() const
    {
        std::lock_guard lock{_mutex};
        return PlayerRepositoryStats{
            .accepted = _accepted,
            .rejected = 0,
            .queue_depth = 0,
            .queue_high_water_mark = 0,
            .operation_failures = 0,
            .operation_latency_nanoseconds = {},
        };
    }
}
