#include "snf/server/player_repository.hpp"

#include <stdexcept>
#include <type_traits>
#include <utility>

namespace snf::server
{
    void InMemoryPlayerRepository::asyncLoad(const PlayerId player, PlayerLoadCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Player load completion must be callable"};
        }

        std::optional<PlayerRecord> record;
        {
            std::lock_guard lock{_mutex};
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

    void InMemoryPlayerRepository::asyncSave(PlayerRecord record, PlayerSaveCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Player save completion must be callable"};
        }

        {
            std::lock_guard lock{_mutex};
            _records.insert_or_assign(record.player, std::move(record));
        }

        completion(PlayerSaveResult{.status = PlayerRepositoryStatus::Success});
    }

    std::optional<PlayerRecord> InMemoryPlayerRepository::find(const PlayerId player) const
    {
        std::lock_guard lock{_mutex};
        const auto iterator = _records.find(player);
        if (iterator == _records.end())
        {
            return std::nullopt;
        }

        return iterator->second;
    }

    ThreadedPlayerRepository::ThreadedPlayerRepository(ThreadedPlayerRepositoryConfig config)
        : _jobs(config.queue_capacity)
    {
        if (config.worker_count == 0)
        {
            throw std::invalid_argument{"Player repository worker count must be positive"};
        }

        try
        {
            _workers.reserve(config.worker_count);
            for (std::size_t index = 0; index < config.worker_count; ++index)
            {
                _workers.emplace_back([this] { runWorker(); });
            }
        }
        catch (...)
        {
            close();
            for (std::thread& worker : _workers)
            {
                worker.join();
            }
            throw;
        }
    }

    ThreadedPlayerRepository::~ThreadedPlayerRepository()
    {
        close();
        for (std::thread& worker : _workers)
        {
            if (worker.joinable())
            {
                worker.join();
            }
        }
    }

    void ThreadedPlayerRepository::asyncLoad(const PlayerId player, PlayerLoadCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Player load completion must be callable"};
        }

        if (_jobs.tryPush(Job{LoadJob{.player = player, .completion = completion}}))
        {
            _accepted.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        _rejected.fetch_add(1, std::memory_order_relaxed);
        completion(PlayerLoadResult{
            .status = PlayerRepositoryStatus::Unavailable,
            .record = std::nullopt,
        });
    }

    void ThreadedPlayerRepository::asyncSave(PlayerRecord record, PlayerSaveCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Player save completion must be callable"};
        }

        if (_jobs.tryPush(Job{SaveJob{.record = std::move(record), .completion = completion}}))
        {
            _accepted.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        _rejected.fetch_add(1, std::memory_order_relaxed);
        completion(PlayerSaveResult{.status = PlayerRepositoryStatus::Unavailable});
    }

    void ThreadedPlayerRepository::close() noexcept
    {
        _jobs.close();
    }

    std::optional<PlayerRecord> ThreadedPlayerRepository::find(const PlayerId player) const
    {
        return _storage.find(player);
    }

    ThreadedPlayerRepositoryStats ThreadedPlayerRepository::stats() const
    {
        return ThreadedPlayerRepositoryStats{
            .accepted = _accepted.load(std::memory_order_relaxed),
            .rejected = _rejected.load(std::memory_order_relaxed),
            .queue_depth = _jobs.size(),
            .queue_high_water_mark = _jobs.highWaterMark(),
        };
    }

    void ThreadedPlayerRepository::runWorker()
    {
        while (auto job = _jobs.pop())
        {
            std::visit(
                [this](auto value)
                {
                    using Value = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<Value, LoadJob>)
                    {
                        _storage.asyncLoad(value.player, std::move(value.completion));
                    }
                    else
                    {
                        _storage.asyncSave(std::move(value.record), std::move(value.completion));
                    }
                },
                std::move(*job));
        }
    }
}
