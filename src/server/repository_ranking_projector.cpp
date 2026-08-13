#include "snf/server/repository_ranking_projector.hpp"

#include <algorithm>
#include <limits>
#include <stdexcept>
#include <utility>

namespace snf::server
{
    RepositoryRankingProjector::RepositoryRankingProjector(RankingStore& store,
                                                           RepositoryRankingProjectorConfig config)
        : _store(store)
        , _config(config)
    {
        if (_config.batch_size == 0 || _config.max_batches_per_poll == 0 ||
            _config.checkpoint_every_events == 0 ||
            _config.poll_interval <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Ranking projector configuration must be positive"};
        }

        RankingCheckpoint checkpoint = _store.loadRankingCheckpoint();
        _projection.restore(checkpoint);
        _checkpoint_offset = checkpoint.offset;
        catchUpAll();
        saveCheckpoint(false);
        _thread = std::thread{[this] { run(); }};
    }

    RepositoryRankingProjector::~RepositoryRankingProjector()
    {
        stop();
    }

    void RepositoryRankingProjector::stop() noexcept
    {
        {
            std::lock_guard lock{_control_mutex};
            _stopping = true;
        }
        _control.notify_all();
        if (_thread.joinable())
        {
            _thread.join();
        }
    }

    std::vector<RankingEntry> RepositoryRankingProjector::standings() const
    {
        std::lock_guard lock{_projection_mutex};
        return _projection.standings();
    }

    RankingPipelineStats RepositoryRankingProjector::stats() const
    {
        std::lock_guard lock{_projection_mutex};
        const std::uint64_t offset = _projection.offset();
        const std::size_t event_count = offset > std::numeric_limits<std::size_t>::max()
                                            ? std::numeric_limits<std::size_t>::max()
                                            : static_cast<std::size_t>(offset);
        const std::uint64_t lag =
            _committed_tail_offset > offset ? _committed_tail_offset - offset : 0;
        return RankingPipelineStats{
            .published = _applied,
            .duplicates = _duplicates,
            .rejected = _rejected,
            .poll_failures = _poll_failures,
            .checkpoint_failures = _checkpoint_failures,
            .event_count = event_count,
            .committed_tail_offset = _committed_tail_offset,
            .projection_offset = offset,
            .projection_lag = lag,
            .checkpoint_offset = _checkpoint_offset,
            .poll_latency_nanoseconds = _poll_latency.snapshot(),
            .checkpoint_latency_nanoseconds = _checkpoint_latency.snapshot(),
        };
    }

    bool RepositoryRankingProjector::catchUpOnce()
    {
        std::uint64_t offset = 0;
        {
            std::lock_guard lock{_projection_mutex};
            offset = _projection.offset();
        }
        const std::uint64_t tail = _store.rankingTailOffset();
        {
            std::lock_guard lock{_projection_mutex};
            if (tail < _projection.offset())
            {
                ++_rejected;
                throw std::runtime_error{"Durable ranking tail regressed behind its projection"};
            }
            _committed_tail_offset = tail;
        }
        const std::vector<PlayerEventRecord> records =
            _store.rankingEventsAfter(offset, _config.batch_size);
        if (records.empty())
        {
            return false;
        }

        std::lock_guard lock{_projection_mutex};
        for (const PlayerEventRecord& record : records)
        {
            _committed_tail_offset = std::max(_committed_tail_offset, record.offset);
            const ProjectionApplyResult result = _projection.apply(record);
            if (result == ProjectionApplyResult::Applied)
            {
                ++_applied;
            }
            else if (result == ProjectionApplyResult::Duplicate)
            {
                ++_duplicates;
            }
            else
            {
                ++_rejected;
                throw std::runtime_error{"Durable ranking event violated projection order"};
            }
        }
        return true;
    }

    void RepositoryRankingProjector::catchUpAll()
    {
        const auto started_at = std::chrono::steady_clock::now();
        try
        {
            while (catchUpOnce())
            {
            }
        }
        catch (...)
        {
            _poll_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at));
            throw;
        }
        _poll_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at));
    }

    void RepositoryRankingProjector::pollOnce()
    {
        const auto started_at = std::chrono::steady_clock::now();
        bool read_failed = false;
        bool checkpoint_failed = false;
        for (std::size_t batch = 0; batch < _config.max_batches_per_poll; ++batch)
        {
            bool caught_up = false;
            try
            {
                caught_up = !catchUpOnce();
            }
            catch (...)
            {
                std::lock_guard lock{_projection_mutex};
                ++_poll_failures;
                read_failed = true;
                break;
            }
            if (caught_up)
            {
                break;
            }
            try
            {
                saveCheckpoint(false);
            }
            catch (...)
            {
                std::lock_guard lock{_projection_mutex};
                ++_checkpoint_failures;
                checkpoint_failed = true;
            }
        }
        if (!read_failed && !checkpoint_failed)
        {
            try
            {
                saveCheckpoint(false);
            }
            catch (...)
            {
                std::lock_guard lock{_projection_mutex};
                ++_checkpoint_failures;
            }
        }
        _poll_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at));
    }

    void RepositoryRankingProjector::saveCheckpoint(const bool force)
    {
        RankingCheckpoint checkpoint;
        {
            std::lock_guard lock{_projection_mutex};
            if (!force &&
                _projection.offset() - _checkpoint_offset < _config.checkpoint_every_events)
            {
                return;
            }
            checkpoint = _projection.checkpoint();
        }

        const auto started_at = std::chrono::steady_clock::now();
        try
        {
            _store.saveRankingCheckpoint(checkpoint);
        }
        catch (...)
        {
            _checkpoint_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::steady_clock::now() - started_at));
            throw;
        }
        _checkpoint_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - started_at));
        std::lock_guard lock{_projection_mutex};
        _checkpoint_offset = checkpoint.offset;
    }

    void RepositoryRankingProjector::run() noexcept
    {
        std::unique_lock control_lock{_control_mutex};
        while (!_stopping)
        {
            _control.wait_for(control_lock, _config.poll_interval, [this] { return _stopping; });
            if (_stopping)
            {
                break;
            }
            control_lock.unlock();
            pollOnce();
            control_lock.lock();
        }
        control_lock.unlock();

        try
        {
            catchUpAll();
        }
        catch (...)
        {
            std::lock_guard lock{_projection_mutex};
            ++_poll_failures;
        }
        try
        {
            saveCheckpoint(true);
        }
        catch (...)
        {
            std::lock_guard lock{_projection_mutex};
            ++_checkpoint_failures;
        }
    }
}
