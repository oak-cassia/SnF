#include "snf/server/player_persistence_service.hpp"

#include <stdexcept>
#include <utility>

namespace snf::server
{
    PlayerPersistenceService::PlayerPersistenceService(PlayerRepository& repository,
                                                       PlayerPersistenceServiceConfig config)
        : _repository(repository)
        , _flush_interval(config.flush_interval)
        , _snapshots(config.queue_capacity)
    {
        if (_flush_interval <= std::chrono::milliseconds::zero())
        {
            throw std::invalid_argument{"Player persistence flush interval must be positive"};
        }

        _thread = std::thread{[this] { run(); }};
    }

    PlayerPersistenceService::~PlayerPersistenceService()
    {
        stop();
    }

    bool PlayerPersistenceService::tryEnqueue(PlayerRecord record) noexcept
    {
        try
        {
            if (_snapshots.tryPush(SnapshotJob{.record = std::move(record)}))
            {
                {
                    std::lock_guard lock{_mutex};
                    ++_snapshots_accepted;
                }
                _wake.notify_one();
                return true;
            }
        }
        catch (...)
        {
            // Allocation failure is treated as bounded admission failure. The Actor
            // retains its dirty mask and can retry on a later turn.
        }

        {
            std::lock_guard lock{_mutex};
            ++_snapshots_rejected;
        }
        return false;
    }

    void PlayerPersistenceService::asyncSave(PlayerRecord record, PlayerSaveCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Final Player save completion must be callable"};
        }

        bool unavailable = false;
        {
            std::lock_guard lock{_mutex};
            if (_stopping || _stopped)
            {
                unavailable = true;
            }
            else
            {
                _final_requests[record.player].push_back(
                    FinalRequest{.record = std::move(record), .completion = std::move(completion)});
            }
        }

        if (unavailable)
        {
            completion(PlayerSaveResult{.status = PlayerRepositoryStatus::Unavailable});
            return;
        }
        _wake.notify_one();
    }

    void PlayerPersistenceService::flush()
    {
        std::unique_lock lock{_mutex};
        if (_stopped)
        {
            return;
        }
        _flush_requested = true;
        _wake.notify_all();
        _idle.wait(lock, [this] { return isFlushCompleteLocked() && !_flush_requested; });
    }

    void PlayerPersistenceService::stop() noexcept
    {
        {
            std::lock_guard lock{_mutex};
            if (_stopping || _stopped)
            {
                return;
            }
            _stopping = true;
        }
        _snapshots.close();
        _wake.notify_all();
        if (_thread.joinable())
        {
            _thread.join();
        }
        std::lock_guard lock{_mutex};
        _stopped = true;
        _idle.notify_all();
    }

    PlayerPersistenceServiceStats PlayerPersistenceService::stats() const
    {
        std::lock_guard lock{_mutex};
        std::size_t pending_players = _pending.size();
        for (const auto& [player, requests] : _final_requests)
        {
            if (!requests.empty() && !_pending.contains(player))
            {
                ++pending_players;
            }
        }
        return PlayerPersistenceServiceStats{
            .snapshots_accepted = _snapshots_accepted,
            .snapshots_rejected = _snapshots_rejected,
            .saves_started = _saves_started,
            .saves_succeeded = _saves_succeeded,
            .saves_failed = _saves_failed,
            .background_retries = _background_retries,
            .final_saves = _final_saves,
            .final_failures = _final_failures,
            .queue_depth = _snapshots.size(),
            .queue_high_water_mark = _snapshots.highWaterMark(),
            .pending_players = pending_players,
            .in_flight_players = _in_flight.size(),
        };
    }

    void PlayerPersistenceService::run() noexcept
    {
        std::unique_lock lock{_mutex};
        while (!_stopping)
        {
            _wake.wait_for(lock,
                           _flush_interval,
                           [this]
                           { return _stopping || _flush_requested || _snapshots.size() != 0; });
            lock.unlock();
            try
            {
                processOnce();
            }
            catch (...)
            {
                // All expected repository failures are converted into a save result
                // in startSave. Keep the service alive if a custom adapter violates
                // that contract; pending snapshots remain available for the next tick.
            }
            lock.lock();
            if (_flush_requested && isFlushCompleteLocked())
            {
                _flush_requested = false;
                _idle.notify_all();
            }
        }
        lock.unlock();

        // stop() is normally called only after flush(), but still drain accepted
        // final requests and wait for every repository callback before the service
        // object can be destroyed. Failed background snapshots are intentionally
        // dropped only at this terminal point; there is no later tick to retry them.
        while (true)
        {
            try
            {
                processOnce();
            }
            catch (...)
            {
                // Keep final callback bookkeeping alive even if a custom adapter
                // throws outside the normal startSave conversion path.
            }

            std::unique_lock stop_lock{_mutex};
            if (_final_requests.empty() && _in_flight.empty())
            {
                _pending.clear();
                break;
            }
            if (_in_flight.empty())
            {
                // A synchronous completion may have drained one final request
                // while another remains queued; schedule the next one immediately.
                continue;
            }
            _idle.wait(stop_lock, [this] { return _in_flight.empty(); });
        }
        std::lock_guard final_lock{_mutex};
        _stopped = true;
        _flush_requested = false;
        _idle.notify_all();
    }

    void PlayerPersistenceService::processOnce()
    {
        drainSnapshots();
        for (StartSave& save : selectSaves())
        {
            startSave(std::move(save));
        }

        std::lock_guard lock{_mutex};
        if (_flush_requested && isFlushCompleteLocked())
        {
            _flush_requested = false;
            _idle.notify_all();
        }
    }

    void PlayerPersistenceService::drainSnapshots()
    {
        while (auto snapshot = _snapshots.tryPop())
        {
            std::lock_guard lock{_mutex};
            _pending.insert_or_assign(snapshot->record.player, std::move(snapshot->record));
        }
    }

    std::vector<PlayerPersistenceService::StartSave> PlayerPersistenceService::selectSaves()
    {
        std::vector<StartSave> saves;
        std::lock_guard lock{_mutex};

        for (auto final = _final_requests.begin(); final != _final_requests.end();)
        {
            const PlayerId player = final->first;
            if (_in_flight.contains(player) || final->second.empty())
            {
                if (final->second.empty())
                {
                    final = _final_requests.erase(final);
                }
                else
                {
                    ++final;
                }
                continue;
            }

            FinalRequest request = std::move(final->second.front());
            final->second.pop_front();
            if (final->second.empty())
            {
                final = _final_requests.erase(final);
            }
            else
            {
                ++final;
            }
            // A final snapshot contains everything the Actor knows. It supersedes
            // an older queued background snapshot, but never jumps over an in-flight
            // save because _in_flight was checked above.
            _pending.erase(player);
            _in_flight.emplace(player,
                               InFlight{.record = request.record,
                                        .final = true,
                                        .completion = std::move(request.completion)});
            saves.push_back(StartSave{
                .player = player,
                .record = std::move(request.record),
                .final = true,
            });
        }

        for (auto pending = _pending.begin(); pending != _pending.end();)
        {
            const PlayerId player = pending->first;
            if (_in_flight.contains(player) || _final_requests.contains(player))
            {
                ++pending;
                continue;
            }

            PlayerRecord record = std::move(pending->second);
            pending = _pending.erase(pending);
            _in_flight.emplace(player,
                               InFlight{.record = record, .final = false, .completion = {}});
            saves.push_back(StartSave{
                .player = player,
                .record = std::move(record),
                .final = false,
            });
        }
        return saves;
    }

    void PlayerPersistenceService::startSave(StartSave save) noexcept
    {
        {
            std::lock_guard lock{_mutex};
            ++_saves_started;
            if (save.final)
            {
                ++_final_saves;
            }
        }

        try
        {
            _repository.asyncSave(
                std::move(save.record),
                [this, player = save.player](PlayerSaveResult result) mutable noexcept
                { completeSave(player, std::move(result)); });
        }
        catch (...)
        {
            completeSave(save.player,
                         PlayerSaveResult{.status = PlayerRepositoryStatus::Unavailable});
        }
    }

    void PlayerPersistenceService::completeSave(const PlayerId player,
                                                PlayerSaveResult result) noexcept
    {
        PlayerSaveCompletion completion;
        bool final = false;
        PlayerRecord failed_record;
        bool retry = false;
        {
            std::lock_guard lock{_mutex};
            const auto in_flight = _in_flight.find(player);
            if (in_flight == _in_flight.end())
            {
                return;
            }
            final = in_flight->second.final;
            completion = std::move(in_flight->second.completion);
            failed_record = std::move(in_flight->second.record);
            _in_flight.erase(in_flight);

            if (result.saved())
            {
                ++_saves_succeeded;
            }
            else
            {
                ++_saves_failed;
                if (final)
                {
                    ++_final_failures;
                }
                else if (!_final_requests.contains(player))
                {
                    // Keep the exact failed snapshot. A newer snapshot, if any,
                    // will replace it when the queue is drained.
                    _pending.insert_or_assign(player, std::move(failed_record));
                    ++_background_retries;
                    retry = true;
                }
            }
        }

        if (completion)
        {
            notifyCompletion(completion, result);
        }
        if (retry)
        {
            _wake.notify_one();
        }
        _idle.notify_all();
    }

    void PlayerPersistenceService::notifyCompletion(PlayerSaveCompletion& completion,
                                                    PlayerSaveResult result) noexcept
    {
        try
        {
            completion(std::move(result));
        }
        catch (...)
        {
            // A repository completion must not be able to terminate its Worker or
            // the persistence thread. Runtime-owned completions are noexcept in
            // production and observe the result through their continuation.
        }
    }

    bool PlayerPersistenceService::isFlushCompleteLocked() const noexcept
    {
        // A failed background snapshot remains in _pending for the next timer tick.
        // A flush must wait for one attempted save, but it must not spin forever
        // while an unavailable repository is being retried.
        return _snapshots.size() == 0 && _final_requests.empty() && _in_flight.empty();
    }
}
