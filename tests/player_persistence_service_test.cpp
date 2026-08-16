#include "snf/server/player_persistence_service.hpp"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <future>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

namespace
{
    using namespace std::chrono_literals;

    class RecordingRepository final : public snf::server::PlayerRepository
    {
    public:
        void asyncLoad(snf::server::PlayerId player,
                       snf::server::PlayerLoadCompletion completion) override
        {
            completion(snf::server::PlayerLoadResult{
                .status = snf::server::PlayerRepositoryStatus::Success,
                .record = std::nullopt,
            });
            static_cast<void>(player);
        }

        void asyncSave(snf::server::PlayerRecord record,
                       snf::server::PlayerSaveCompletion completion) override
        {
            std::lock_guard lock{_mutex};
            ++_save_calls;
            ++_active_saves;
            _maximum_active_saves = std::max(_maximum_active_saves, _active_saves);
            _pending.push_back(PendingSave{
                .record = std::move(record),
                .completion = std::move(completion),
            });
            _wake.notify_all();
        }

        void waitForSaveCalls(const std::size_t count)
        {
            std::unique_lock lock{_mutex};
            const bool completed =
                _wake.wait_for(lock, 1s, [this, count] { return _save_calls >= count; });
            assert(completed);
        }

        snf::server::PlayerRecord pendingRecord() const
        {
            std::lock_guard lock{_mutex};
            assert(!_pending.empty());
            return _pending.front().record;
        }

        void completeNext(const snf::server::PlayerRepositoryStatus status)
        {
            PendingSave pending;
            {
                std::lock_guard lock{_mutex};
                assert(!_pending.empty());
                pending = std::move(_pending.front());
                _pending.pop_front();
                --_active_saves;
                _wake.notify_all();
            }
            pending.completion(snf::server::PlayerSaveResult{.status = status});
        }

        [[nodiscard]] std::size_t maximumActiveSaves() const
        {
            std::lock_guard lock{_mutex};
            return _maximum_active_saves;
        }

    private:
        struct PendingSave
        {
            snf::server::PlayerRecord record;
            snf::server::PlayerSaveCompletion completion;
        };

        mutable std::mutex _mutex;
        std::condition_variable _wake;
        std::deque<PendingSave> _pending;
        std::size_t _save_calls{0};
        std::size_t _active_saves{0};
        std::size_t _maximum_active_saves{0};
    };

    snf::server::PlayerRecord record(const std::uint64_t player,
                                     const std::uint64_t handled)
    {
        return snf::server::PlayerRecord{
            .player = snf::server::PlayerId{.value = player},
            .handled_command_count = handled,
            .last_location = std::nullopt,
        };
    }

    void test_persistence_coalesces_and_serializes_per_player()
    {
        RecordingRepository repository;
        snf::server::PlayerPersistenceService service{
            repository,
            snf::server::PlayerPersistenceServiceConfig{
                .queue_capacity = 8,
                .flush_interval = 1ms,
            }};

        assert(service.tryEnqueue(record(1, 1)));
        repository.waitForSaveCalls(1);
        assert(service.tryEnqueue(record(1, 2)));
        assert(service.tryEnqueue(record(1, 3)));

        repository.completeNext(snf::server::PlayerRepositoryStatus::Success);
        repository.waitForSaveCalls(2);
        assert(repository.pendingRecord().handled_command_count == 3);
        repository.completeNext(snf::server::PlayerRepositoryStatus::Success);
        service.flush();

        assert(repository.maximumActiveSaves() == 1);
        const auto stats = service.stats();
        assert(stats.saves_succeeded == 2);
        assert(stats.pending_players == 0);
        assert(stats.in_flight_players == 0);
    }

    void test_persistence_retries_failed_background_snapshot_and_final_save_waits()
    {
        RecordingRepository repository;
        snf::server::PlayerPersistenceService service{
            repository,
            snf::server::PlayerPersistenceServiceConfig{
                .queue_capacity = 4,
                .flush_interval = 1ms,
            }};

        assert(service.tryEnqueue(record(2, 10)));
        repository.waitForSaveCalls(1);
        repository.completeNext(snf::server::PlayerRepositoryStatus::Unavailable);
        repository.waitForSaveCalls(2);
        repository.completeNext(snf::server::PlayerRepositoryStatus::Success);

        std::promise<snf::server::PlayerSaveResult> final_result_promise;
        auto final_result = final_result_promise.get_future();
        service.asyncSave(record(2, 11),
                          [&final_result_promise](snf::server::PlayerSaveResult result)
                          { final_result_promise.set_value(result); });
        repository.waitForSaveCalls(3);
        assert(repository.maximumActiveSaves() == 1);
        assert(repository.pendingRecord().handled_command_count == 11);
        repository.completeNext(snf::server::PlayerRepositoryStatus::Success);
        assert(final_result.wait_for(1s) == std::future_status::ready);
        assert(final_result.get().saved());

        const auto stats = service.stats();
        assert(stats.background_retries >= 1);
        assert(stats.final_saves == 1);
        assert(stats.final_failures == 0);
    }

    void test_persistence_queue_rejection_is_bounded_and_counted()
    {
        RecordingRepository repository;
        snf::server::PlayerPersistenceService service{
            repository,
            snf::server::PlayerPersistenceServiceConfig{
                .queue_capacity = 1,
                .flush_interval = 1s,
            }};

        assert(service.tryEnqueue(record(3, 1)));
        repository.waitForSaveCalls(1);
        bool rejected = false;
        for (std::uint64_t handled = 2; handled < 10000 && !rejected; ++handled)
        {
            rejected = !service.tryEnqueue(record(3, handled));
        }
        assert(rejected);
        assert(service.stats().snapshots_rejected >= 1);
        repository.completeNext(snf::server::PlayerRepositoryStatus::Success);
        repository.waitForSaveCalls(2);
        repository.completeNext(snf::server::PlayerRepositoryStatus::Success);
        service.flush();
    }
}

void run_player_persistence_service_tests()
{
    test_persistence_coalesces_and_serializes_per_player();
    test_persistence_retries_failed_background_snapshot_and_final_save_waits();
    test_persistence_queue_rejection_is_bounded_and_counted();
}
