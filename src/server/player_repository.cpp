#include "snf/server/player_repository.hpp"

#include <limits>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace snf::server
{
    InMemoryPlayerRepository::InMemoryPlayerRepository(
        const std::size_t max_idempotency_records_per_player, const std::size_t max_ranking_events)
        : _max_idempotency_records_per_player(max_idempotency_records_per_player)
        , _max_ranking_events(max_ranking_events)
    {
        if (_max_idempotency_records_per_player == 0)
        {
            throw std::invalid_argument{"Purchase idempotency capacity must be positive"};
        }
        if (_max_ranking_events == 0)
        {
            throw std::invalid_argument{"Ranking event capacity must be positive"};
        }
        _ranking_events.reserve(_max_ranking_events);
    }

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

    void InMemoryPlayerRepository::asyncPurchase(PurchaseRequest request,
                                                 PurchaseCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Purchase completion must be callable"};
        }
        if (request.player.value == 0 || request.idempotency_key.value == 0 ||
            request.product.value == 0)
        {
            throw std::invalid_argument{"Purchase identity fields must be non-zero"};
        }

        PurchaseTransactionResult result;
        {
            std::lock_guard lock{_mutex};
            auto& record = _records
                               .try_emplace(request.player,
                                            PlayerRecord{
                                                .player = request.player,
                                                .handled_command_count = 0,
                                                .last_location = std::nullopt,
                                                .currency_balance = INITIAL_CURRENCY_BALANCE,
                                                .purchased_item_count = 0,
                                                .ranking_score = 0,
                                                .last_domain_event_sequence = 0,
                                            })
                               .first->second;
            auto& purchases = _purchases[request.player];
            if (const auto existing = purchases.find(request.idempotency_key.value);
                existing != purchases.end())
            {
                if (existing->second.product != request.product)
                {
                    ++_purchase_stats.rejected;
                    result = PurchaseTransactionResult{
                        .status = PurchaseStatus::IdempotencyConflict,
                        .player = request.player,
                        .idempotency_key = request.idempotency_key,
                        .product = request.product,
                        .currency_balance = record.currency_balance,
                        .purchased_item_count = record.purchased_item_count,
                        .replayed = false,
                    };
                }
                else
                {
                    ++_purchase_stats.replayed;
                    result = existing->second.result;
                    // The outcome is replayed, while balances are the current
                    // authoritative record. Returning an old absolute snapshot and
                    // applying it in PlayerActor would roll back later purchases.
                    result.currency_balance = record.currency_balance;
                    result.purchased_item_count = record.purchased_item_count;
                    result.replayed = true;
                }
            }
            else if (request.product != BASIC_PRODUCT)
            {
                ++_purchase_stats.rejected;
                result = PurchaseTransactionResult{
                    .status = PurchaseStatus::ProductNotFound,
                    .player = request.player,
                    .idempotency_key = request.idempotency_key,
                    .product = request.product,
                    .currency_balance = record.currency_balance,
                    .purchased_item_count = record.purchased_item_count,
                    .replayed = false,
                };
            }
            else if (purchases.size() == _max_idempotency_records_per_player)
            {
                ++_purchase_stats.rejected;
                result = PurchaseTransactionResult{
                    .status = PurchaseStatus::IdempotencyCapacityExceeded,
                    .player = request.player,
                    .idempotency_key = request.idempotency_key,
                    .product = request.product,
                    .currency_balance = record.currency_balance,
                    .purchased_item_count = record.purchased_item_count,
                    .replayed = false,
                };
            }
            else
            {
                PurchaseStatus status = PurchaseStatus::Committed;
                if (record.currency_balance < BASIC_PRODUCT_PRICE)
                {
                    status = PurchaseStatus::InsufficientFunds;
                }
                else if (record.purchased_item_count >
                         std::numeric_limits<std::uint64_t>::max() - BASIC_PRODUCT_GRANT_COUNT)
                {
                    status = PurchaseStatus::InventoryCapacityExceeded;
                }
                if (status == PurchaseStatus::Committed)
                {
                    record.currency_balance -= BASIC_PRODUCT_PRICE;
                    record.purchased_item_count += BASIC_PRODUCT_GRANT_COUNT;
                    ++_purchase_stats.committed;
                }
                else
                {
                    ++_purchase_stats.rejected;
                }

                result = PurchaseTransactionResult{
                    .status = status,
                    .player = request.player,
                    .idempotency_key = request.idempotency_key,
                    .product = request.product,
                    .currency_balance = record.currency_balance,
                    .purchased_item_count = record.purchased_item_count,
                    .replayed = false,
                };
                purchases.emplace(request.idempotency_key.value,
                                  StoredPurchase{
                                      .product = request.product,
                                      .result = result,
                                  });
            }
        }

        completion(std::move(result));
    }

    void InMemoryPlayerRepository::asyncAwardRankingScore(RankingAwardRequest request,
                                                          RankingAwardCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Ranking award completion must be callable"};
        }
        if (request.player.value == 0 || request.award_id.value == 0 || request.score_delta == 0)
        {
            throw std::invalid_argument{"Ranking award identity and delta must be non-zero"};
        }

        RankingAwardTransactionResult result;
        {
            std::lock_guard lock{_mutex};
            auto& record = _records
                               .try_emplace(request.player,
                                            PlayerRecord{
                                                .player = request.player,
                                                .handled_command_count = 0,
                                                .last_location = std::nullopt,
                                                .currency_balance = INITIAL_CURRENCY_BALANCE,
                                                .purchased_item_count = 0,
                                                .ranking_score = 0,
                                                .last_domain_event_sequence = 0,
                                            })
                               .first->second;
            auto& awards = _ranking_awards[request.player];
            if (const auto existing = awards.find(request.award_id.value); existing != awards.end())
            {
                result = existing->second.result;
                result.authoritative_score = record.ranking_score;
                result.authoritative_sequence = record.last_domain_event_sequence;
                if (existing->second.score_delta == request.score_delta)
                {
                    ++_ranking_award_stats.replayed;
                    result.replayed = true;
                }
                else
                {
                    ++_ranking_award_stats.rejected;
                    result.status = RankingAwardStatus::IdempotencyConflict;
                    result.score_delta = request.score_delta;
                    result.replayed = false;
                }
            }
            else if (_ranking_events.size() == _max_ranking_events)
            {
                ++_ranking_award_stats.rejected;
                result = RankingAwardTransactionResult{
                    .status = RankingAwardStatus::CapacityExceeded,
                    .player = request.player,
                    .award_id = request.award_id,
                    .score_delta = request.score_delta,
                    .authoritative_score = record.ranking_score,
                    .authoritative_sequence = record.last_domain_event_sequence,
                };
            }
            else if (record.ranking_score >
                     std::numeric_limits<std::uint64_t>::max() - request.score_delta)
            {
                ++_ranking_award_stats.rejected;
                result = RankingAwardTransactionResult{
                    .status = RankingAwardStatus::ScoreOverflow,
                    .player = request.player,
                    .award_id = request.award_id,
                    .score_delta = request.score_delta,
                    .authoritative_score = record.ranking_score,
                    .authoritative_sequence = record.last_domain_event_sequence,
                };
            }
            else if (record.last_domain_event_sequence == std::numeric_limits<std::uint64_t>::max())
            {
                ++_ranking_award_stats.rejected;
                result = RankingAwardTransactionResult{
                    .status = RankingAwardStatus::SequenceOverflow,
                    .player = request.player,
                    .award_id = request.award_id,
                    .score_delta = request.score_delta,
                    .authoritative_score = record.ranking_score,
                    .authoritative_sequence = record.last_domain_event_sequence,
                };
            }
            else if (_ranking_events.size() == std::numeric_limits<std::uint64_t>::max())
            {
                ++_ranking_award_stats.rejected;
                result = RankingAwardTransactionResult{
                    .status = RankingAwardStatus::EventOffsetOverflow,
                    .player = request.player,
                    .award_id = request.award_id,
                    .score_delta = request.score_delta,
                    .authoritative_score = record.ranking_score,
                    .authoritative_sequence = record.last_domain_event_sequence,
                };
            }
            else
            {
                record.ranking_score += request.score_delta;
                ++record.last_domain_event_sequence;
                const auto offset = static_cast<std::uint64_t>(_ranking_events.size()) + 1;
                result = RankingAwardTransactionResult{
                    .status = RankingAwardStatus::Committed,
                    .player = request.player,
                    .award_id = request.award_id,
                    .score_delta = request.score_delta,
                    .event_sequence = record.last_domain_event_sequence,
                    .event_score = record.ranking_score,
                    .global_offset = offset,
                    .authoritative_score = record.ranking_score,
                    .authoritative_sequence = record.last_domain_event_sequence,
                    .replayed = false,
                };
                _ranking_events.push_back(PlayerEventRecord{
                    .offset = offset,
                    .event =
                        PlayerScoreChanged{
                            .player = request.player,
                            .sequence = record.last_domain_event_sequence,
                            .score = record.ranking_score,
                        },
                });
                awards.emplace(request.award_id.value,
                               StoredRankingAward{
                                   .score_delta = request.score_delta,
                                   .result = result,
                               });
                ++_ranking_award_stats.committed;
            }
        }

        completion(std::move(result));
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

    std::vector<PlayerEventRecord>
    InMemoryPlayerRepository::rankingEventsAfter(const std::uint64_t offset) const
    {
        std::lock_guard lock{_mutex};
        if (offset > _ranking_events.size())
        {
            throw std::out_of_range{"Ranking event offset is beyond its tail"};
        }
        return std::vector<PlayerEventRecord>{
            _ranking_events.begin() + static_cast<std::size_t>(offset), _ranking_events.end()};
    }

    InMemoryPlayerRepository::PurchaseStats InMemoryPlayerRepository::purchaseStats() const
    {
        std::lock_guard lock{_mutex};
        return _purchase_stats;
    }

    InMemoryPlayerRepository::RankingAwardStats InMemoryPlayerRepository::rankingAwardStats() const
    {
        std::lock_guard lock{_mutex};
        return _ranking_award_stats;
    }

    ThreadedPlayerRepository::ThreadedPlayerRepository(ThreadedPlayerRepositoryConfig config)
        : _storage(config.max_idempotency_records_per_player, config.max_ranking_events)
        , _jobs(config.queue_capacity)
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

    void ThreadedPlayerRepository::asyncPurchase(PurchaseRequest request,
                                                 PurchaseCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Purchase completion must be callable"};
        }

        if (_jobs.tryPush(Job{PurchaseJob{
                .request = request,
                .completion = completion,
            }}))
        {
            _accepted.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        _rejected.fetch_add(1, std::memory_order_relaxed);
        completion(PurchaseTransactionResult{
            .status = PurchaseStatus::Unavailable,
            .player = request.player,
            .idempotency_key = request.idempotency_key,
            .product = request.product,
            .currency_balance = 0,
            .purchased_item_count = 0,
            .replayed = false,
        });
    }

    void ThreadedPlayerRepository::asyncAwardRankingScore(RankingAwardRequest request,
                                                          RankingAwardCompletion completion)
    {
        if (!completion)
        {
            throw std::invalid_argument{"Ranking award completion must be callable"};
        }

        if (_jobs.tryPush(Job{RankingAwardJob{
                .request = request,
                .completion = completion,
            }}))
        {
            _accepted.fetch_add(1, std::memory_order_relaxed);
            return;
        }

        _rejected.fetch_add(1, std::memory_order_relaxed);
        completion(RankingAwardTransactionResult{
            .status = RankingAwardStatus::Unavailable,
            .player = request.player,
            .award_id = request.award_id,
            .score_delta = request.score_delta,
        });
    }

    void ThreadedPlayerRepository::close() noexcept
    {
        _jobs.close();
    }

    std::optional<PlayerRecord> ThreadedPlayerRepository::find(const PlayerId player) const
    {
        return _storage.find(player);
    }

    PlayerRepositoryStats ThreadedPlayerRepository::stats() const
    {
        const InMemoryPlayerRepository::PurchaseStats purchases = _storage.purchaseStats();
        const InMemoryPlayerRepository::RankingAwardStats ranking_awards =
            _storage.rankingAwardStats();
        return PlayerRepositoryStats{
            .accepted = _accepted.load(std::memory_order_relaxed),
            .rejected = _rejected.load(std::memory_order_relaxed),
            .queue_depth = _jobs.size(),
            .queue_high_water_mark = _jobs.highWaterMark(),
            .purchase_committed = purchases.committed,
            .purchase_replayed = purchases.replayed,
            .purchase_rejected = purchases.rejected,
            .ranking_awards_committed = ranking_awards.committed,
            .ranking_awards_replayed = ranking_awards.replayed,
            .ranking_awards_rejected = ranking_awards.rejected,
            .operation_failures = 0,
            .operation_latency_nanoseconds = {},
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
                    else if constexpr (std::is_same_v<Value, SaveJob>)
                    {
                        _storage.asyncSave(std::move(value.record), std::move(value.completion));
                    }
                    else if constexpr (std::is_same_v<Value, PurchaseJob>)
                    {
                        _storage.asyncPurchase(value.request, std::move(value.completion));
                    }
                    else
                    {
                        _storage.asyncAwardRankingScore(value.request, std::move(value.completion));
                    }
                },
                std::move(*job));
        }
    }
}
