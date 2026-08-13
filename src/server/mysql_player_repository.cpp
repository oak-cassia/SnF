#include "snf/server/mysql_player_repository.hpp"

#include "snf/runtime/bounded_queue.hpp"
#include "snf/runtime/distribution.hpp"

#include <mysql/mysql.h>

#include <atomic>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    using MySqlResult = std::unique_ptr<MYSQL_RES, decltype(&::mysql_free_result)>;

    [[nodiscard]] std::runtime_error mysql_error(MYSQL* connection,
                                                 const std::string_view operation)
    {
        return std::runtime_error{std::string{operation} + ": " + ::mysql_error(connection)};
    }

    [[nodiscard]] unsigned int timeout_seconds(const std::chrono::seconds timeout)
    {
        if (timeout <= std::chrono::seconds::zero() ||
            timeout.count() > std::numeric_limits<unsigned int>::max())
        {
            throw std::invalid_argument{"MySQL timeout is outside the client range"};
        }
        return static_cast<unsigned int>(timeout.count());
    }

    void validate_config(const snf::server::MySqlPlayerRepositoryConfig& config)
    {
        if (config.host.empty() || config.user.empty() || config.database.empty() ||
            config.port == 0 || config.worker_count == 0 || config.queue_capacity == 0 ||
            config.max_idempotency_records_per_player == 0)
        {
            throw std::invalid_argument{"MySQL repository configuration is incomplete"};
        }
        for (const unsigned char character : config.database)
        {
            if (std::isalnum(character) == 0 && character != '_')
            {
                throw std::invalid_argument{"MySQL database name must be an identifier"};
            }
        }
        static_cast<void>(timeout_seconds(config.connect_timeout));
        static_cast<void>(timeout_seconds(config.read_timeout));
        static_cast<void>(timeout_seconds(config.write_timeout));
    }

    class MySqlConnection final
    {
    public:
        explicit MySqlConnection(const snf::server::MySqlPlayerRepositoryConfig& config)
            : _connection(::mysql_init(nullptr))
        {
            if (_connection == nullptr)
            {
                throw std::runtime_error{"mysql_init failed"};
            }

            try
            {
                const unsigned int connect_timeout = timeout_seconds(config.connect_timeout);
                const unsigned int read_timeout = timeout_seconds(config.read_timeout);
                const unsigned int write_timeout = timeout_seconds(config.write_timeout);
                setOption(MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
                setOption(MYSQL_OPT_READ_TIMEOUT, &read_timeout);
                setOption(MYSQL_OPT_WRITE_TIMEOUT, &write_timeout);

                if (::mysql_real_connect(_connection,
                                         config.host.c_str(),
                                         config.user.c_str(),
                                         config.password.c_str(),
                                         config.database.c_str(),
                                         config.port,
                                         nullptr,
                                         CLIENT_FOUND_ROWS) == nullptr)
                {
                    throw mysql_error(_connection, "mysql_real_connect");
                }
                execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED");
            }
            catch (...)
            {
                ::mysql_close(_connection);
                _connection = nullptr;
                throw;
            }
        }

        ~MySqlConnection()
        {
            if (_connection != nullptr)
            {
                ::mysql_close(_connection);
            }
        }

        MySqlConnection(const MySqlConnection&) = delete;
        MySqlConnection& operator=(const MySqlConnection&) = delete;

        void execute(const std::string_view sql)
        {
            if (::mysql_real_query(_connection, sql.data(), sql.size()) != 0)
            {
                throw mysql_error(_connection, "mysql query");
            }
            if (::mysql_field_count(_connection) != 0)
            {
                MySqlResult discarded{::mysql_store_result(_connection), &::mysql_free_result};
                if (!discarded)
                {
                    throw mysql_error(_connection, "mysql_store_result");
                }
            }
        }

        [[nodiscard]] MySqlResult query(const std::string_view sql)
        {
            if (::mysql_real_query(_connection, sql.data(), sql.size()) != 0)
            {
                throw mysql_error(_connection, "mysql query");
            }
            MySqlResult result{::mysql_store_result(_connection), &::mysql_free_result};
            if (!result)
            {
                throw mysql_error(_connection, "mysql_store_result");
            }
            return result;
        }

        [[nodiscard]] MYSQL* get() const noexcept
        {
            return _connection;
        }

    private:
        void setOption(const mysql_option option, const void* value)
        {
            if (::mysql_options(_connection, option, value) != 0)
            {
                throw mysql_error(_connection, "mysql_options");
            }
        }

        MYSQL* _connection{nullptr};
    };

    class Transaction final
    {
    public:
        explicit Transaction(MySqlConnection& connection)
            : _connection(connection)
        {
            _connection.execute("START TRANSACTION");
        }

        ~Transaction()
        {
            if (_active)
            {
                try
                {
                    _connection.execute("ROLLBACK");
                }
                catch (...)
                {
                }
            }
        }

        Transaction(const Transaction&) = delete;
        Transaction& operator=(const Transaction&) = delete;

        void commit()
        {
            _connection.execute("COMMIT");
            _active = false;
        }

    private:
        MySqlConnection& _connection;
        bool _active{true};
    };

    template <typename Integer>
    [[nodiscard]] Integer parse_integer(const char* const text, const std::string_view field)
    {
        if (text == nullptr)
        {
            throw std::runtime_error{std::string{field} + " is unexpectedly NULL"};
        }
        Integer value{};
        const std::string_view input{text};
        const auto [end, error] = std::from_chars(input.data(), input.data() + input.size(), value);
        if (error != std::errc{} || end != input.data() + input.size())
        {
            throw std::runtime_error{std::string{field} + " is not a valid integer"};
        }
        return value;
    }

    [[nodiscard]] std::string player_select(const snf::server::PlayerId player,
                                            const bool for_update)
    {
        return "SELECT handled_command_count, zone_id, position_x, position_y, "
               "currency_balance, purchased_item_count, ranking_score, "
               "last_domain_event_sequence FROM snf_players WHERE player_id=" +
               std::to_string(player.value) + (for_update ? " FOR UPDATE" : "");
    }

    [[nodiscard]] snf::server::PlayerRecord decode_player(const snf::server::PlayerId player,
                                                          MYSQL_ROW row)
    {
        const bool has_zone = row[1] != nullptr;
        if (has_zone != (row[2] != nullptr) || has_zone != (row[3] != nullptr))
        {
            throw std::runtime_error{"Player location columns are inconsistent"};
        }

        std::optional<snf::server::PlayerLocation> location;
        if (has_zone)
        {
            location = snf::server::PlayerLocation{
                .zone =
                    snf::server::ZoneId{.value = parse_integer<std::uint64_t>(row[1], "zone_id")},
                .position =
                    {
                        .x = parse_integer<std::int32_t>(row[2], "position_x"),
                        .y = parse_integer<std::int32_t>(row[3], "position_y"),
                    },
            };
        }

        return snf::server::PlayerRecord{
            .player = player,
            .handled_command_count = parse_integer<std::uint64_t>(row[0], "handled_command_count"),
            .last_location = location,
            .currency_balance = parse_integer<std::uint64_t>(row[4], "currency_balance"),
            .purchased_item_count = parse_integer<std::uint64_t>(row[5], "purchased_item_count"),
            .ranking_score = parse_integer<std::uint64_t>(row[6], "ranking_score"),
            .last_domain_event_sequence =
                parse_integer<std::uint64_t>(row[7], "last_domain_event_sequence"),
        };
    }

    class MySqlStore final
    {
    public:
        MySqlStore(const snf::server::MySqlPlayerRepositoryConfig& config, const bool ensure_schema)
            : _config(config)
            , _connection(config)
        {
            if (ensure_schema)
            {
                ensureSchema();
            }
        }

        [[nodiscard]] snf::server::PlayerLoadResult load(const snf::server::PlayerId player)
        {
            auto result = _connection.query(player_select(player, false));
            if (::mysql_num_rows(result.get()) == 0)
            {
                return snf::server::PlayerLoadResult{
                    .status = snf::server::PlayerRepositoryStatus::Success,
                    .record = std::nullopt,
                };
            }
            if (::mysql_num_rows(result.get()) != 1)
            {
                throw std::runtime_error{"Player identity returned more than one row"};
            }
            MYSQL_ROW row = ::mysql_fetch_row(result.get());
            return snf::server::PlayerLoadResult{
                .status = snf::server::PlayerRepositoryStatus::Success,
                .record = decode_player(player, row),
            };
        }

        [[nodiscard]] snf::server::PlayerSaveResult save(const snf::server::PlayerRecord& record)
        {
            const std::string zone =
                record.last_location ? std::to_string(record.last_location->zone.value) : "NULL";
            const std::string position_x =
                record.last_location ? std::to_string(record.last_location->position.x) : "NULL";
            const std::string position_y =
                record.last_location ? std::to_string(record.last_location->position.y) : "NULL";
            // The INSERT seeds a missing Player. Once the row exists, disconnect
            // snapshots own only activity/location; purchase and ranking transactions
            // are the sole writers of their durable fields.
            const std::string sql =
                "INSERT INTO snf_players (player_id, handled_command_count, zone_id, "
                "position_x, position_y, currency_balance, purchased_item_count, ranking_score, "
                "last_domain_event_sequence) VALUES (" +
                std::to_string(record.player.value) + "," +
                std::to_string(record.handled_command_count) + "," + zone + "," + position_x + "," +
                position_y + "," + std::to_string(record.currency_balance) + "," +
                std::to_string(record.purchased_item_count) + "," +
                std::to_string(record.ranking_score) + "," +
                std::to_string(record.last_domain_event_sequence) +
                ") ON DUPLICATE KEY UPDATE "
                "handled_command_count=GREATEST(handled_command_count, "
                "VALUES(handled_command_count)), "
                "zone_id=VALUES(zone_id), position_x=VALUES(position_x), "
                "position_y=VALUES(position_y)";
            _connection.execute(sql);
            return snf::server::PlayerSaveResult{
                .status = snf::server::PlayerRepositoryStatus::Success,
            };
        }

        [[nodiscard]] snf::server::PurchaseTransactionResult
        purchase(const snf::server::PurchaseRequest& request)
        {
            Transaction transaction{_connection};
            _connection.execute(
                "INSERT IGNORE INTO snf_players (player_id, handled_command_count, zone_id, "
                "position_x, position_y, currency_balance, purchased_item_count, ranking_score, "
                "last_domain_event_sequence) VALUES (" +
                std::to_string(request.player.value) + ",0,NULL,NULL,NULL," +
                std::to_string(snf::server::INITIAL_CURRENCY_BALANCE) + ",0,0,0)");

            auto player_result = _connection.query(player_select(request.player, true));
            if (::mysql_num_rows(player_result.get()) != 1)
            {
                throw std::runtime_error{"Purchase could not lock its Player row"};
            }
            const snf::server::PlayerRecord player =
                decode_player(request.player, ::mysql_fetch_row(player_result.get()));

            auto existing = _connection.query(
                "SELECT product_id, status FROM snf_purchase_idempotency WHERE player_id=" +
                std::to_string(request.player.value) +
                " AND idempotency_key=" + std::to_string(request.idempotency_key.value));
            if (::mysql_num_rows(existing.get()) != 0)
            {
                if (::mysql_num_rows(existing.get()) != 1)
                {
                    throw std::runtime_error{"Idempotency identity returned more than one row"};
                }
                MYSQL_ROW row = ::mysql_fetch_row(existing.get());
                const snf::server::ProductId stored_product{
                    .value = parse_integer<std::uint32_t>(row[0], "product_id")};
                const std::uint16_t stored_status =
                    parse_integer<std::uint16_t>(row[1], "purchase status");
                if (stored_status > static_cast<std::uint16_t>(
                                        snf::server::PurchaseStatus::InventoryCapacityExceeded))
                {
                    throw std::runtime_error{"Stored purchase status is invalid"};
                }
                transaction.commit();
                return snf::server::PurchaseTransactionResult{
                    .status = stored_product == request.product
                                  ? static_cast<snf::server::PurchaseStatus>(stored_status)
                                  : snf::server::PurchaseStatus::IdempotencyConflict,
                    .player = request.player,
                    .idempotency_key = request.idempotency_key,
                    .product = request.product,
                    .currency_balance = player.currency_balance,
                    .purchased_item_count = player.purchased_item_count,
                    .replayed = stored_product == request.product,
                };
            }

            if (request.product != snf::server::BASIC_PRODUCT)
            {
                transaction.commit();
                return result(request,
                              snf::server::PurchaseStatus::ProductNotFound,
                              player.currency_balance,
                              player.purchased_item_count);
            }

            auto count_result =
                _connection.query("SELECT COUNT(*) FROM snf_purchase_idempotency WHERE player_id=" +
                                  std::to_string(request.player.value));
            MYSQL_ROW count_row = ::mysql_fetch_row(count_result.get());
            if (parse_integer<std::uint64_t>(count_row[0], "idempotency count") >=
                _config.max_idempotency_records_per_player)
            {
                transaction.commit();
                return result(request,
                              snf::server::PurchaseStatus::IdempotencyCapacityExceeded,
                              player.currency_balance,
                              player.purchased_item_count);
            }

            snf::server::PurchaseStatus status = snf::server::PurchaseStatus::Committed;
            std::uint64_t currency = player.currency_balance;
            std::uint64_t items = player.purchased_item_count;
            if (currency < snf::server::BASIC_PRODUCT_PRICE)
            {
                status = snf::server::PurchaseStatus::InsufficientFunds;
            }
            else if (items > std::numeric_limits<std::uint64_t>::max() -
                                 snf::server::BASIC_PRODUCT_GRANT_COUNT)
            {
                status = snf::server::PurchaseStatus::InventoryCapacityExceeded;
            }
            else
            {
                currency -= snf::server::BASIC_PRODUCT_PRICE;
                items += snf::server::BASIC_PRODUCT_GRANT_COUNT;
                _connection.execute(
                    "UPDATE snf_players SET currency_balance=" + std::to_string(currency) +
                    ", purchased_item_count=" + std::to_string(items) +
                    " WHERE player_id=" + std::to_string(request.player.value));
            }

            _connection.execute(
                "INSERT INTO snf_purchase_idempotency (player_id, idempotency_key, product_id, "
                "status) VALUES (" +
                std::to_string(request.player.value) + "," +
                std::to_string(request.idempotency_key.value) + "," +
                std::to_string(request.product.value) + "," +
                std::to_string(static_cast<std::uint16_t>(status)) + ")");
            if (_config.purchase_fault_injector)
            {
                _config.purchase_fault_injector(snf::server::MySqlPurchaseFaultPoint::BeforeCommit);
            }
            transaction.commit();
            if (_config.purchase_fault_injector)
            {
                _config.purchase_fault_injector(
                    snf::server::MySqlPurchaseFaultPoint::AfterCommitBeforeCompletion);
            }
            return result(request, status, currency, items);
        }

        [[nodiscard]] snf::server::RankingAwardTransactionResult
        awardRankingScore(const snf::server::RankingAwardRequest& request)
        {
            if (request.score_delta == 0)
            {
                throw std::invalid_argument{"Ranking award delta must be non-zero"};
            }

            Transaction transaction{_connection};
            _connection.execute(
                "INSERT IGNORE INTO snf_players (player_id, handled_command_count, zone_id, "
                "position_x, position_y, currency_balance, purchased_item_count, ranking_score, "
                "last_domain_event_sequence) VALUES (" +
                std::to_string(request.player.value) + ",0,NULL,NULL,NULL," +
                std::to_string(snf::server::INITIAL_CURRENCY_BALANCE) + ",0,0,0)");

            auto player_result = _connection.query(player_select(request.player, true));
            if (::mysql_num_rows(player_result.get()) != 1)
            {
                throw std::runtime_error{"Ranking award could not lock its Player row"};
            }
            const snf::server::PlayerRecord player =
                decode_player(request.player, ::mysql_fetch_row(player_result.get()));

            auto existing = _connection.query(
                "SELECT player_sequence, award_id, score_delta, absolute_score, global_offset "
                "FROM snf_player_events WHERE player_id=" +
                std::to_string(request.player.value) +
                " AND award_id=" + std::to_string(request.award_id.value));
            if (::mysql_num_rows(existing.get()) != 0)
            {
                if (::mysql_num_rows(existing.get()) != 1)
                {
                    throw std::runtime_error{"Ranking award identity returned more than one row"};
                }
                MYSQL_ROW row = ::mysql_fetch_row(existing.get());
                const std::uint64_t stored_delta =
                    parse_integer<std::uint64_t>(row[2], "ranking score_delta");
                transaction.commit();
                return snf::server::RankingAwardTransactionResult{
                    .status = stored_delta == request.score_delta
                                  ? snf::server::RankingAwardStatus::Committed
                                  : snf::server::RankingAwardStatus::IdempotencyConflict,
                    .player = request.player,
                    .award_id = request.award_id,
                    .score_delta = request.score_delta,
                    .event_sequence =
                        parse_integer<std::uint64_t>(row[0], "ranking player_sequence"),
                    .event_score = parse_integer<std::uint64_t>(row[3], "ranking absolute_score"),
                    .global_offset = parse_integer<std::uint64_t>(row[4], "ranking global_offset"),
                    .authoritative_score = player.ranking_score,
                    .authoritative_sequence = player.last_domain_event_sequence,
                    .replayed = stored_delta == request.score_delta,
                };
            }

            if (player.ranking_score >
                std::numeric_limits<std::uint64_t>::max() - request.score_delta)
            {
                transaction.commit();
                return rankingFailure(
                    request, snf::server::RankingAwardStatus::ScoreOverflow, player);
            }
            if (player.last_domain_event_sequence == std::numeric_limits<std::uint64_t>::max())
            {
                transaction.commit();
                return rankingFailure(
                    request, snf::server::RankingAwardStatus::SequenceOverflow, player);
            }

            auto stream = _connection.query(
                "SELECT last_offset FROM snf_event_stream WHERE stream_name='ranking' FOR UPDATE");
            if (::mysql_num_rows(stream.get()) != 1)
            {
                throw std::runtime_error{"Ranking event stream cursor is missing"};
            }
            const std::uint64_t last_offset = parse_integer<std::uint64_t>(
                ::mysql_fetch_row(stream.get())[0], "ranking last_offset");
            if (last_offset == std::numeric_limits<std::uint64_t>::max())
            {
                transaction.commit();
                return rankingFailure(
                    request, snf::server::RankingAwardStatus::EventOffsetOverflow, player);
            }

            const std::uint64_t offset = last_offset + 1;
            const std::uint64_t sequence = player.last_domain_event_sequence + 1;
            const std::uint64_t score = player.ranking_score + request.score_delta;
            _connection.execute("UPDATE snf_players SET ranking_score=" + std::to_string(score) +
                                ", last_domain_event_sequence=" + std::to_string(sequence) +
                                " WHERE player_id=" + std::to_string(request.player.value));
            _connection.execute(
                "INSERT INTO snf_player_events (global_offset, player_id, player_sequence, "
                "award_id, score_delta, absolute_score) VALUES (" +
                std::to_string(offset) + "," + std::to_string(request.player.value) + "," +
                std::to_string(sequence) + "," + std::to_string(request.award_id.value) + "," +
                std::to_string(request.score_delta) + "," + std::to_string(score) + ")");
            _connection.execute("UPDATE snf_event_stream SET last_offset=" +
                                std::to_string(offset) + " WHERE stream_name='ranking'");
            if (_config.ranking_award_fault_injector)
            {
                _config.ranking_award_fault_injector(
                    snf::server::MySqlRankingAwardFaultPoint::BeforeCommit);
            }
            transaction.commit();
            if (_config.ranking_award_fault_injector)
            {
                _config.ranking_award_fault_injector(
                    snf::server::MySqlRankingAwardFaultPoint::AfterCommitBeforeCompletion);
            }
            return snf::server::RankingAwardTransactionResult{
                .status = snf::server::RankingAwardStatus::Committed,
                .player = request.player,
                .award_id = request.award_id,
                .score_delta = request.score_delta,
                .event_sequence = sequence,
                .event_score = score,
                .global_offset = offset,
                .authoritative_score = score,
                .authoritative_sequence = sequence,
                .replayed = false,
            };
        }

        [[nodiscard]] std::vector<snf::server::PlayerEventRecord>
        rankingEventsAfter(const std::uint64_t offset, const std::size_t limit)
        {
            if (limit == 0)
            {
                throw std::invalid_argument{"Ranking event read limit must be positive"};
            }
            auto rows = _connection.query(
                "SELECT global_offset, player_id, player_sequence, absolute_score "
                "FROM snf_player_events WHERE global_offset>" +
                std::to_string(offset) + " ORDER BY global_offset LIMIT " + std::to_string(limit));
            std::vector<snf::server::PlayerEventRecord> records;
            records.reserve(static_cast<std::size_t>(::mysql_num_rows(rows.get())));
            while (MYSQL_ROW row = ::mysql_fetch_row(rows.get()))
            {
                records.push_back(snf::server::PlayerEventRecord{
                    .offset = parse_integer<std::uint64_t>(row[0], "ranking global_offset"),
                    .event =
                        snf::server::PlayerScoreChanged{
                            .player = snf::server::PlayerId{.value = parse_integer<std::uint64_t>(
                                                                row[1], "ranking player_id")},
                            .sequence =
                                parse_integer<std::uint64_t>(row[2], "ranking player_sequence"),
                            .score = parse_integer<std::uint64_t>(row[3], "ranking absolute_score"),
                        },
                });
            }
            return records;
        }

        [[nodiscard]] std::uint64_t rankingTailOffset()
        {
            auto row = _connection.query(
                "SELECT last_offset FROM snf_event_stream WHERE stream_name='ranking'");
            if (::mysql_num_rows(row.get()) != 1)
            {
                throw std::runtime_error{"Ranking event stream cursor is missing"};
            }
            return parse_integer<std::uint64_t>(::mysql_fetch_row(row.get())[0],
                                                "ranking last_offset");
        }

        [[nodiscard]] snf::server::RankingCheckpoint loadRankingCheckpoint()
        {
            Transaction transaction{_connection};
            auto meta = _connection.query(
                "SELECT global_offset, generation FROM snf_ranking_checkpoint_meta "
                "WHERE singleton_id=1 FOR UPDATE");
            if (::mysql_num_rows(meta.get()) != 1)
            {
                throw std::runtime_error{"Ranking checkpoint metadata is missing"};
            }
            MYSQL_ROW meta_row = ::mysql_fetch_row(meta.get());
            const std::uint64_t offset =
                parse_integer<std::uint64_t>(meta_row[0], "ranking checkpoint offset");
            const std::uint64_t generation =
                parse_integer<std::uint64_t>(meta_row[1], "ranking checkpoint generation");

            auto rows = _connection.query("SELECT player_id, score, player_sequence "
                                          "FROM snf_ranking_checkpoint_entries WHERE generation=" +
                                          std::to_string(generation) + " ORDER BY player_id");
            std::vector<snf::server::RankingEntry> entries;
            entries.reserve(static_cast<std::size_t>(::mysql_num_rows(rows.get())));
            while (MYSQL_ROW row = ::mysql_fetch_row(rows.get()))
            {
                entries.push_back(snf::server::RankingEntry{
                    .player = snf::server::PlayerId{.value = parse_integer<std::uint64_t>(
                                                        row[0], "checkpoint player_id")},
                    .score = parse_integer<std::uint64_t>(row[1], "checkpoint score"),
                    .last_sequence =
                        parse_integer<std::uint64_t>(row[2], "checkpoint player_sequence"),
                });
            }
            snf::server::RankingCheckpoint checkpoint{
                .offset = offset,
                .entries = std::move(entries),
            };
            snf::server::RankingProjection validation;
            validation.restore(checkpoint);
            transaction.commit();
            return checkpoint;
        }

        void saveRankingCheckpoint(const snf::server::RankingCheckpoint& checkpoint)
        {
            snf::server::RankingProjection validation;
            validation.restore(checkpoint);

            auto current = _connection.query(
                "SELECT global_offset, generation FROM snf_ranking_checkpoint_meta "
                "WHERE singleton_id=1");
            if (::mysql_num_rows(current.get()) != 1)
            {
                throw std::runtime_error{"Ranking checkpoint metadata is missing"};
            }
            MYSQL_ROW current_row = ::mysql_fetch_row(current.get());
            const std::uint64_t current_offset =
                parse_integer<std::uint64_t>(current_row[0], "ranking checkpoint offset");
            const std::uint64_t current_generation =
                parse_integer<std::uint64_t>(current_row[1], "ranking checkpoint generation");
            if (checkpoint.offset < current_offset)
            {
                throw std::out_of_range{"Ranking checkpoint would regress its durable offset"};
            }
            if (current_generation == std::numeric_limits<std::uint64_t>::max())
            {
                throw std::overflow_error{"Ranking checkpoint generation is exhausted"};
            }
            const std::uint64_t next_generation = current_generation + 1;

            // A post-commit process crash may leave the previously active generation.
            // Reclaim it before writing the next one without taking the stream lock.
            _connection.execute("DELETE FROM snf_ranking_checkpoint_entries WHERE generation<" +
                                std::to_string(current_generation));

            Transaction transaction{_connection};
            for (const snf::server::RankingEntry& entry : checkpoint.entries)
            {
                _connection.execute("INSERT INTO snf_ranking_checkpoint_entries "
                                    "(generation, player_id, score, player_sequence) VALUES (" +
                                    std::to_string(next_generation) + "," +
                                    std::to_string(entry.player.value) + "," +
                                    std::to_string(entry.score) + "," +
                                    std::to_string(entry.last_sequence) + ")");
            }

            // Only the pointer swap is serialized with awards. Snapshot row writes
            // above do not hold the strict ranking stream cursor.
            if (_config.ranking_checkpoint_fault_injector)
            {
                _config.ranking_checkpoint_fault_injector(
                    snf::server::MySqlRankingCheckpointFaultPoint::BeforePointerSwap);
            }
            auto stream = _connection.query(
                "SELECT last_offset FROM snf_event_stream WHERE stream_name='ranking' FOR UPDATE");
            if (::mysql_num_rows(stream.get()) != 1)
            {
                throw std::runtime_error{"Ranking event stream cursor is missing"};
            }
            const std::uint64_t tail = parse_integer<std::uint64_t>(
                ::mysql_fetch_row(stream.get())[0], "ranking last_offset");
            if (checkpoint.offset > tail)
            {
                throw std::out_of_range{"Ranking checkpoint is beyond the event tail"};
            }

            auto locked_meta = _connection.query(
                "SELECT global_offset, generation FROM snf_ranking_checkpoint_meta "
                "WHERE singleton_id=1 FOR UPDATE");
            if (::mysql_num_rows(locked_meta.get()) != 1)
            {
                throw std::runtime_error{"Ranking checkpoint metadata is missing"};
            }
            MYSQL_ROW locked_meta_row = ::mysql_fetch_row(locked_meta.get());
            const std::uint64_t locked_offset =
                parse_integer<std::uint64_t>(locked_meta_row[0], "ranking checkpoint offset");
            const std::uint64_t locked_generation =
                parse_integer<std::uint64_t>(locked_meta_row[1], "ranking checkpoint generation");
            if (locked_generation != current_generation || checkpoint.offset < locked_offset)
            {
                throw std::runtime_error{"Concurrent ranking checkpoint update detected"};
            }
            _connection.execute("UPDATE snf_ranking_checkpoint_meta SET global_offset=" +
                                std::to_string(checkpoint.offset) + ", generation=" +
                                std::to_string(next_generation) + " WHERE singleton_id=1");
            if (_config.ranking_checkpoint_fault_injector)
            {
                _config.ranking_checkpoint_fault_injector(
                    snf::server::MySqlRankingCheckpointFaultPoint::BeforeCommit);
            }
            transaction.commit();
            if (_config.ranking_checkpoint_fault_injector)
            {
                _config.ranking_checkpoint_fault_injector(
                    snf::server::MySqlRankingCheckpointFaultPoint::AfterCommit);
            }
            _connection.execute("DELETE FROM snf_ranking_checkpoint_entries WHERE generation<" +
                                std::to_string(next_generation));
        }

    private:
        void ensureSchema()
        {
            _connection.execute(
                "CREATE TABLE IF NOT EXISTS snf_schema_version (version INT UNSIGNED NOT NULL "
                "PRIMARY KEY) ENGINE=InnoDB");
            _connection.execute("INSERT IGNORE INTO snf_schema_version (version) VALUES (1)");
            auto version = _connection.query("SELECT MAX(version) FROM snf_schema_version");
            MYSQL_ROW version_row = ::mysql_fetch_row(version.get());
            if (version_row == nullptr)
            {
                throw std::runtime_error{"MySQL schema version is unsupported"};
            }
            const std::uint32_t schema_version =
                parse_integer<std::uint32_t>(version_row[0], "schema version");
            if (schema_version == 0 || schema_version > 4)
            {
                throw std::runtime_error{"MySQL schema version is unsupported"};
            }

            _connection.execute(
                "CREATE TABLE IF NOT EXISTS snf_players ("
                "player_id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                "handled_command_count BIGINT UNSIGNED NOT NULL, "
                "zone_id BIGINT UNSIGNED NULL, position_x INT NULL, position_y INT NULL, "
                "currency_balance BIGINT UNSIGNED NOT NULL, "
                "purchased_item_count BIGINT UNSIGNED NOT NULL, "
                "ranking_score BIGINT UNSIGNED NOT NULL, "
                "last_domain_event_sequence BIGINT UNSIGNED NOT NULL, "
                "CONSTRAINT snf_player_location_complete CHECK ((zone_id IS NULL AND "
                "position_x IS NULL AND position_y IS NULL) OR (zone_id IS NOT NULL AND "
                "position_x IS NOT NULL AND position_y IS NOT NULL))) ENGINE=InnoDB");
            _connection.execute(
                "CREATE TABLE IF NOT EXISTS snf_purchase_idempotency ("
                "player_id BIGINT UNSIGNED NOT NULL, idempotency_key BIGINT UNSIGNED NOT NULL, "
                "product_id INT UNSIGNED NOT NULL, status TINYINT UNSIGNED NOT NULL, "
                "created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), "
                "PRIMARY KEY (player_id, idempotency_key), "
                "CONSTRAINT snf_purchase_player_fk FOREIGN KEY (player_id) "
                "REFERENCES snf_players(player_id) ON DELETE CASCADE) ENGINE=InnoDB");
            _connection.execute(
                "CREATE TABLE IF NOT EXISTS snf_event_stream ("
                "stream_name VARCHAR(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY "
                "KEY, last_offset BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB");
            _connection.execute("INSERT IGNORE INTO snf_event_stream (stream_name, last_offset) "
                                "VALUES ('ranking',0)");
            _connection.execute(
                "CREATE TABLE IF NOT EXISTS snf_player_events ("
                "global_offset BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                "player_id BIGINT UNSIGNED NOT NULL, "
                "player_sequence BIGINT UNSIGNED NOT NULL, "
                "award_id BIGINT UNSIGNED NOT NULL, "
                "score_delta BIGINT UNSIGNED NOT NULL, "
                "absolute_score BIGINT UNSIGNED NOT NULL, "
                "created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6), "
                "UNIQUE KEY snf_player_event_sequence (player_id, player_sequence), "
                "UNIQUE KEY snf_player_event_award (player_id, award_id), "
                "CONSTRAINT snf_player_event_player_fk FOREIGN KEY (player_id) "
                "REFERENCES snf_players(player_id) ON DELETE CASCADE) ENGINE=InnoDB");
            if (schema_version == 1)
            {
                _connection.execute("INSERT INTO snf_schema_version (version) VALUES (2)");
            }
            _connection.execute("CREATE TABLE IF NOT EXISTS snf_ranking_checkpoint_meta ("
                                "singleton_id TINYINT UNSIGNED NOT NULL PRIMARY KEY, "
                                "global_offset BIGINT UNSIGNED NOT NULL, "
                                "CONSTRAINT snf_ranking_checkpoint_singleton CHECK "
                                "(singleton_id=1)) ENGINE=InnoDB");
            _connection.execute(
                "INSERT IGNORE INTO snf_ranking_checkpoint_meta (singleton_id, global_offset) "
                "VALUES (1,0)");
            _connection.execute("CREATE TABLE IF NOT EXISTS snf_ranking_checkpoint_entries ("
                                "player_id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
                                "score BIGINT UNSIGNED NOT NULL, "
                                "player_sequence BIGINT UNSIGNED NOT NULL) ENGINE=InnoDB");
            if (schema_version <= 2)
            {
                _connection.execute("INSERT INTO snf_schema_version (version) VALUES (3)");
            }
            if (schema_version <= 3)
            {
                auto meta_generation = _connection.query(
                    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() "
                    "AND table_name='snf_ranking_checkpoint_meta' AND column_name='generation'");
                if (parse_integer<std::uint64_t>(::mysql_fetch_row(meta_generation.get())[0],
                                                 "checkpoint meta generation column count") == 0)
                {
                    _connection.execute(
                        "ALTER TABLE snf_ranking_checkpoint_meta ADD COLUMN generation "
                        "BIGINT UNSIGNED NOT NULL DEFAULT 0");
                }

                auto entry_generation = _connection.query(
                    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() "
                    "AND table_name='snf_ranking_checkpoint_entries' AND column_name='generation'");
                if (parse_integer<std::uint64_t>(::mysql_fetch_row(entry_generation.get())[0],
                                                 "checkpoint entry generation column count") == 0)
                {
                    _connection.execute(
                        "ALTER TABLE snf_ranking_checkpoint_entries ADD COLUMN generation "
                        "BIGINT UNSIGNED NOT NULL DEFAULT 0");
                }
                _connection.execute("ALTER TABLE snf_ranking_checkpoint_entries DROP PRIMARY KEY, "
                                    "ADD PRIMARY KEY (generation, player_id)");
                _connection.execute("INSERT INTO snf_schema_version (version) VALUES (4)");
            }
        }

        [[nodiscard]] static snf::server::PurchaseTransactionResult
        result(const snf::server::PurchaseRequest& request,
               const snf::server::PurchaseStatus status,
               const std::uint64_t currency,
               const std::uint64_t items)
        {
            return snf::server::PurchaseTransactionResult{
                .status = status,
                .player = request.player,
                .idempotency_key = request.idempotency_key,
                .product = request.product,
                .currency_balance = currency,
                .purchased_item_count = items,
                .replayed = false,
            };
        }

        [[nodiscard]] static snf::server::RankingAwardTransactionResult
        rankingFailure(const snf::server::RankingAwardRequest& request,
                       const snf::server::RankingAwardStatus status,
                       const snf::server::PlayerRecord& player)
        {
            return snf::server::RankingAwardTransactionResult{
                .status = status,
                .player = request.player,
                .award_id = request.award_id,
                .score_delta = request.score_delta,
                .authoritative_score = player.ranking_score,
                .authoritative_sequence = player.last_domain_event_sequence,
            };
        }

        const snf::server::MySqlPlayerRepositoryConfig& _config;
        MySqlConnection _connection;
    };
}

namespace snf::server
{
    class MySqlPlayerRepository::Impl final
    {
    public:
        struct LoadJob
        {
            PlayerId player;
            PlayerLoadCompletion completion;
        };

        struct SaveJob
        {
            PlayerRecord record;
            PlayerSaveCompletion completion;
        };

        struct PurchaseJob
        {
            PurchaseRequest request;
            PurchaseCompletion completion;
        };

        struct RankingAwardJob
        {
            RankingAwardRequest request;
            RankingAwardCompletion completion;
        };

        using Job = std::variant<LoadJob, SaveJob, PurchaseJob, RankingAwardJob>;

        explicit Impl(MySqlPlayerRepositoryConfig config)
            : _config(std::move(config))
            , _jobs(_config.queue_capacity)
        {
            validate_config(_config);
            MySqlStore schema{_config, true};

            try
            {
                _workers.reserve(_config.worker_count);
                for (std::size_t index = 0; index < _config.worker_count; ++index)
                {
                    _workers.emplace_back([this] { runWorker(); });
                }
            }
            catch (...)
            {
                close();
                join();
                throw;
            }
        }

        ~Impl()
        {
            close();
            join();
        }

        void asyncLoad(const PlayerId player, PlayerLoadCompletion completion)
        {
            if (!completion)
            {
                throw std::invalid_argument{"Player load completion must be callable"};
            }
            if (player.value == 0)
            {
                throw std::invalid_argument{"PlayerId must be non-zero"};
            }
            if (push(Job{LoadJob{.player = player, .completion = completion}}))
            {
                return;
            }
            completion(PlayerLoadResult{
                .status = PlayerRepositoryStatus::Unavailable,
                .record = std::nullopt,
            });
        }

        void asyncSave(PlayerRecord record, PlayerSaveCompletion completion)
        {
            if (!completion)
            {
                throw std::invalid_argument{"Player save completion must be callable"};
            }
            if (record.player.value == 0)
            {
                throw std::invalid_argument{"PlayerId must be non-zero"};
            }
            if (push(Job{SaveJob{.record = std::move(record), .completion = completion}}))
            {
                return;
            }
            completion(PlayerSaveResult{.status = PlayerRepositoryStatus::Unavailable});
        }

        void asyncPurchase(const PurchaseRequest request, PurchaseCompletion completion)
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
            if (push(Job{PurchaseJob{.request = request, .completion = completion}}))
            {
                return;
            }
            completion(unavailable(request));
        }

        void asyncAwardRankingScore(const RankingAwardRequest request,
                                    RankingAwardCompletion completion)
        {
            if (!completion)
            {
                throw std::invalid_argument{"Ranking award completion must be callable"};
            }
            if (request.player.value == 0 || request.award_id.value == 0 ||
                request.score_delta == 0)
            {
                throw std::invalid_argument{"Ranking award identity and delta must be non-zero"};
            }
            if (push(Job{RankingAwardJob{.request = request, .completion = completion}}))
            {
                return;
            }
            completion(rankingUnavailable(request));
        }

        void close() noexcept
        {
            _jobs.close();
        }

        [[nodiscard]] std::optional<PlayerRecord> find(const PlayerId player)
        {
            const auto started_at = std::chrono::steady_clock::now();
            try
            {
                MySqlStore store{_config, false};
                const PlayerLoadResult loaded = store.load(player);
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
                return loaded.record;
            }
            catch (...)
            {
                _operation_failures.fetch_add(1, std::memory_order_relaxed);
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
                return std::nullopt;
            }
        }

        [[nodiscard]] PlayerRepositoryStats stats() const
        {
            return PlayerRepositoryStats{
                .accepted = _accepted.load(std::memory_order_relaxed),
                .rejected = _rejected.load(std::memory_order_relaxed),
                .queue_depth = _jobs.size(),
                .queue_high_water_mark = _jobs.highWaterMark(),
                .purchase_committed = _purchase_committed.load(std::memory_order_relaxed),
                .purchase_replayed = _purchase_replayed.load(std::memory_order_relaxed),
                .purchase_rejected = _purchase_rejected.load(std::memory_order_relaxed),
                .ranking_awards_committed =
                    _ranking_awards_committed.load(std::memory_order_relaxed),
                .ranking_awards_replayed = _ranking_awards_replayed.load(std::memory_order_relaxed),
                .ranking_awards_rejected = _ranking_awards_rejected.load(std::memory_order_relaxed),
                .operation_failures = _operation_failures.load(std::memory_order_relaxed),
                .operation_latency_nanoseconds = _operation_latency.snapshot(),
                .ranking_award_latency_nanoseconds = _ranking_award_latency.snapshot(),
            };
        }

        [[nodiscard]] std::vector<PlayerEventRecord> rankingEventsAfter(const std::uint64_t offset,
                                                                        const std::size_t limit)
        {
            const auto started_at = std::chrono::steady_clock::now();
            try
            {
                MySqlStore store{_config, false};
                auto records = store.rankingEventsAfter(offset, limit);
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
                return records;
            }
            catch (...)
            {
                _operation_failures.fetch_add(1, std::memory_order_relaxed);
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
                throw;
            }
        }

        [[nodiscard]] std::uint64_t rankingTailOffset()
        {
            const auto started_at = std::chrono::steady_clock::now();
            try
            {
                MySqlStore store{_config, false};
                const std::uint64_t offset = store.rankingTailOffset();
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
                return offset;
            }
            catch (...)
            {
                _operation_failures.fetch_add(1, std::memory_order_relaxed);
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
                throw;
            }
        }

        [[nodiscard]] RankingCheckpoint loadRankingCheckpoint()
        {
            const auto started_at = std::chrono::steady_clock::now();
            try
            {
                MySqlStore store{_config, false};
                auto checkpoint = store.loadRankingCheckpoint();
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
                return checkpoint;
            }
            catch (...)
            {
                _operation_failures.fetch_add(1, std::memory_order_relaxed);
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
                throw;
            }
        }

        void saveRankingCheckpoint(const RankingCheckpoint& checkpoint)
        {
            const auto started_at = std::chrono::steady_clock::now();
            try
            {
                MySqlStore store{_config, false};
                store.saveRankingCheckpoint(checkpoint);
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
            }
            catch (...)
            {
                _operation_failures.fetch_add(1, std::memory_order_relaxed);
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
                throw;
            }
        }

    private:
        [[nodiscard]] bool push(Job job)
        {
            if (_jobs.tryPush(std::move(job)))
            {
                _accepted.fetch_add(1, std::memory_order_relaxed);
                return true;
            }
            _rejected.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        void join() noexcept
        {
            for (std::thread& worker : _workers)
            {
                if (worker.joinable())
                {
                    worker.join();
                }
            }
        }

        void runWorker()
        {
            static_cast<void>(::mysql_thread_init());
            std::unique_ptr<MySqlStore> store;
            while (auto job = _jobs.pop())
            {
                const auto started_at = std::chrono::steady_clock::now();
                std::visit(
                    [this, &store, started_at](auto value)
                    {
                        using Value = std::decay_t<decltype(value)>;
                        if constexpr (std::is_same_v<Value, LoadJob>)
                        {
                            PlayerLoadResult result;
                            try
                            {
                                ensureStore(store);
                                result = store->load(value.player);
                            }
                            catch (...)
                            {
                                failStore(store);
                                result = PlayerLoadResult{
                                    .status = PlayerRepositoryStatus::Unavailable,
                                    .record = std::nullopt,
                                };
                            }
                            invoke(value.completion, std::move(result));
                        }
                        else if constexpr (std::is_same_v<Value, SaveJob>)
                        {
                            PlayerSaveResult result;
                            try
                            {
                                ensureStore(store);
                                result = store->save(value.record);
                            }
                            catch (...)
                            {
                                failStore(store);
                                result = PlayerSaveResult{
                                    .status = PlayerRepositoryStatus::Unavailable,
                                };
                            }
                            invoke(value.completion, result);
                        }
                        else if constexpr (std::is_same_v<Value, PurchaseJob>)
                        {
                            PurchaseTransactionResult result;
                            try
                            {
                                ensureStore(store);
                                result = store->purchase(value.request);
                                countPurchase(result);
                            }
                            catch (...)
                            {
                                failStore(store);
                                result = unavailable(value.request);
                            }
                            invoke(value.completion, std::move(result));
                        }
                        else
                        {
                            RankingAwardTransactionResult result;
                            try
                            {
                                ensureStore(store);
                                result = store->awardRankingScore(value.request);
                                countRankingAward(result);
                            }
                            catch (...)
                            {
                                failStore(store);
                                result = rankingUnavailable(value.request);
                            }
                            _ranking_award_latency.record(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    std::chrono::steady_clock::now() - started_at));
                            invoke(value.completion, std::move(result));
                        }
                    },
                    std::move(*job));
                const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at);
                _operation_latency.record(duration);
            }
            store.reset();
            ::mysql_thread_end();
        }

        void ensureStore(std::unique_ptr<MySqlStore>& store)
        {
            if (!store)
            {
                store = std::make_unique<MySqlStore>(_config, false);
            }
        }

        void failStore(std::unique_ptr<MySqlStore>& store) noexcept
        {
            _operation_failures.fetch_add(1, std::memory_order_relaxed);
            store.reset();
        }

        void countPurchase(const PurchaseTransactionResult& result) noexcept
        {
            if (result.replayed)
            {
                _purchase_replayed.fetch_add(1, std::memory_order_relaxed);
            }
            else if (result.status == PurchaseStatus::Committed)
            {
                _purchase_committed.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                _purchase_rejected.fetch_add(1, std::memory_order_relaxed);
            }
        }

        void countRankingAward(const RankingAwardTransactionResult& result) noexcept
        {
            if (result.replayed)
            {
                _ranking_awards_replayed.fetch_add(1, std::memory_order_relaxed);
            }
            else if (result.status == RankingAwardStatus::Committed)
            {
                _ranking_awards_committed.fetch_add(1, std::memory_order_relaxed);
            }
            else
            {
                _ranking_awards_rejected.fetch_add(1, std::memory_order_relaxed);
            }
        }

        template <typename Completion, typename Result>
        void invoke(Completion& completion, Result result) noexcept
        {
            try
            {
                completion(std::move(result));
            }
            catch (...)
            {
                _operation_failures.fetch_add(1, std::memory_order_relaxed);
            }
        }

        [[nodiscard]] static PurchaseTransactionResult
        unavailable(const PurchaseRequest& request) noexcept
        {
            return PurchaseTransactionResult{
                .status = PurchaseStatus::Unavailable,
                .player = request.player,
                .idempotency_key = request.idempotency_key,
                .product = request.product,
                .currency_balance = 0,
                .purchased_item_count = 0,
                .replayed = false,
            };
        }

        [[nodiscard]] static RankingAwardTransactionResult
        rankingUnavailable(const RankingAwardRequest& request) noexcept
        {
            return RankingAwardTransactionResult{
                .status = RankingAwardStatus::Unavailable,
                .player = request.player,
                .award_id = request.award_id,
                .score_delta = request.score_delta,
            };
        }

        MySqlPlayerRepositoryConfig _config;
        snf::runtime::BoundedQueue<Job> _jobs;
        std::vector<std::thread> _workers;
        std::atomic<std::uint64_t> _accepted{0};
        std::atomic<std::uint64_t> _rejected{0};
        std::atomic<std::uint64_t> _purchase_committed{0};
        std::atomic<std::uint64_t> _purchase_replayed{0};
        std::atomic<std::uint64_t> _purchase_rejected{0};
        std::atomic<std::uint64_t> _ranking_awards_committed{0};
        std::atomic<std::uint64_t> _ranking_awards_replayed{0};
        std::atomic<std::uint64_t> _ranking_awards_rejected{0};
        std::atomic<std::uint64_t> _operation_failures{0};
        snf::runtime::Distribution _operation_latency;
        snf::runtime::Distribution _ranking_award_latency;
    };

    MySqlPlayerRepository::MySqlPlayerRepository(MySqlPlayerRepositoryConfig config)
        : _impl(std::make_unique<Impl>(std::move(config)))
    {
    }

    MySqlPlayerRepository::~MySqlPlayerRepository() = default;

    void MySqlPlayerRepository::asyncLoad(const PlayerId player, PlayerLoadCompletion completion)
    {
        _impl->asyncLoad(player, std::move(completion));
    }

    void MySqlPlayerRepository::asyncSave(PlayerRecord record, PlayerSaveCompletion completion)
    {
        _impl->asyncSave(std::move(record), std::move(completion));
    }

    void MySqlPlayerRepository::asyncPurchase(PurchaseRequest request,
                                              PurchaseCompletion completion)
    {
        _impl->asyncPurchase(request, std::move(completion));
    }

    void MySqlPlayerRepository::asyncAwardRankingScore(RankingAwardRequest request,
                                                       RankingAwardCompletion completion)
    {
        _impl->asyncAwardRankingScore(request, std::move(completion));
    }

    void MySqlPlayerRepository::close() noexcept
    {
        _impl->close();
    }

    std::optional<PlayerRecord> MySqlPlayerRepository::find(const PlayerId player) const
    {
        return _impl->find(player);
    }

    PlayerRepositoryStats MySqlPlayerRepository::stats() const
    {
        return _impl->stats();
    }

    RankingCheckpoint MySqlPlayerRepository::loadRankingCheckpoint() const
    {
        return _impl->loadRankingCheckpoint();
    }

    void MySqlPlayerRepository::saveRankingCheckpoint(const RankingCheckpoint& checkpoint)
    {
        _impl->saveRankingCheckpoint(checkpoint);
    }

    std::uint64_t MySqlPlayerRepository::rankingTailOffset() const
    {
        return _impl->rankingTailOffset();
    }

    std::vector<PlayerEventRecord>
    MySqlPlayerRepository::rankingEventsAfter(const std::uint64_t offset,
                                              const std::size_t limit) const
    {
        return _impl->rankingEventsAfter(offset, limit);
    }
}
