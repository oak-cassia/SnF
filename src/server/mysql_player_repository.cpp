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
                ") ON DUPLICATE KEY UPDATE handled_command_count=VALUES(handled_command_count), "
                "zone_id=VALUES(zone_id), position_x=VALUES(position_x), "
                "position_y=VALUES(position_y), currency_balance=VALUES(currency_balance), "
                "purchased_item_count=VALUES(purchased_item_count), "
                "ranking_score=VALUES(ranking_score), "
                "last_domain_event_sequence=VALUES(last_domain_event_sequence)";
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

    private:
        void ensureSchema()
        {
            _connection.execute(
                "CREATE TABLE IF NOT EXISTS snf_schema_version (version INT UNSIGNED NOT NULL "
                "PRIMARY KEY) ENGINE=InnoDB");
            _connection.execute("INSERT IGNORE INTO snf_schema_version (version) VALUES (1)");
            auto version = _connection.query("SELECT MAX(version) FROM snf_schema_version");
            MYSQL_ROW version_row = ::mysql_fetch_row(version.get());
            if (version_row == nullptr ||
                parse_integer<std::uint32_t>(version_row[0], "schema version") != 1)
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

        using Job = std::variant<LoadJob, SaveJob, PurchaseJob>;

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
                .operation_failures = _operation_failures.load(std::memory_order_relaxed),
                .operation_latency_nanoseconds = _operation_latency.snapshot(),
            };
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
                    [this, &store](auto value)
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
                        else
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
                    },
                    std::move(*job));
                _operation_latency.record(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    std::chrono::steady_clock::now() - started_at));
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

        MySqlPlayerRepositoryConfig _config;
        snf::runtime::BoundedQueue<Job> _jobs;
        std::vector<std::thread> _workers;
        std::atomic<std::uint64_t> _accepted{0};
        std::atomic<std::uint64_t> _rejected{0};
        std::atomic<std::uint64_t> _purchase_committed{0};
        std::atomic<std::uint64_t> _purchase_replayed{0};
        std::atomic<std::uint64_t> _purchase_rejected{0};
        std::atomic<std::uint64_t> _operation_failures{0};
        snf::runtime::Distribution _operation_latency;
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
}
