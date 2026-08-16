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
            config.port == 0 || config.worker_count == 0 || config.queue_capacity == 0)
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

    [[nodiscard]] std::string player_select(const snf::server::PlayerId player)
    {
        return "SELECT handled_command_count, zone_id, position_x, position_y, "
               "currency_balance, purchased_item_count FROM snf_players WHERE player_id=" +
               std::to_string(player.value);
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
                .zone = snf::server::ZoneId{
                    .value = parse_integer<std::uint64_t>(row[1], "zone_id")},
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
        };
    }

    class MySqlStore final
    {
    public:
        MySqlStore(const snf::server::MySqlPlayerRepositoryConfig& config,
                   const bool ensure_schema)
            : _connection(config)
        {
            if (ensure_schema)
            {
                ensureSchema();
            }
        }

        [[nodiscard]] snf::server::PlayerLoadResult load(const snf::server::PlayerId player)
        {
            auto result = _connection.query(player_select(player));
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
            return snf::server::PlayerLoadResult{
                .status = snf::server::PlayerRepositoryStatus::Success,
                .record = decode_player(player, ::mysql_fetch_row(result.get())),
            };
        }

        [[nodiscard]] snf::server::PlayerSaveResult
        save(const snf::server::PlayerRecord& record)
        {
            const std::string zone =
                record.last_location ? std::to_string(record.last_location->zone.value) : "NULL";
            const std::string position_x =
                record.last_location ? std::to_string(record.last_location->position.x) : "NULL";
            const std::string position_y =
                record.last_location ? std::to_string(record.last_location->position.y) : "NULL";
            _connection.execute(
                "INSERT INTO snf_players (player_id, handled_command_count, zone_id, "
                "position_x, position_y, currency_balance, purchased_item_count) VALUES (" +
                std::to_string(record.player.value) + "," +
                std::to_string(record.handled_command_count) + "," + zone + "," + position_x + "," +
                position_y + "," + std::to_string(record.currency_balance) + "," +
                std::to_string(record.purchased_item_count) + ") ON DUPLICATE KEY UPDATE "
                "handled_command_count=VALUES(handled_command_count), "
                "zone_id=VALUES(zone_id), position_x=VALUES(position_x), "
                "position_y=VALUES(position_y), currency_balance=VALUES(currency_balance), "
                "purchased_item_count=VALUES(purchased_item_count)");
            return snf::server::PlayerSaveResult{
                .status = snf::server::PlayerRepositoryStatus::Success,
            };
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
            if (schema_version == 0 || schema_version > 6)
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
                "CONSTRAINT snf_player_location_complete CHECK ((zone_id IS NULL AND "
                "position_x IS NULL AND position_y IS NULL) OR (zone_id IS NOT NULL AND "
                "position_x IS NOT NULL AND position_y IS NOT NULL))) ENGINE=InnoDB");

            if (schema_version < 5)
            {
                _connection.execute("DROP TABLE IF EXISTS snf_ranking_checkpoint_entries");
                _connection.execute("DROP TABLE IF EXISTS snf_ranking_checkpoint_meta");
                _connection.execute("DROP TABLE IF EXISTS snf_player_events");
                _connection.execute("DROP TABLE IF EXISTS snf_event_stream");

                auto ranking_score = _connection.query(
                    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() "
                    "AND table_name=\"snf_players\" AND column_name=\"ranking_score\"");
                if (parse_integer<std::uint64_t>(::mysql_fetch_row(ranking_score.get())[0],
                                                 "ranking score column count") != 0)
                {
                    _connection.execute("ALTER TABLE snf_players DROP COLUMN ranking_score");
                }

                auto event_sequence = _connection.query(
                    "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() "
                    "AND table_name=\"snf_players\" AND "
                    "column_name=\"last_domain_event_sequence\"");
                if (parse_integer<std::uint64_t>(::mysql_fetch_row(event_sequence.get())[0],
                                                 "domain event sequence column count") != 0)
                {
                    _connection.execute(
                        "ALTER TABLE snf_players DROP COLUMN last_domain_event_sequence");
                }
                _connection.execute("INSERT INTO snf_schema_version (version) VALUES (5)");
            }

            if (schema_version < 6)
            {
                _connection.execute("DROP TABLE IF EXISTS snf_purchase_idempotency");
                _connection.execute("INSERT INTO snf_schema_version (version) VALUES (6)");
            }
        }

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

        using Job = std::variant<LoadJob, SaveJob>;

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
            if (push(Job{SaveJob{
                    .record = std::move(record),
                    .completion = completion,
                }}))
            {
                return;
            }
            completion(PlayerSaveResult{.status = PlayerRepositoryStatus::Unavailable});
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

        MySqlPlayerRepositoryConfig _config;
        snf::runtime::BoundedQueue<Job> _jobs;
        std::vector<std::thread> _workers;
        std::atomic<std::uint64_t> _accepted{0};
        std::atomic<std::uint64_t> _rejected{0};
        std::atomic<std::uint64_t> _operation_failures{0};
        snf::runtime::Distribution _operation_latency;
    };

    MySqlPlayerRepository::MySqlPlayerRepository(MySqlPlayerRepositoryConfig config)
        : _impl(std::make_unique<Impl>(std::move(config)))
    {
    }

    MySqlPlayerRepository::~MySqlPlayerRepository() = default;

    void MySqlPlayerRepository::asyncLoad(const PlayerId player,
                                          PlayerLoadCompletion completion)
    {
        _impl->asyncLoad(player, std::move(completion));
    }

    void MySqlPlayerRepository::asyncSave(PlayerRecord record,
                                          PlayerSaveCompletion completion)
    {
        _impl->asyncSave(std::move(record), std::move(completion));
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
