// Stage 8B DbClient contracts.
//
//   --stub   Admission, queueing, queued timeouts, stale poll events, shutdown.
//            Uses a never-accepted listener, so connections stay mid-handshake and
//            every request lands in the queue. No database needed.
//   --mysql  LoadPlayer end to end, and the in-flight timeout that poisons a
//            connection. Skips with 77 when SNF_MYSQL_TEST_HOST is unset.

#include "snf/adapter/game_actor_factory.hpp"
#include "snf/adapter/game_payloads.hpp"
#include "snf/adapter/game_request_sink.hpp"
#include "snf/adapter/player_actor_adapter.hpp"
#include "snf/net/tcp_listener.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/db_client.hpp"
#include "snf/worker/poller.hpp"
#include "snf/worker/worker.hpp"

#include "socket_test_support.hpp"

#include <mysql/mysql.h>

#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace
{
    using namespace std::chrono_literals;
    using Clock = std::chrono::steady_clock;

    constexpr int SKIP_EXIT_CODE = 77;

    [[nodiscard]] std::optional<std::string> environment(const char* const name)
    {
        const char* const value = std::getenv(name);
        return value == nullptr ? std::nullopt : std::optional<std::string>{value};
    }

    class RecordingSink final : public snf::worker::DbCompletionSink
    {
    public:
        struct Completion
        {
            snf::worker::AwaitKey key;
            snf::worker::DbResult result;
        };

        void completeDb(const snf::worker::AwaitKey key, snf::worker::DbResult result) override
        {
            completions.push_back(Completion{.key = key, .result = std::move(result)});
        }

        [[nodiscard]] const snf::worker::DbFailure* failureAt(const std::size_t index) const
        {
            return std::get_if<snf::worker::DbFailure>(&completions.at(index).result);
        }

        [[nodiscard]] const snf::worker::LoadPlayerResult* loadAt(const std::size_t index) const
        {
            return std::get_if<snf::worker::LoadPlayerResult>(&completions.at(index).result);
        }

        std::vector<Completion> completions;
    };

    [[nodiscard]] snf::worker::AwaitKey awaitKey(const std::uint64_t entity, const std::uint64_t operation)
    {
        return snf::worker::AwaitKey{
            .actor = snf::worker::ActorKey{.kind = snf::worker::ActorKind::Player, .entity = entity},
            .incarnation = snf::worker::ActorIncarnation{1},
            .operation = snf::worker::OperationId{operation},
        };
    }

    [[nodiscard]] snf::worker::DbClientConfig stubConfig(const std::uint16_t port)
    {
        return snf::worker::DbClientConfig{
            .host_ip = "127.0.0.1",
            .port = port,
            .user = "snf",
            .password = "snf-test",
            .database = "snf_test",
            .ssl_mode = snf::worker::DbSslMode::Disabled,
            .connection_count = 1,
            .max_queued_operations = 2,
            .operation_timeout = 50ms,
        };
    }

    void test_config_rejects_hostnames_and_empty_fields()
    {
        auto config = stubConfig(3306);
        assert(snf::worker::isValid(config));

        // A hostname would make connect resolve DNS synchronously on the worker.
        config.host_ip = "db.internal";
        assert(!snf::worker::isValid(config));

        config = stubConfig(3306);
        config.database.clear();
        assert(!snf::worker::isValid(config));

        config = stubConfig(3306);
        config.connection_count = 0;
        assert(!snf::worker::isValid(config));
    }

    void test_queue_admission_and_rejection()
    {
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());

        RecordingSink sink;
        snf::worker::Poller poller{16};
        snf::worker::DbClient client{stubConfig(port), sink};
        client.start(poller);

        // The handshake never completes against a listener nobody accepts, so no
        // slot ever reaches Idle and every request has to queue.
        static_cast<void>(client.advance(snf::worker::WorkerBudgets::defaults().db));
        assert(client.inFlightCount() == 0);

        const auto deadline = Clock::now() + 10s;
        assert(
            client.tryStart(awaitKey(1, 1), snf::worker::LoadPlayerRequest{.player_id = 1}, deadline).status == snf::worker::DbSubmitStatus::Pending
        );
        assert(
            client.tryStart(awaitKey(2, 2), snf::worker::LoadPlayerRequest{.player_id = 2}, deadline).status == snf::worker::DbSubmitStatus::Pending
        );
        assert(client.queuedCount() == 2);

        // Third one exceeds max_queued_operations: bounded admission fails instead
        // of waiting.
        const auto rejected = client.tryStart(awaitKey(3, 3), snf::worker::LoadPlayerRequest{.player_id = 3}, deadline);
        assert(rejected.status == snf::worker::DbSubmitStatus::Rejected);
        assert(!rejected.inline_result.has_value());
        assert(client.queuedCount() == 2);
        assert(client.metrics().submit_rejections == 1);
        assert(sink.completions.empty());

        client.shutdown(poller);
    }

    void test_queued_timeout_leaves_the_connection_alone()
    {
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());

        RecordingSink sink;
        snf::worker::Poller poller{16};
        snf::worker::DbClient client{stubConfig(port), sink};
        client.start(poller);
        static_cast<void>(client.advance(snf::worker::WorkerBudgets::defaults().db));

        const auto opened_before = client.metrics().connections_opened;

        const auto deadline = Clock::now() - 1ms; // already past
        assert(
            client.tryStart(awaitKey(1, 1), snf::worker::LoadPlayerRequest{.player_id = 1}, deadline).status == snf::worker::DbSubmitStatus::Pending
        );

        client.expireDeadlines(Clock::now());

        assert(client.queuedCount() == 0);
        assert(sink.completions.size() == 1);
        const auto* failure = sink.failureAt(0);
        assert(failure != nullptr);
        assert(failure->kind == snf::worker::DbFailureKind::TimedOut);
        // Nothing was ever sent, so the connection must survive. Tearing it down
        // here would turn queue pressure into connection loss.
        assert(!failure->reached_server);
        assert(client.metrics().queued_timeouts == 1);
        assert(client.metrics().connections_poisoned == 0);
        assert(client.metrics().connections_opened == opened_before);

        client.shutdown(poller);
    }

    void test_stale_poll_event_is_ignored()
    {
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());

        RecordingSink sink;
        snf::worker::Poller poller{16};
        snf::worker::DbClient client{stubConfig(port), sink};
        client.start(poller);
        static_cast<void>(client.advance(snf::worker::WorkerBudgets::defaults().db));

        const auto stale_before = client.metrics().stale_poll_events;

        // Generation 0 never belongs to a live slot: generations start at 1.
        client.onPollEvent(snf::worker::PollToken{snf::worker::PollTargetKind::DbConnection, 0, 0});
        // An index past the slot table is equally stale.
        client.onPollEvent(snf::worker::PollToken{snf::worker::PollTargetKind::DbConnection, 99, 1});
        // So is a token for another target kind.
        client.onPollEvent(snf::worker::PollToken{snf::worker::PollTargetKind::ClientConnection, 0, 1});

        assert(client.metrics().stale_poll_events == stale_before + 3);

        client.shutdown(poller);
    }

    void test_shutdown_drains_the_queue()
    {
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());

        RecordingSink sink;
        snf::worker::Poller poller{16};
        snf::worker::DbClient client{stubConfig(port), sink};
        client.start(poller);
        static_cast<void>(client.advance(snf::worker::WorkerBudgets::defaults().db));

        const auto deadline = Clock::now() + 10s;
        assert(
            client.tryStart(awaitKey(1, 1), snf::worker::LoadPlayerRequest{.player_id = 1}, deadline).status == snf::worker::DbSubmitStatus::Pending
        );
        assert(
            client.tryStart(awaitKey(2, 2), snf::worker::LoadPlayerRequest{.player_id = 2}, deadline).status == snf::worker::DbSubmitStatus::Pending
        );

        client.beginShutdown();
        assert(client.shuttingDown());
        assert(client.queuedCount() == 0);
        assert(sink.completions.size() == 2);
        assert(sink.failureAt(0)->kind == snf::worker::DbFailureKind::Overloaded);

        // New work is refused once shutdown has begun.
        assert(
            client.tryStart(awaitKey(3, 3), snf::worker::LoadPlayerRequest{.player_id = 3}, deadline).status == snf::worker::DbSubmitStatus::Rejected
        );

        client.shutdown(poller);
    }

    // The worker owns the DB sockets in its own poller. There is no second polling
    // loop anywhere: DbClient only adds, modifies and removes registrations.
    //
    // Metrics are read after join(), never while the loop runs: the worker thread
    // owns them and reading them concurrently is a genuine data race.
    void test_worker_loop_survives_an_unreachable_database()
    {
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());

        snf::worker::Worker worker(snf::worker::WorkerId{0}, 1, snf::worker::WorkerBudgets::defaults(), snf::worker::WorkerInboxConfig{});
        auto config = stubConfig(port);
        config.connection_count = 2;
        worker.configureDb(config);
        assert(worker.dbEnabled());

        std::thread runner(
            [&worker]()
            {
                worker.run();
            }
        );

        std::this_thread::sleep_for(300ms);

        // Shutdown must finish on its own deadline even though the database never
        // responded: actor quiescence never waits on physical DB progress.
        const auto stop_started_at = Clock::now();
        worker.requestStop();
        runner.join();
        const auto stop_duration = Clock::now() - stop_started_at;

        assert(worker.dbMetrics().connections_opened >= 2);
        // The server never answers, so nothing may reach the ready state.
        assert(worker.dbMetrics().connections_ready == 0);
        // Alive but not spinning. A loop blocked inside the handshake would show no
        // iterations at all; a busy loop would show thousands. With nothing runnable
        // the worker sleeps out its poll timeout, so a handful is exactly right.
        assert(worker.metrics().loop_iterations >= 2);
        assert(worker.metrics().loop_iterations < 500);
        assert(stop_duration < 5s);
    }

    struct MySqlTestConfig
    {
        std::string host;
        std::uint16_t port{3306};
        std::string user;
        std::string password;
        std::string database;
    };

    [[nodiscard]] std::uint16_t testPort()
    {
        const auto text = environment("SNF_MYSQL_TEST_PORT");
        if (!text)
        {
            return 3306;
        }
        std::uint32_t value = 0;
        const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), value);
        if (error != std::errc{} || end != text->data() + text->size() || value == 0 || value > 65535)
        {
            throw std::invalid_argument{"SNF_MYSQL_TEST_PORT is invalid"};
        }
        return static_cast<std::uint16_t>(value);
    }

    [[nodiscard]] std::optional<MySqlTestConfig> testConfig()
    {
        const auto host = environment("SNF_MYSQL_TEST_HOST");
        if (!host)
        {
            return std::nullopt;
        }
        return MySqlTestConfig{
            .host = *host,
            .port = testPort(),
            .user = environment("SNF_MYSQL_TEST_USER").value_or("snf"),
            .password = environment("SNF_MYSQL_TEST_PASSWORD").value_or("snf-test"),
            .database = environment("SNF_MYSQL_TEST_DATABASE").value_or("snf_test"),
        };
    }

    [[nodiscard]] snf::worker::DbClientConfig liveConfig(const MySqlTestConfig& config)
    {
        return snf::worker::DbClientConfig{
            .host_ip = config.host,
            .port = config.port,
            .user = config.user,
            .password = config.password,
            .database = config.database,
            .ssl_mode = snf::worker::DbSslMode::Disabled,
            .connection_count = 2,
            .operation_timeout = 2000ms,
        };
    }

    void test_worker_connects_through_its_own_poller(const MySqlTestConfig& config);

    // Runs the client's loop the way the worker will: poll for readiness, then let
    // the client advance under its budget.
    void pump(snf::worker::DbClient& client, snf::worker::Poller& poller, const std::chrono::milliseconds budget)
    {
        const auto deadline = Clock::now() + budget;
        const auto db_budget = snf::worker::WorkerBudgets::defaults().db;
        while (Clock::now() < deadline)
        {
            const bool more = client.advance(db_budget);
            if (!more)
            {
                for (const auto& event : poller.wait(10ms))
                {
                    client.onPollEvent(event.token);
                }
            }
        }
    }

    void prepareSchema(const MySqlTestConfig& config)
    {
        MYSQL* mysql = ::mysql_init(nullptr);
        assert(mysql != nullptr);
        unsigned int ssl_mode = SSL_MODE_DISABLED;
        static_cast<void>(::mysql_options(mysql, MYSQL_OPT_SSL_MODE, &ssl_mode));
        bool get_public_key = true;
        static_cast<void>(::mysql_options(mysql, MYSQL_OPT_GET_SERVER_PUBLIC_KEY, &get_public_key));
        assert(
            ::mysql_real_connect(
                mysql, config.host.c_str(), config.user.c_str(), config.password.c_str(), config.database.c_str(), config.port, nullptr, 0
            ) != nullptr
        );

        static constexpr std::string_view STATEMENTS[] = {
            "CREATE TABLE IF NOT EXISTS snf_players ("
            "player_id BIGINT UNSIGNED NOT NULL PRIMARY KEY, "
            "handled_command_count BIGINT UNSIGNED NOT NULL, "
            "zone_id BIGINT UNSIGNED NULL, position_x INT NULL, position_y INT NULL, "
            "currency_balance BIGINT UNSIGNED NOT NULL, "
            "purchased_item_count BIGINT UNSIGNED NOT NULL, "
            "street_experience BIGINT UNSIGNED NOT NULL DEFAULT 0, "
            "equipped_skill_id INT UNSIGNED NOT NULL DEFAULT 1) ENGINE=InnoDB",
            "CREATE TABLE IF NOT EXISTS snf_player_skills ("
            "player_id BIGINT UNSIGNED NOT NULL, skill_id INT UNSIGNED NOT NULL, "
            "PRIMARY KEY (player_id, skill_id)) ENGINE=InnoDB",
            "DELETE FROM snf_player_skills WHERE player_id IN (900001, 900002)",
            "DELETE FROM snf_players WHERE player_id IN (900001, 900002)",
            "INSERT INTO snf_players (player_id, handled_command_count, zone_id, position_x, position_y, "
            "currency_balance, purchased_item_count, street_experience, equipped_skill_id) "
            "VALUES (900001, 7, 42, 11, 22, 500, 3, 1200, 2)",
            "INSERT INTO snf_player_skills (player_id, skill_id) VALUES (900001, 1), (900001, 2)",
        };

        for (const std::string_view statement : STATEMENTS)
        {
            assert(::mysql_real_query(mysql, statement.data(), statement.size()) == 0);
        }
        ::mysql_close(mysql);
    }

    void test_load_player_streams_both_queries(const MySqlTestConfig& config)
    {
        prepareSchema(config);

        RecordingSink sink;
        snf::worker::Poller poller{16};
        snf::worker::DbClient client{liveConfig(config), sink};
        client.start(poller);
        pump(client, poller, 3000ms);

        const auto deadline = Clock::now() + 5s;
        assert(
            client.tryStart(awaitKey(900001, 1), snf::worker::LoadPlayerRequest{.player_id = 900001}, deadline).status ==
            snf::worker::DbSubmitStatus::Pending
        );
        assert(
            client.tryStart(awaitKey(900002, 2), snf::worker::LoadPlayerRequest{.player_id = 900002}, deadline).status ==
            snf::worker::DbSubmitStatus::Pending
        );

        while (sink.completions.size() < 2 && Clock::now() < deadline)
        {
            pump(client, poller, 100ms);
        }
        assert(sink.completions.size() == 2);

        for (const auto& completion : sink.completions)
        {
            const auto* loaded = std::get_if<snf::worker::LoadPlayerResult>(&completion.result);
            assert(loaded != nullptr);
            if (completion.key.actor.entity == 900001)
            {
                // The player row and the skill rows arrive as one logical operation
                // with a single completion.
                assert(loaded->found);
                assert(loaded->row.handled_command_count == 7);
                assert(loaded->row.has_location);
                assert(loaded->row.zone_id == 42);
                assert(loaded->row.position_x == 11);
                assert(loaded->row.position_y == 22);
                assert(loaded->row.currency_balance == 500);
                assert(loaded->row.street_experience == 1200);
                assert(loaded->row.equipped_skill_id == 2);
                assert(loaded->owned_skill_ids.size() == 2);
                assert(loaded->owned_skill_ids[0] == 1);
                assert(loaded->owned_skill_ids[1] == 2);
            }
            else
            {
                // A missing player is a successful load with no row, not a failure,
                // and it must not run the skills query.
                assert(!loaded->found);
                assert(loaded->owned_skill_ids.empty());
            }
        }

        assert(client.metrics().operations_completed == 2);
        assert(client.metrics().connections_poisoned == 0);

        client.shutdown(poller);
    }

    void test_worker_connects_through_its_own_poller(const MySqlTestConfig& config)
    {
        snf::worker::Worker worker(snf::worker::WorkerId{0}, 1, snf::worker::WorkerBudgets::defaults(), snf::worker::WorkerInboxConfig{});
        worker.configureDb(liveConfig(config));

        std::thread runner(
            [&worker]()
            {
                worker.run();
            }
        );

        std::this_thread::sleep_for(2s);
        worker.requestStop();
        runner.join();

        // A slot only leaves Connecting when a readiness event arrives through
        // Worker::processPollEvents, so reaching the ready state is what proves the
        // DbConnection branch is wired into the worker's own poll dispatch.
        assert(worker.dbMetrics().connections_ready == 2);
        assert(worker.dbMetrics().connections_poisoned == 0);
    }

    [[nodiscard]] snf::worker::SavePlayerRequest saveRequest(const std::uint64_t player_id, std::vector<std::uint32_t> skills)
    {
        return snf::worker::SavePlayerRequest{
            .player_id = player_id,
            .handled_command_count = 11,
            .has_location = true,
            .zone_id = 3,
            .position_x = -7,
            .position_y = 9,
            .currency_balance = 640,
            .purchased_item_count = 2,
            .street_experience = 1500,
            .equipped_skill_id = 1,
            .owned_skill_ids = std::move(skills),
        };
    }

    void pumpUntil(snf::worker::DbClient& client, snf::worker::Poller& poller, const RecordingSink& sink, const std::size_t completions)
    {
        const auto deadline = Clock::now() + 10s;
        while (sink.completions.size() < completions && Clock::now() < deadline)
        {
            pump(client, poller, 50ms);
        }
    }

    // 8F: the whole transaction commits as one operation and the saved row reads
    // back through the load path.
    void test_save_player_commits_as_one_operation(const MySqlTestConfig& config)
    {
        prepareSchema(config);

        RecordingSink sink;
        snf::worker::Poller poller{16};
        snf::worker::DbClient client{liveConfig(config), sink};
        client.start(poller);
        pump(client, poller, 2000ms);

        const auto deadline = Clock::now() + 8s;
        assert(client.tryStart(awaitKey(900002, 1), saveRequest(900002, {1, 2, 3}), deadline).status == snf::worker::DbSubmitStatus::Pending);
        pumpUntil(client, poller, sink, 1);

        assert(sink.completions.size() == 1);
        const auto* saved = std::get_if<snf::worker::SavePlayerResult>(&sink.completions[0].result);
        assert(saved != nullptr);
        assert(saved->outcome == snf::worker::SaveOutcome::Committed);
        assert(client.metrics().commits_acknowledged == 1);
        assert(client.metrics().commits_unknown == 0);
        assert(client.metrics().connections_poisoned == 0);

        // Read it back: the row and the full skill set landed together.
        assert(
            client.tryStart(awaitKey(900002, 2), snf::worker::LoadPlayerRequest{.player_id = 900002}, deadline).status ==
            snf::worker::DbSubmitStatus::Pending
        );
        pumpUntil(client, poller, sink, 2);

        const auto* loaded = std::get_if<snf::worker::LoadPlayerResult>(&sink.completions[1].result);
        assert(loaded != nullptr);
        assert(loaded->found);
        assert(loaded->row.handled_command_count == 11);
        assert(loaded->row.has_location);
        assert(loaded->row.position_x == -7);
        assert(loaded->row.street_experience == 1500);
        assert(loaded->owned_skill_ids.size() == 3);

        client.shutdown(poller);
    }

    // 8F: a statement failing inside the transaction rolls back, and nothing from
    // the transaction survives. The player row must not be there on its own.
    void test_failed_statement_rolls_back_without_partial_write(const MySqlTestConfig& config)
    {
        prepareSchema(config);

        RecordingSink sink;
        snf::worker::Poller poller{16};
        snf::worker::DbClient client{liveConfig(config), sink};
        client.start(poller);
        pump(client, poller, 2000ms);

        const auto deadline = Clock::now() + 8s;
        // Two identical skill ids collide on the (player_id, skill_id) primary key,
        // so the insert fails after the row upsert has already been applied.
        assert(client.tryStart(awaitKey(900003, 1), saveRequest(900003, {4, 4}), deadline).status == snf::worker::DbSubmitStatus::Pending);
        pumpUntil(client, poller, sink, 1);

        assert(sink.completions.size() == 1);
        const auto* saved = std::get_if<snf::worker::SavePlayerResult>(&sink.completions[0].result);
        assert(saved != nullptr);
        assert(saved->outcome == snf::worker::SaveOutcome::FailedBeforeCommit);
        assert(client.metrics().rollbacks == 1);
        assert(client.metrics().commits_acknowledged == 0);
        assert(client.metrics().commits_unknown == 0);
        // The rollback was acknowledged, so the connection is still good.
        assert(client.metrics().connections_poisoned == 0);

        // The upsert must have been rolled back with the rest of the transaction.
        assert(
            client.tryStart(awaitKey(900003, 2), snf::worker::LoadPlayerRequest{.player_id = 900003}, deadline).status ==
            snf::worker::DbSubmitStatus::Pending
        );
        pumpUntil(client, poller, sink, 2);

        const auto* loaded = std::get_if<snf::worker::LoadPlayerResult>(&sink.completions[1].result);
        assert(loaded != nullptr);
        assert(!loaded->found);

        client.shutdown(poller);
    }

    // 8F: an in-flight timeout before COMMIT was dispatched is known not to have
    // applied, because dropping the connection discards the transaction.
    void test_save_timeout_before_commit_is_failed_before_commit(const MySqlTestConfig& config)
    {
        prepareSchema(config);

        RecordingSink sink;
        snf::worker::Poller poller{16};
        auto client_config = liveConfig(config);
        client_config.connection_count = 1;
        snf::worker::DbClient client{client_config, sink};
        client.start(poller);
        pump(client, poller, 2000ms);

        assert(client.tryStart(awaitKey(900004, 1), saveRequest(900004, {1}), Clock::now() - 1ms).status == snf::worker::DbSubmitStatus::Pending);
        assert(client.inFlightCount() == 1);
        client.expireDeadlines(Clock::now());

        assert(sink.completions.size() == 1);
        const auto* saved = std::get_if<snf::worker::SavePlayerResult>(&sink.completions[0].result);
        assert(saved != nullptr);
        assert(saved->outcome == snf::worker::SaveOutcome::FailedBeforeCommit);
        assert(client.metrics().commits_unknown == 0);
        assert(client.metrics().connections_poisoned == 1);

        client.shutdown(poller);
    }

    void test_save_rejects_an_empty_loadout()
    {
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());

        RecordingSink sink;
        snf::worker::Poller poller{16};
        snf::worker::DbClient client{stubConfig(port), sink};
        client.start(poller);

        // Same rejection the blocking repository makes, applied at admission so no
        // blocked state is ever created for it.
        const auto rejected = client.tryStart(awaitKey(1, 1), saveRequest(1, {}), Clock::now() + 10s);
        assert(rejected.status == snf::worker::DbSubmitStatus::Rejected);
        assert(client.queuedCount() == 0);

        client.shutdown(poller);
    }

    // The Stage 8 vertical slice, end to end over a real socket:
    //   ping arrives for a player that has no actor
    //   -> Loading, LoadPlayer submitted
    //   -> the worker keeps running while the query is outstanding
    //   -> completeDb builds the actor from the load result
    //   -> Loading becomes Queued and the original ping is processed
    //   -> pong comes back on the socket
    void test_player_activation_loads_from_the_database(const MySqlTestConfig& config)
    {
        prepareSchema(config);

        snf::worker::WorkerActorConfig actor_config{};
        actor_config.actor_table_capacity = 16;
        actor_config.max_mailbox_messages_per_actor = 8;
        actor_config.max_mailbox_bytes_per_actor = 64 * 1024;
        actor_config.max_mailbox_messages_total = 64;
        actor_config.max_mailbox_bytes_total = 256 * 1024;
        actor_config.max_turns_per_actor_slice = 8;
        actor_config.await_timeout = 5s;
        actor_config.max_concurrent_loading = 4;

        snf::worker::WorkerNetworkConfig network_config{};
        network_config.table.capacity = 8;
        network_config.poll_registration_capacity = 9;
        network_config.max_accepts_per_poll = 8;
        network_config.receive_chunk_bytes = 1024;

        snf::adapter::GameActorFactory factory;
        snf::adapter::GameRequestSink request_sink;
        // With a database attached the factory stops building players eagerly.
        factory.setPlayerLoadEnabled(true);

        snf::worker::Worker worker(
            snf::worker::WorkerId{0},
            1,
            snf::worker::WorkerBudgets::defaults(),
            snf::worker::WorkerInboxConfig{},
            network_config,
            request_sink,
            actor_config,
            factory
        );
        worker.configureDb(liveConfig(config));
        factory.setTimerAdmission(worker);
        request_sink.setWorker(worker);

        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());
        worker.attachListener(std::move(listener));

        std::thread runner(
            [&worker]()
            {
                worker.run();
            }
        );

        const snf::protocol::Frame ping_frame{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 4321,
            .payload = {std::byte{0xBE}, std::byte{0xEF}},
        };
        const snf::protocol::Frame expected_pong{
            .type = snf::protocol::MessageType::Pong,
            .request_id = ping_frame.request_id,
            .payload = ping_frame.payload,
        };

        auto client = snf::test::connectClient(port);
        snf::test::sendAll(client.getDescriptor(), snf::protocol::encode_frame(ping_frame));

        // The pong only comes back if the activation load completed and the actor
        // then processed the message that triggered it.
        const auto encoded = snf::test::receiveExact(client.getDescriptor(), snf::protocol::encode_frame(expected_pong).size());
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(encoded);
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        assert(decoded.frames.front() == expected_pong);

        worker.requestStop();
        runner.join();

        assert(worker.dbMetrics().operations_completed >= 1);
        assert(worker.dbMetrics().connections_poisoned == 0);
        assert(worker.metrics().actor.activation_loads_started >= 1);
        assert(worker.metrics().actor.activation_load_failures == 0);
        assert(worker.metrics().actor.stale_activation_completions == 0);
        assert(worker.metrics().network.sent_frames == 1);
    }

    // The row-to-domain mapping, without a database in the way.
    void test_loaded_row_becomes_player_state()
    {
        snf::adapter::GameActorFactory factory;
        const snf::worker::LoadPlayerResult loaded{
            .found = true,
            .row =
                snf::worker::LoadedPlayerRow{
                    .player_id = 77,
                    .handled_command_count = 9,
                    .has_location = true,
                    .zone_id = 5,
                    .position_x = -12,
                    .position_y = 34,
                    .currency_balance = 250,
                    .purchased_item_count = 4,
                    .street_experience = 880,
                    .equipped_skill_id = 2,
                },
            .owned_skill_ids = {1, 2},
        };

        auto result = factory.constructLoaded(snf::worker::ActorKey{.kind = snf::worker::ActorKind::Player, .entity = 77}, loaded);
        assert(result.isReady());

        const auto* adapter = dynamic_cast<const snf::adapter::PlayerActorAdapter*>(result.instance.get());
        assert(adapter != nullptr);
        const auto& state = adapter->player().state();
        assert(state.identity().has_value());
        assert(state.identity()->value == 77);
        assert(state.handledCommandCount() == 9);
        assert(state.currencyBalance() == 250);
        assert(state.streetExperience() == 880);
        assert(state.lastLocation().has_value());
        assert(state.lastLocation()->zone.value == 5);
        // A negative coordinate survives the round trip; the column is a signed INT.
        assert(state.lastLocation()->position.x == -12);
        assert(state.lastLocation()->position.y == 34);
        assert(state.getSkillLoadout().getEquippedSkillId().value == 2);

        // A missing row is a new player rather than a failure.
        const auto fresh = factory.constructLoaded(
            snf::worker::ActorKey{.kind = snf::worker::ActorKind::Player, .entity = 78}, snf::worker::LoadPlayerResult{.found = false}
        );
        assert(fresh.isReady());

        // Only players take the activation-load path.
        const auto rejected = factory.constructLoaded(snf::worker::ActorKey{.kind = snf::worker::ActorKind::Zone, .entity = 1}, loaded);
        assert(rejected.isRejected());
    }

    void test_in_flight_timeout_poisons_the_connection(const MySqlTestConfig& config)
    {
        prepareSchema(config);

        RecordingSink sink;
        snf::worker::Poller poller{16};
        auto client_config = liveConfig(config);
        client_config.connection_count = 1;
        snf::worker::DbClient client{client_config, sink};
        client.start(poller);
        pump(client, poller, 3000ms);

        const auto opened_before = client.metrics().connections_opened;

        // A deadline already in the past makes the operation expire while it is on
        // the connection rather than in the queue.
        assert(
            client.tryStart(awaitKey(900001, 1), snf::worker::LoadPlayerRequest{.player_id = 900001}, Clock::now() - 1ms).status ==
            snf::worker::DbSubmitStatus::Pending
        );
        assert(client.inFlightCount() == 1);

        client.expireDeadlines(Clock::now());

        assert(sink.completions.size() == 1);
        const auto* failure = sink.failureAt(0);
        assert(failure != nullptr);
        assert(failure->kind == snf::worker::DbFailureKind::TimedOut);
        // The protocol stream was cut mid-operation, so the connection cannot be
        // reused and a fresh one takes its place.
        assert(failure->reached_server);
        assert(client.metrics().in_flight_timeouts == 1);
        assert(client.metrics().connections_poisoned == 1);
        // Reopening is deferred to the maintenance tick so that a database which is
        // refusing connections cannot turn poisoning into a hot loop.
        assert(client.metrics().connections_opened == opened_before);
        client.maintainConnections(Clock::now());
        assert(client.metrics().connections_opened == opened_before + 1);

        client.shutdown(poller);
    }
}

int main(const int argc, const char* const* const argv)
{
    const std::string_view mode = argc > 1 ? std::string_view{argv[1]} : std::string_view{"--stub"};

    if (::mysql_library_init(0, nullptr, nullptr) != 0)
    {
        std::cerr << "mysql_library_init failed" << std::endl;
        return 1;
    }

    int exit_code = 0;
    if (mode == "--stub")
    {
        std::cout << "Running DbClient contracts (no database)..." << std::endl;
        test_config_rejects_hostnames_and_empty_fields();
        std::cout << "  - test_config_rejects_hostnames_and_empty_fields PASSED" << std::endl;
        test_queue_admission_and_rejection();
        std::cout << "  - test_queue_admission_and_rejection PASSED" << std::endl;
        test_queued_timeout_leaves_the_connection_alone();
        std::cout << "  - test_queued_timeout_leaves_the_connection_alone PASSED" << std::endl;
        test_stale_poll_event_is_ignored();
        std::cout << "  - test_stale_poll_event_is_ignored PASSED" << std::endl;
        test_shutdown_drains_the_queue();
        std::cout << "  - test_shutdown_drains_the_queue PASSED" << std::endl;
        test_worker_loop_survives_an_unreachable_database();
        std::cout << "  - test_worker_loop_survives_an_unreachable_database PASSED" << std::endl;
        test_loaded_row_becomes_player_state();
        std::cout << "  - test_loaded_row_becomes_player_state PASSED" << std::endl;
        test_save_rejects_an_empty_loadout();
        std::cout << "  - test_save_rejects_an_empty_loadout PASSED" << std::endl;
    }
    else if (mode == "--mysql")
    {
        const auto config = testConfig();
        if (!config)
        {
            std::cout << "SNF_MYSQL_TEST_HOST is unset; DbClient live contracts did not run." << std::endl;
            exit_code = SKIP_EXIT_CODE;
        }
        else
        {
            std::cout << "Running DbClient contracts (real MySQL)..." << std::endl;
            test_load_player_streams_both_queries(*config);
            std::cout << "  - test_load_player_streams_both_queries PASSED" << std::endl;
            test_worker_connects_through_its_own_poller(*config);
            std::cout << "  - test_worker_connects_through_its_own_poller PASSED" << std::endl;
            test_in_flight_timeout_poisons_the_connection(*config);
            std::cout << "  - test_in_flight_timeout_poisons_the_connection PASSED" << std::endl;
            test_player_activation_loads_from_the_database(*config);
            std::cout << "  - test_player_activation_loads_from_the_database PASSED" << std::endl;
            test_save_player_commits_as_one_operation(*config);
            std::cout << "  - test_save_player_commits_as_one_operation PASSED" << std::endl;
            test_failed_statement_rolls_back_without_partial_write(*config);
            std::cout << "  - test_failed_statement_rolls_back_without_partial_write PASSED" << std::endl;
            test_save_timeout_before_commit_is_failed_before_commit(*config);
            std::cout << "  - test_save_timeout_before_commit_is_failed_before_commit PASSED" << std::endl;
        }
    }
    else
    {
        std::cerr << "unknown mode: " << mode << std::endl;
        exit_code = 1;
    }

    ::mysql_library_end();
    return exit_code;
}
