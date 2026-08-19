#include "snf/load/load_client.hpp"
#include "snf/server/game_server.hpp"
#include "snf/server/mysql_player_repository.hpp"
#include "snf/server/player_persistence_service.hpp"

#include <mysql/mysql.h>

#include <cassert>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <future>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

namespace
{
    using namespace std::chrono_literals;

    std::optional<std::string> environment(const char* const name)
    {
        const char* const value = std::getenv(name);
        return value == nullptr ? std::nullopt : std::optional<std::string>{value};
    }

    std::uint16_t test_port()
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

    snf::server::MySqlPlayerRepositoryConfig config()
    {
        return snf::server::MySqlPlayerRepositoryConfig{
            .host = environment("SNF_MYSQL_TEST_HOST").value(),
            .port = test_port(),
            .user = environment("SNF_MYSQL_TEST_USER").value_or("snf"),
            .password = environment("SNF_MYSQL_TEST_PASSWORD").value_or("snf-test"),
            .database = environment("SNF_MYSQL_TEST_DATABASE").value_or("snf_test"),
            .worker_count = 2,
            .queue_capacity = 32,
            .connect_timeout = 5s,
            .read_timeout = 5s,
            .write_timeout = 5s,
        };
    }

    void execute_sql(const snf::server::MySqlPlayerRepositoryConfig& repository_config, const std::string_view sql)
    {
        MYSQL* connection = ::mysql_init(nullptr);
        assert(connection != nullptr);
        if (::mysql_real_connect(connection,
                                 repository_config.host.c_str(),
                                 repository_config.user.c_str(),
                                 repository_config.password.c_str(),
                                 repository_config.database.c_str(),
                                 repository_config.port,
                                 nullptr,
                                 0) == nullptr)
        {
            const std::string error = ::mysql_error(connection);
            ::mysql_close(connection);
            throw std::runtime_error{error};
        }
        if (::mysql_real_query(connection, sql.data(), sql.size()) != 0)
        {
            const std::string error = ::mysql_error(connection);
            ::mysql_close(connection);
            throw std::runtime_error{error};
        }
        ::mysql_close(connection);
    }

    void reset_storage(const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        {
            snf::server::MySqlPlayerRepository repository{repository_config};
        }
        execute_sql(repository_config, "DELETE FROM snf_players");
    }

    snf::server::PlayerLoadResult load(snf::server::PlayerRepository& repository, const snf::server::PlayerId player)
    {
        std::promise<snf::server::PlayerLoadResult> completion;
        auto future = completion.get_future();
        repository.asyncLoad(player, [&completion](snf::server::PlayerLoadResult result) { completion.set_value(std::move(result)); });
        assert(future.wait_for(5s) == std::future_status::ready);
        return future.get();
    }

    snf::server::PlayerSaveResult save(snf::server::PlayerRepository& repository, snf::server::PlayerRecord record)
    {
        std::promise<snf::server::PlayerSaveResult> completion;
        auto future = completion.get_future();
        repository.asyncSave(std::move(record), [&completion](snf::server::PlayerSaveResult result) { completion.set_value(result); });
        assert(future.wait_for(5s) == std::future_status::ready);
        return future.get();
    }

    void test_record_survives_repository_restart(const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        const snf::server::PlayerId player{.value = 1001};
        {
            snf::server::MySqlPlayerRepository repository{repository_config};
            assert(save(repository,
                        snf::server::PlayerRecord{
                            .player = player,
                            .handled_command_count = 9,
                            .last_location =
                                snf::server::PlayerLocation{
                                    .zone = snf::server::ZoneId{.value = 7},
                                    .position = {.x = -8, .y = 12},
                                },
                            .currency_balance = 700,
                            .purchased_item_count = 3,
                            .street_experience = 29500,
                        })
                       .saved());
        }

        snf::server::MySqlPlayerRepository restarted{repository_config};
        const auto loaded = load(restarted, player);
        assert(loaded.status == snf::server::PlayerRepositoryStatus::Success);
        assert(loaded.record.has_value());
        assert(loaded.record->handled_command_count == 9);
        assert((loaded.record->last_location == snf::server::PlayerLocation{
                                                    .zone = snf::server::ZoneId{.value = 7},
                                                    .position = {.x = -8, .y = 12},
                                                }));
        assert(loaded.record->currency_balance == 700);
        assert(loaded.record->purchased_item_count == 3);
        assert(loaded.record->street_experience == 29500);
    }

    void test_actor_snapshot_overwrites_the_complete_player_record(const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        const snf::server::PlayerId player{.value = 1002};
        snf::server::MySqlPlayerRepository repository{repository_config};
        assert(save(repository,
                    snf::server::PlayerRecord{
                        .player = player,
                        .handled_command_count = 1,
                        .last_location = std::nullopt,
                        .currency_balance = 500,
                        .purchased_item_count = 5,
                        .street_experience = 1000,
                    })
                   .saved());

        snf::server::PlayerPersistenceService persistence{repository,
                                                          snf::server::PlayerPersistenceServiceConfig{
                                                              .queue_capacity = 4,
                                                              .flush_interval = 1ms,
                                                          }};
        assert(persistence.tryEnqueue(snf::server::PlayerRecord{
            .player = player,
            .handled_command_count = 2,
            .last_location = std::nullopt,
            .currency_balance = 700,
            .purchased_item_count = 3,
            .street_experience = 29500,
        }));
        persistence.flush();
        persistence.stop();

        const auto loaded = load(repository, player);
        assert(loaded.record.has_value());
        assert(loaded.record->handled_command_count == 2);
        assert(loaded.record->currency_balance == 700);
        assert(loaded.record->purchased_item_count == 3);
        // The UPDATE clause has to carry the column too, or only the first save lands.
        assert(loaded.record->street_experience == 29500);
    }

    class RunningMySqlServer final
    {
    public:
        explicit RunningMySqlServer(const snf::server::MySqlPlayerRepositoryConfig& repository_config)
            : _server(
                  [&repository_config]
                  {
                      snf::server::GameServerConfig server_config;
                      server_config.port = 0;
                      server_config.shutdown_grace_period = 500ms;
                      server_config.mysql_player_repository = repository_config;
                      return server_config;
                  }())
            , _thread(
                  [this]
                  {
                      try
                      {
                          _server.run();
                      }
                      catch (...)
                      {
                          _failure = std::current_exception();
                      }
                  })
        {
        }

        ~RunningMySqlServer()
        {
            if (_thread.joinable())
            {
                _server.requestStop();
                _thread.join();
            }
        }

        [[nodiscard]] std::uint16_t port() const noexcept
        {
            return _server.getPort();
        }

        void stop()
        {
            _server.requestStop();
            _thread.join();
            if (_failure)
            {
                std::rethrow_exception(_failure);
            }
        }

    private:
        snf::server::GameServer _server;
        std::exception_ptr _failure;
        std::thread _thread;
    };

    void run_zone_cycle(const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        RunningMySqlServer server{repository_config};
        const snf::load::LoadClient client{snf::load::LoadClientConfig{
            .host = "127.0.0.1",
            .port = server.port(),
            .connections = 6,
            .duration = 400ms,
            .requests_per_second = 20,
            .scenario = snf::load::LoadScenario::Zone,
            .players_per_zone = 3,
            .connect_timeout = 2s,
            .request_timeout = 2s,
        }};
        const auto result = client.run();
        assert(result.success);
        assert(result.sent_bootstrap_requests == 12);
        assert(result.received_gameplay_responses > 0);
        server.stop();
    }

    void test_game_server_restores_mysql_players_across_restart(const snf::server::MySqlPlayerRepositoryConfig& repository_config)
    {
        run_zone_cycle(repository_config);
        run_zone_cycle(repository_config);
    }
}

int main()
{
    if (!environment("SNF_MYSQL_TEST_HOST"))
    {
        return 77;
    }

    const auto repository_config = config();
    reset_storage(repository_config);
    test_record_survives_repository_restart(repository_config);
    test_actor_snapshot_overwrites_the_complete_player_record(repository_config);
    test_game_server_restores_mysql_players_across_restart(repository_config);
}
