#include "snf/load/load_client.hpp"
#include "snf/server/game_server.hpp"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <exception>
#include <thread>
#include <utility>

namespace
{
    using namespace std::chrono_literals;

    class RunningServer
    {
    public:
        RunningServer()
            : _server(0, 200ms)
            , _thread(
                  [this]
                  {
                      try
                      {
                          _server.run();
                      }
                      catch (...)
                      {
                          _server_error = std::current_exception();
                      }
                  }
              )
        {
        }

        explicit RunningServer(const snf::server::GameServerConfig& config)
            : _server(config)
            , _thread(
                  [this]
                  {
                      try
                      {
                          _server.run();
                      }
                      catch (...)
                      {
                          _server_error = std::current_exception();
                      }
                  }
              )
        {
        }

        ~RunningServer()
        {
            if (_thread.joinable())
            {
                _server.requestStop();
                _thread.join();
            }
        }

        RunningServer(const RunningServer&) = delete;
        RunningServer& operator=(const RunningServer&) = delete;

        [[nodiscard]] std::uint16_t getPort() const noexcept
        {
            return _server.getPort();
        }

        void stop()
        {
            _server.requestStop();
            _thread.join();

            if (_server_error)
            {
                std::rethrow_exception(_server_error);
            }
        }

    private:
        snf::server::GameServer _server;
        std::exception_ptr _server_error;
        std::thread _thread;
    };

    void test_completes_non_blocking_ping_pong()
    {
        RunningServer server;
        const snf::load::LoadClient client{snf::load::LoadClientConfig{
            .host = "127.0.0.1",
            .port = server.getPort(),
            .connections = 8,
            .duration = 250ms,
            .requests_per_second = 20,
            .connect_timeout = 1s,
            .request_timeout = 1s,
        }};

        const auto result = client.run();

        assert(result.success);
        assert(result.error.empty());
        assert(result.requested_connections == 8);
        assert(result.successful_connections == 8);
        assert(result.failed_connections == 0);
        assert(result.maximum_active_connections == 8);
        assert(result.sent_requests >= 8);
        assert(result.received_responses == result.sent_requests);
        assert(result.request_timeouts == 0);
        assert(result.invalid_responses == 0);
        assert(result.socket_errors == 0);
        assert(result.round_trip_times.size() == result.received_responses);

        for (const auto round_trip_time : result.round_trip_times)
        {
            assert(round_trip_time >= std::chrono::steady_clock::duration::zero());
        }

        server.stop();
    }

    void test_reports_connection_failure()
    {
        std::uint16_t closed_port = 0;

        {
            RunningServer server;
            closed_port = server.getPort();
            server.stop();
        }

        const snf::load::LoadClient client{snf::load::LoadClientConfig{
            .host = "127.0.0.1",
            .port = closed_port,
            .connections = 4,
            .duration = 100ms,
            .requests_per_second = 10,
            .connect_timeout = 200ms,
            .request_timeout = 200ms,
        }};

        const auto result = client.run();

        assert(!result.success);
        assert(!result.error.empty());
        assert(result.requested_connections == 4);
        assert(result.successful_connections == 0);
        assert(result.failed_connections == 4);
    }

    void test_completes_authenticated_zone_movement_workload()
    {
        RunningServer server;
        const snf::load::LoadClient client{snf::load::LoadClientConfig{
            .host = "127.0.0.1",
            .port = server.getPort(),
            .connections = 6,
            .duration = 400ms,
            .requests_per_second = 20,
            .scenario = snf::load::LoadScenario::Zone,
            .players_per_zone = 3,
            .connect_timeout = 1s,
            .request_timeout = 1s,
        }};

        const auto result = client.run();

        assert(result.success);
        assert(result.error.empty());
        assert(result.successful_connections == 6);
        assert(result.failed_connections == 0);
        assert(result.sent_bootstrap_requests == 12);
        assert(result.received_bootstrap_responses == 12);
        assert(result.sent_gameplay_requests > 0);
        assert(result.received_gameplay_responses == result.sent_gameplay_requests);
        assert(result.gameplay_round_trip_times.size() == result.received_gameplay_responses);
        assert(result.request_timeouts == 0);
        assert(result.invalid_responses == 0);
        assert(result.socket_errors == 0);

        server.stop();
    }

    void test_completes_battle_workload_and_accounts_for_push_frames()
    {
        snf::server::GameServerConfig server_config;
        server_config.port = 0;
        server_config.shutdown_grace_period = 200ms;
        server_config.room_battle_duration = 5s;
        server_config.room_tick_interval = 20ms;
        server_config.room_wave_count = 1;
        server_config.room_minions_per_wave = 1;
        server_config.room_minion_health = 1'000'000;
        server_config.room_boss_spawn_after = 4s;

        RunningServer server{server_config};
        const snf::load::LoadClient client{snf::load::LoadClientConfig{
            .host = "127.0.0.1",
            .port = server.getPort(),
            .connections = 4,
            .duration = 700ms,
            .requests_per_second = 50,
            .scenario = snf::load::LoadScenario::Battle,
            .players_per_zone = 4,
            .players_per_room = 4,
            .connect_timeout = 1s,
            .request_timeout = 1s,
        }};

        const auto result = client.run();

        assert(result.success);
        assert(result.error.empty());
        assert(result.successful_connections == 4);
        assert(result.failed_connections == 0);
        assert(result.sent_bootstrap_requests == 13); // auth + Zone + Room for all, BattleStart for one leader
        assert(result.received_bootstrap_responses == result.sent_bootstrap_requests);
        assert(result.sent_gameplay_requests > 0);
        assert(result.received_gameplay_responses == result.sent_gameplay_requests);
        assert(result.battle_digest_frames > 4);
        assert(result.battle_digest_bytes > result.battle_digest_frames * snf::protocol::MIN_BODY_SIZE);
        assert(result.unsolicited_frames == result.battle_digest_frames);
        assert(!result.battle_digest_intervals.empty());
        assert(result.request_timeouts == 0);
        assert(result.invalid_responses == 0);
        assert(result.socket_errors == 0);

        server.stop();
    }
}

int main()
{
    test_completes_non_blocking_ping_pong();
    test_reports_connection_failure();
    test_completes_authenticated_zone_movement_workload();
    test_completes_battle_workload_and_accounts_for_push_frames();
}
