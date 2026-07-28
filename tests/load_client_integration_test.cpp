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
                  })
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
}

int main()
{
    test_completes_non_blocking_ping_pong();
    test_reports_connection_failure();
}
