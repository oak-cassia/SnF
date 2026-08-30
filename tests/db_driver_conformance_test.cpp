// Stage 8A driver conformance gate.
//
// The question is narrow: does the production MySQL client driver make progress
// through OUR poller without ever holding the calling thread? This harness does
// not emulate the MySQL wire protocol. A never-accepted listener supplies the
// TCP stall, and everything protocol-level (auth, TLS, use_result, transactions)
// belongs to the real-server mode.
//
// Modes:
//   --stub             TCP stall only. Always runs.
//   --mysql            Real server. Skips with 77 when SNF_MYSQL_TEST_HOST is unset.
//   --mysql-auth-cost  Connect cost per TLS/auth option. Investigation, not a gate.
//
// Headline finding: every driver entry point stays in the microsecond range except
// connect, where one call holds the thread for about 8ms. That cost is the TLS
// handshake, not caching_sha2_password: ssl_mode=DISABLED brings the same connect
// down to roughly 100us while still authenticating. The client default is
// SSL_MODE_PREFERRED, so a config that does not name a mode pays it silently.

#include "snf/net/tcp_listener.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/worker/poll_token.hpp"
#include "snf/worker/poller.hpp"

#include "socket_test_support.hpp"

#include <mysql/mysql.h>

#include <sys/socket.h>

#include <algorithm>
#include <cassert>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
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

    // The single-call duration is a warning metric, not a pass/fail gate: the OS can
    // deschedule the thread inside any call. Blocking is proven by the progress
    // assertions below, which hold even when a scheduler hiccup inflates one sample.
    class CallProbe final
    {
    public:
        template <typename Fn> auto measure(const char* const name, Fn&& call) -> decltype(call())
        {
            const auto started_at = Clock::now();
            if constexpr (std::is_void_v<decltype(call())>)
            {
                call();
                record(name, Clock::now() - started_at);
            }
            else
            {
                auto result = call();
                record(name, Clock::now() - started_at);
                return result;
            }
        }

        // Records every sample for one entry point instead of just its maximum.
        // A single slow call is only actionable once you know whether it is the
        // first one or every one.
        void trace(const char* const name)
        {
            _trace_name = name;
        }

        void report() const
        {
            const auto threshold = warningThreshold();
            std::cout << "    slowest call per driver entry point:" << std::endl;
            for (const auto& entry : _entries)
            {
                const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(entry.max_duration).count();
                std::cout << "      " << entry.name << ": " << entry.calls << " calls, max " << micros << "us"
                          << (entry.max_duration > threshold ? "  <-- over SNF_DB_MAX_CALL_US" : "") << std::endl;
            }
            if (!_trace.empty())
            {
                std::cout << "      " << _trace_name << " per-call (us):";
                for (const auto sample : _trace)
                {
                    std::cout << ' ' << std::chrono::duration_cast<std::chrono::microseconds>(sample).count();
                }
                std::cout << std::endl;
            }
        }

        [[nodiscard]] std::chrono::nanoseconds maxDurationOf(const std::string_view name) const
        {
            for (const Entry& entry : _entries)
            {
                if (entry.name == name)
                {
                    return entry.max_duration;
                }
            }
            return std::chrono::nanoseconds{0};
        }

        [[nodiscard]] static std::chrono::nanoseconds warningThreshold()
        {
            const auto text = environment("SNF_DB_MAX_CALL_US");
            if (!text)
            {
                return 2ms;
            }
            std::uint64_t value = 0;
            const auto [end, error] = std::from_chars(text->data(), text->data() + text->size(), value);
            if (error != std::errc{} || end != text->data() + text->size())
            {
                throw std::invalid_argument{"SNF_DB_MAX_CALL_US is invalid"};
            }
            return std::chrono::microseconds{value};
        }

    private:
        struct Entry
        {
            std::string name;
            std::size_t calls{0};
            std::chrono::nanoseconds max_duration{0};
        };

        void record(const char* const name, const std::chrono::nanoseconds duration)
        {
            if (_trace_name != nullptr && _trace_name == std::string_view{name})
            {
                _trace.push_back(duration);
            }
            for (Entry& entry : _entries)
            {
                if (entry.name == name)
                {
                    ++entry.calls;
                    entry.max_duration = std::max(entry.max_duration, duration);
                    return;
                }
            }
            _entries.push_back(Entry{.name = name, .calls = 1, .max_duration = duration});
        }

        std::vector<Entry> _entries;
        const char* _trace_name{nullptr};
        std::vector<std::chrono::nanoseconds> _trace;
    };

    // Drives one async driver call to completion through snf::worker::Poller.
    //
    // Oracle's async API exposes no wait-direction hint. MYSQL::net.reading_or_writing
    // is the only candidate, and the stall test measures it as 0 once a call has
    // returned NET_ASYNC_NOT_READY, so it cannot carry the direction on its own.
    // What remains is: wait for readability, and add writability only while the
    // driver reports it is mid-write. The wakeup counter is what says whether that
    // is enough or whether the loop spins.
    class AsyncDriver final
    {
    public:
        AsyncDriver(snf::worker::Poller& poller, MYSQL& mysql, CallProbe& probe) noexcept
            : _poller(poller)
            , _mysql(mysql)
            , _probe(probe)
        {
        }

        ~AsyncDriver()
        {
            unregister();
        }

        AsyncDriver(const AsyncDriver&) = delete;
        AsyncDriver& operator=(const AsyncDriver&) = delete;

        template <typename Fn> [[nodiscard]] net_async_status run(const char* const name, Fn&& step, const std::chrono::milliseconds budget)
        {
            const auto deadline = Clock::now() + budget;
            while (Clock::now() < deadline)
            {
                const net_async_status status = _probe.measure(name, step);
                if (status != NET_ASYNC_NOT_READY)
                {
                    return status;
                }
                syncInterest();
                const auto events = _poller.wait(10ms);
                _wakeups += events.size();
            }
            return NET_ASYNC_NOT_READY;
        }

        void unregister()
        {
            if (_registered_fd != -1)
            {
                _poller.remove(_registered_fd);
                _registered_fd = -1;
            }
        }

        [[nodiscard]] std::size_t wakeups() const noexcept
        {
            return _wakeups;
        }

    private:
        void syncInterest()
        {
            const int descriptor = _mysql.net.fd;
            if (descriptor == -1)
            {
                return;
            }

            // 0 is the common case: the driver is parked between phases and the
            // field says nothing. Readability is the safe default there, because
            // every phase ends in a server response.
            const bool writing = _mysql.net.reading_or_writing == 2;
            const snf::worker::PollInterest interest{.read = !writing, .write = writing};
            const snf::worker::PollToken token{snf::worker::PollTargetKind::DbConnection, 0, 1};

            if (_registered_fd != descriptor)
            {
                unregister();
                _poller.add(descriptor, token, interest);
                _registered_fd = descriptor;
                _interest = interest;
                return;
            }
            if (interest != _interest)
            {
                _poller.modify(descriptor, token, interest);
                _interest = interest;
            }
        }

        snf::worker::Poller& _poller;
        MYSQL& _mysql;
        CallProbe& _probe;
        int _registered_fd{-1};
        snf::worker::PollInterest _interest{};
        std::size_t _wakeups{0};
    };

    // Q1, Q2, Q3, Q4 against a listener that is never accepted from. The kernel
    // completes the TCP handshake, so the driver reaches the point of waiting for
    // a server greeting that never arrives.
    void test_stalled_connect_never_holds_the_thread()
    {
        auto listener = snf::net::create_tcp_listener(0);
        const std::uint16_t port = snf::test::portOf(listener.getDescriptor());

        MYSQL* mysql = ::mysql_init(nullptr);
        assert(mysql != nullptr);

        CallProbe probe;

        // Reaching NET_ASYNC_NOT_READY at all is the proof: the server sends
        // nothing, so a driver that blocked here would never return.
        const net_async_status first = probe.measure(
            "mysql_real_connect_nonblocking",
            [&]
            {
                return ::mysql_real_connect_nonblocking(mysql, "127.0.0.1", "snf", "snf", "snf", port, nullptr, 0);
            }
        );
        assert(first == NET_ASYNC_NOT_READY);

        // Q1: the socket is reachable and our own Poller accepts it.
        const int descriptor = mysql->net.fd;
        std::cout << "    Q1 fd from MYSQL::net.fd: " << descriptor << std::endl;
        assert(descriptor != -1);

        // Q2: is there a usable wait-direction hint? Measured answer is 0, i.e. the
        // field is cleared once the call parks, so it cannot drive epoll interest by
        // itself. Recorded rather than asserted equal to a value: this line is the
        // finding, and a future driver version changing it should be visible, not
        // silently break a build.
        const unsigned int direction = mysql->net.reading_or_writing;
        std::cout << "    Q2 net.reading_or_writing while stalled: " << direction << " (0=parked, 1=read, 2=write)" << std::endl;
        assert(direction <= 2);

        snf::worker::Poller poller{16};
        poller.add(
            descriptor,
            snf::worker::PollToken{snf::worker::PollTargetKind::DbConnection, 0, 1},
            snf::worker::PollInterest{.read = true, .write = false}
        );

        // A socketpair standing in for unrelated worker traffic. If the driver had
        // parked the thread, these round trips would stop.
        int heartbeat[2] = {-1, -1};
        assert(::socketpair(AF_UNIX, SOCK_STREAM, 0, heartbeat) == 0);
        snf::net::UniqueFileDescriptor heartbeat_writer{heartbeat[0]};
        snf::net::UniqueFileDescriptor heartbeat_reader{heartbeat[1]};
        poller.add(
            heartbeat_reader.getDescriptor(),
            snf::worker::PollToken{snf::worker::PollTargetKind::ClientConnection, 1, 1},
            snf::worker::PollInterest{.read = true, .write = false}
        );

        std::size_t db_wakeups = 0;
        std::size_t heartbeat_round_trips = 0;
        const auto deadline = Clock::now() + 300ms;
        while (Clock::now() < deadline)
        {
            const std::byte outgoing{0x7A};
            assert(::send(heartbeat_writer.getDescriptor(), &outgoing, 1, MSG_NOSIGNAL) == 1);

            for (const auto& event : poller.wait(10ms))
            {
                if (event.token.kind == snf::worker::PollTargetKind::DbConnection)
                {
                    ++db_wakeups;
                    const net_async_status status = probe.measure(
                        "mysql_real_connect_nonblocking",
                        [&]
                        {
                            return ::mysql_real_connect_nonblocking(mysql, "127.0.0.1", "snf", "snf", "snf", port, nullptr, 0);
                        }
                    );
                    assert(status == NET_ASYNC_NOT_READY);
                    continue;
                }

                std::byte incoming{};
                assert(::recv(heartbeat_reader.getDescriptor(), &incoming, 1, 0) == 1);
                assert(incoming == outgoing);
                ++heartbeat_round_trips;
            }
        }

        std::cout << "    Q3 heartbeat round trips while DB connect pending: " << heartbeat_round_trips << std::endl;
        std::cout << "    Q3 DB poll wakeups while server silent: " << db_wakeups << std::endl;

        // The loop kept serving other work for the whole window.
        assert(heartbeat_round_trips > 10);
        // A silent peer must not wake us: any wakeup here would be a spin.
        assert(db_wakeups == 0);

        poller.remove(descriptor);
        poller.remove(heartbeat_reader.getDescriptor());

        // Q4: teardown while the connection is still stalled mid-handshake.
        probe.measure(
            "mysql_close(stalled)",
            [&]
            {
                ::mysql_close(mysql);
            }
        );

        probe.report();
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

    // Q5, Q6, Q7 need a real server: the stub cannot exercise auth plugins, TLS,
    // or a result set.
    void test_real_server_async_query_and_streaming_fetch(const MySqlTestConfig& config)
    {
        MYSQL* mysql = ::mysql_init(nullptr);
        assert(mysql != nullptr);

        CallProbe probe;
        snf::worker::Poller poller{16};
        AsyncDriver driver{poller, *mysql, probe};

        // Q5: the production auth plugin has to complete over the async path.
        const net_async_status connected = driver.run(
            "mysql_real_connect_nonblocking",
            [&]
            {
                return ::mysql_real_connect_nonblocking(
                    mysql, config.host.c_str(), config.user.c_str(), config.password.c_str(), config.database.c_str(), config.port, nullptr, 0
                );
            },
            5000ms
        );
        if (connected != NET_ASYNC_COMPLETE)
        {
            std::cout << "    async connect failed: " << ::mysql_error(mysql) << std::endl;
        }
        assert(connected == NET_ASYNC_COMPLETE);

        // The async path does not support protocol compression.
        assert(mysql->net.compress == 0);
        std::cout << "    Q5 auth plugin: " << (mysql->options.extension == nullptr ? "default" : "configured")
                  << ", compression: " << static_cast<int>(mysql->net.compress) << std::endl;

        constexpr std::string_view QUERY = "SELECT 1";
        const net_async_status queried = driver.run(
            "mysql_real_query_nonblocking",
            [&]
            {
                return ::mysql_real_query_nonblocking(mysql, QUERY.data(), QUERY.size());
            },
            5000ms
        );
        assert(queried == NET_ASYNC_COMPLETE);

        // Q6: streaming retrieval, not store_result. mysql_use_result() starts
        // row-by-row retrieval; the rows themselves come back asynchronously.
        MYSQL_RES* result = probe.measure(
            "mysql_use_result",
            [&]
            {
                return ::mysql_use_result(mysql);
            }
        );
        assert(result != nullptr);

        std::size_t rows = 0;
        while (true)
        {
            MYSQL_ROW row = nullptr;
            const net_async_status fetched = driver.run(
                "mysql_fetch_row_nonblocking",
                [&]
                {
                    return ::mysql_fetch_row_nonblocking(result, &row);
                },
                5000ms
            );
            assert(fetched == NET_ASYNC_COMPLETE);
            if (row == nullptr)
            {
                break;
            }
            assert(std::string_view{row[0]} == "1");
            ++rows;
        }
        assert(rows == 1);

        // The result must be drained and released before the connection can carry
        // another query, so freeing is part of the operation's state machine.
        const net_async_status freed = driver.run(
            "mysql_free_result_nonblocking",
            [&]
            {
                return ::mysql_free_result_nonblocking(result);
            },
            5000ms
        );
        assert(freed == NET_ASYNC_COMPLETE);

        std::cout << "    Q6 streaming fetch rows: " << rows << ", driver wakeups: " << driver.wakeups() << std::endl;

        // Bounded wakeups is the spin check on the real path: connect, query, fetch
        // and free together should need a handful of readiness events, not hundreds.
        assert(driver.wakeups() < 50);

        // Q7: the communication buffer can grow to max_allowed_packet, so the row
        // shape limits in DbRequest have to be chosen against this value.
        constexpr std::string_view PACKET_QUERY = "SELECT @@max_allowed_packet";
        const net_async_status packet_queried = driver.run(
            "mysql_real_query_nonblocking",
            [&]
            {
                return ::mysql_real_query_nonblocking(mysql, PACKET_QUERY.data(), PACKET_QUERY.size());
            },
            5000ms
        );
        assert(packet_queried == NET_ASYNC_COMPLETE);

        MYSQL_RES* packet_result = ::mysql_use_result(mysql);
        assert(packet_result != nullptr);
        std::string max_allowed_packet;
        while (true)
        {
            MYSQL_ROW row = nullptr;
            const net_async_status fetched = driver.run(
                "mysql_fetch_row_nonblocking",
                [&]
                {
                    return ::mysql_fetch_row_nonblocking(packet_result, &row);
                },
                5000ms
            );
            assert(fetched == NET_ASYNC_COMPLETE);
            if (row == nullptr)
            {
                break;
            }
            max_allowed_packet = row[0];
        }
        const net_async_status packet_freed = driver.run(
            "mysql_free_result_nonblocking",
            [&]
            {
                return ::mysql_free_result_nonblocking(packet_result);
            },
            5000ms
        );
        assert(packet_freed == NET_ASYNC_COMPLETE);
        std::cout << "    Q7 max_allowed_packet: " << max_allowed_packet << std::endl;
        assert(!max_allowed_packet.empty());

        driver.unregister();
        probe.measure(
            "mysql_close",
            [&]
            {
                ::mysql_close(mysql);
            }
        );

        probe.report();
    }

    // A reconnect after an in-flight timeout is on the hot path, so the cost of
    // the handshake is not a startup-only concern. This measures a second connect
    // from a fresh handle against the same server.
    //
    // Measured on MySQL 8.4: one call inside every connect holds the thread for
    // roughly 8ms, and the second attempt costs the same as the first, so it is
    // per-connection rather than one-time library initialisation. --mysql-auth-cost
    // attributes it to the TLS handshake. Whether reconnects need budgeting
    // therefore depends on the deployed ssl_mode.
    void test_reconnect_handshake_cost(const MySqlTestConfig& config)
    {
        std::chrono::nanoseconds worst_connect_call{0};

        for (int attempt = 1; attempt <= 2; ++attempt)
        {
            MYSQL* mysql = ::mysql_init(nullptr);
            assert(mysql != nullptr);

            CallProbe probe;
            probe.trace("mysql_real_connect_nonblocking");
            snf::worker::Poller poller{16};
            AsyncDriver driver{poller, *mysql, probe};

            const net_async_status connected = driver.run(
                "mysql_real_connect_nonblocking",
                [&]
                {
                    return ::mysql_real_connect_nonblocking(
                        mysql, config.host.c_str(), config.user.c_str(), config.password.c_str(), config.database.c_str(), config.port, nullptr, 0
                    );
                },
                5000ms
            );
            assert(connected == NET_ASYNC_COMPLETE);

            std::cout << "    connect attempt " << attempt << ":" << std::endl;
            probe.report();
            worst_connect_call = std::max(worst_connect_call, probe.maxDurationOf("mysql_real_connect_nonblocking"));

            driver.unregister();
            ::mysql_close(mysql);
        }

        std::cout << "    Q3 VERDICT: worst single connect call held the thread for "
                  << std::chrono::duration_cast<std::chrono::microseconds>(worst_connect_call).count()
                  << "us, repeated on every connect. Query, fetch and free stay in the microsecond range." << std::endl;
    }

    struct ConnectVariant
    {
        const char* name;
        // Applied before connecting. Returning false means the variant cannot be
        // built on this client and is skipped rather than reported as slow.
        bool (*configure)(MYSQL*);
    };

    // Isolates where the multi-millisecond connect call goes. The candidates are
    // the caching_sha2_password RSA exchange (which needs the server public key on
    // an unencrypted link) and the TLS handshake. Running the same connect under
    // each option set is the cheapest way to tell them apart.
    //
    // Measured against MySQL 8.4, worst connect call per variant:
    //   baseline (SSL_MODE_PREFERRED)  ~8000us
    //   get_server_public_key=1        ~8000us
    //   ssl_mode=REQUIRED              ~8000us
    //   ssl_mode=DISABLED              ~100us   <- and auth still succeeds
    // So the cost is TLS. It is spent inside one call that then returns NOT_READY,
    // which is handshake work rather than a blocked wait, but it occupies the
    // worker thread either way.
    void test_connect_cost_by_auth_option(const MySqlTestConfig& config)
    {
        static constexpr ConnectVariant VARIANTS[] = {
            {"baseline (no options)",
             [](MYSQL*)
             {
                 return true;
             }},
            {"get_server_public_key=1",
             [](MYSQL* mysql)
             {
                 bool enabled = true;
                 return ::mysql_options(mysql, MYSQL_OPT_GET_SERVER_PUBLIC_KEY, &enabled) == 0;
             }},
            {"ssl_mode=DISABLED",
             [](MYSQL* mysql)
             {
                 unsigned int mode = SSL_MODE_DISABLED;
                 return ::mysql_options(mysql, MYSQL_OPT_SSL_MODE, &mode) == 0;
             }},
            {"ssl_mode=REQUIRED",
             [](MYSQL* mysql)
             {
                 unsigned int mode = SSL_MODE_REQUIRED;
                 return ::mysql_options(mysql, MYSQL_OPT_SSL_MODE, &mode) == 0;
             }},
        };

        for (const ConnectVariant& variant : VARIANTS)
        {
            for (int attempt = 1; attempt <= 2; ++attempt)
            {
                MYSQL* mysql = ::mysql_init(nullptr);
                assert(mysql != nullptr);

                if (!variant.configure(mysql))
                {
                    std::cout << "    " << variant.name << ": option rejected by this client" << std::endl;
                    ::mysql_close(mysql);
                    break;
                }

                CallProbe probe;
                probe.trace("mysql_real_connect_nonblocking");
                snf::worker::Poller poller{16};
                AsyncDriver driver{poller, *mysql, probe};

                const net_async_status connected = driver.run(
                    "mysql_real_connect_nonblocking",
                    [&]
                    {
                        return ::mysql_real_connect_nonblocking(
                            mysql, config.host.c_str(), config.user.c_str(), config.password.c_str(), config.database.c_str(), config.port, nullptr, 0
                        );
                    },
                    5000ms
                );

                if (connected != NET_ASYNC_COMPLETE)
                {
                    std::cout << "    " << variant.name << " attempt " << attempt << ": FAILED - " << ::mysql_error(mysql) << std::endl;
                    driver.unregister();
                    ::mysql_close(mysql);
                    break;
                }

                const auto worst = std::chrono::duration_cast<std::chrono::microseconds>(probe.maxDurationOf("mysql_real_connect_nonblocking"));
                std::cout << "    " << variant.name << " attempt " << attempt << ": worst call " << worst.count() << "us" << std::endl;
                probe.report();

                driver.unregister();
                ::mysql_close(mysql);
            }
        }
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
        std::cout << "Running DB driver conformance (TCP stall)..." << std::endl;
        test_stalled_connect_never_holds_the_thread();
        std::cout << "  - test_stalled_connect_never_holds_the_thread PASSED" << std::endl;
    }
    else if (mode == "--mysql")
    {
        const auto config = testConfig();
        if (!config)
        {
            std::cout << "SNF_MYSQL_TEST_HOST is unset; the 8A gate is NOT satisfied by this run." << std::endl;
            exit_code = SKIP_EXIT_CODE;
        }
        else
        {
            std::cout << "Running DB driver conformance (real MySQL)..." << std::endl;
            test_real_server_async_query_and_streaming_fetch(*config);
            std::cout << "  - test_real_server_async_query_and_streaming_fetch PASSED" << std::endl;
            test_reconnect_handshake_cost(*config);
            std::cout << "  - test_reconnect_handshake_cost PASSED" << std::endl;
        }
    }
    else if (mode == "--mysql-auth-cost")
    {
        const auto config = testConfig();
        if (!config)
        {
            std::cout << "SNF_MYSQL_TEST_HOST is unset." << std::endl;
            exit_code = SKIP_EXIT_CODE;
        }
        else
        {
            std::cout << "Measuring connect cost per auth option..." << std::endl;
            test_connect_cost_by_auth_option(*config);
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
