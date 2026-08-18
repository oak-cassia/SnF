#include "snf/load/load_client.hpp"

#include "snf/load/client_connection.hpp"
#include "snf/net/system_error.hpp"
#include "snf/net/unique_file_descriptor.hpp"

#include <algorithm>
#include <array>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <stdexcept>
#include <sys/epoll.h>
#include <sys/timerfd.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>

namespace
{
    constexpr std::size_t MAX_READY_EVENTS = 256;

    snf::net::UniqueFileDescriptor create_epoll_instance()
    {
        const int epoll_descriptor = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_descriptor == -1)
        {
            snf::net::throw_system_error("epoll_create1");
        }

        return snf::net::UniqueFileDescriptor{epoll_descriptor};
    }

    snf::net::UniqueFileDescriptor create_request_timer()
    {
        const int timer_descriptor = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (timer_descriptor == -1)
        {
            snf::net::throw_system_error("timerfd_create");
        }

        return snf::net::UniqueFileDescriptor{timer_descriptor};
    }

    void arm_request_timer(const int timer_descriptor, const std::size_t requests_per_second)
    {
        constexpr std::int64_t NANOSECONDS_PER_SECOND = 1'000'000'000;
        const auto interval_nanoseconds = static_cast<long>(
            NANOSECONDS_PER_SECOND / static_cast<std::int64_t>(requests_per_second));

        itimerspec timer_settings{};
        timer_settings.it_value.tv_nsec = 1;
        timer_settings.it_interval.tv_sec = interval_nanoseconds / NANOSECONDS_PER_SECOND;
        timer_settings.it_interval.tv_nsec = interval_nanoseconds % NANOSECONDS_PER_SECOND;

        if (::timerfd_settime(timer_descriptor, 0, &timer_settings, nullptr) == -1)
        {
            snf::net::throw_system_error("timerfd_settime(arm)");
        }
    }

    void disarm_request_timer(const int timer_descriptor)
    {
        const itimerspec timer_settings{};
        if (::timerfd_settime(timer_descriptor, 0, &timer_settings, nullptr) == -1)
        {
            snf::net::throw_system_error("timerfd_settime(disarm)");
        }
    }

    void consume_timer_expirations(const int timer_descriptor)
    {
        std::uint64_t expiration_count = 0;

        while (::read(timer_descriptor, &expiration_count, sizeof(expiration_count)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (errno != EAGAIN && errno != EWOULDBLOCK)
            {
                snf::net::throw_system_error("read(timerfd)");
            }

            return;
        }
    }

    int get_wait_timeout(const std::chrono::steady_clock::time_point deadline)
    {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
        {
            return 0;
        }

        const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(deadline - now);
        return static_cast<int>(
            std::min<std::int64_t>(remaining.count(), std::numeric_limits<int>::max()));
    }

    void update_epoll_events(const int epoll_descriptor,
                             const snf::load::ClientConnection& connection,
                             const int operation)
    {
        epoll_event event{};
        event.events = connection.getDesiredEvents();
        event.data.fd = connection.getDescriptor();

        if (::epoll_ctl(epoll_descriptor, operation, connection.getDescriptor(), &event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(client)");
        }
    }

    void register_timer(const int epoll_descriptor, const int timer_descriptor)
    {
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = timer_descriptor;

        if (::epoll_ctl(epoll_descriptor, EPOLL_CTL_ADD, timer_descriptor, &event) == -1)
        {
            snf::net::throw_system_error("epoll_ctl(timerfd)");
        }
    }

    void remove_epoll_events(const int epoll_descriptor, const int connection_descriptor)
    {
        if (::epoll_ctl(epoll_descriptor, EPOLL_CTL_DEL, connection_descriptor, nullptr) == -1 &&
            errno != ENOENT && errno != EBADF)
        {
            snf::net::throw_system_error("epoll_ctl(EPOLL_CTL_DEL client)");
        }
    }

    void remember_first_error(std::string& first_error, const std::string& error)
    {
        if (first_error.empty())
        {
            first_error = error;
        }
    }

    std::chrono::steady_clock::time_point earliest_connection_deadline(
        const std::unordered_map<int, snf::load::ClientConnection>& connections)
    {
        auto earliest_deadline = std::chrono::steady_clock::time_point::max();

        for (const auto& connection_entry : connections)
        {
            earliest_deadline = std::min(earliest_deadline, connection_entry.second.getDeadline());
        }

        return earliest_deadline;
    }

    bool has_connecting_connection(
        const std::unordered_map<int, snf::load::ClientConnection>& connections)
    {
        return std::ranges::any_of(connections,
                                   [](const auto& connection_entry)
                                   { return connection_entry.second.isConnecting(); });
    }

    bool
    all_connections_idle(const std::unordered_map<int, snf::load::ClientConnection>& connections)
    {
        return std::ranges::all_of(connections,
                                   [](const auto& connection_entry)
                                   { return connection_entry.second.isIdle(); });
    }

    void record_runtime_error(snf::load::LoadClientResult& result,
                              const snf::load::ClientError& error)
    {
        remember_first_error(result.error, error.message);

        if (error.kind == snf::load::ClientErrorKind::Protocol)
        {
            ++result.invalid_responses;
        }
        else
        {
            ++result.socket_errors;
        }
    }
}

namespace snf::load
{
    LoadClient::LoadClient(LoadClientConfig config)
        : _config(std::move(config))
    {
    }

    LoadClientResult LoadClient::run() const
    {
        LoadClientResult result{
            .success = false,
            .error = {},
            .requested_connections = _config.connections,
            .successful_connections = 0,
            .failed_connections = 0,
            .maximum_active_connections = 0,
            .sent_requests = 0,
            .received_responses = 0,
            .sent_bootstrap_requests = 0,
            .received_bootstrap_responses = 0,
            .sent_gameplay_requests = 0,
            .received_gameplay_responses = 0,
            .request_timeouts = 0,
            .invalid_responses = 0,
            .socket_errors = 0,
            .load_duration = _config.duration,
            .round_trip_times = {},
            .gameplay_round_trip_times = {},
        };

        try
        {
            constexpr std::size_t MAX_CONNECTION_COUNT = 100'000;
            constexpr std::size_t MAX_REQUESTS_PER_SECOND = 1'000'000;

            if (_config.connections == 0 || _config.connections > MAX_CONNECTION_COUNT)
            {
                result.error = "Connection count is out of range";
                result.failed_connections = _config.connections;
                return result;
            }

            if (_config.requests_per_second == 0 ||
                _config.requests_per_second > MAX_REQUESTS_PER_SECOND)
            {
                result.error = "Requests per second is out of range";
                return result;
            }
            if (_config.players_per_zone == 0)
            {
                result.error = "Players per Zone must be positive";
                return result;
            }
            if (_config.scenario != LoadScenario::Ping && _config.scenario != LoadScenario::Zone)
            {
                result.error = "Load scenario is invalid";
                return result;
            }

            if (_config.duration <= std::chrono::milliseconds::zero() ||
                _config.connect_timeout <= std::chrono::milliseconds::zero() ||
                _config.request_timeout <= std::chrono::milliseconds::zero())
            {
                result.error = "Duration and timeout values must be positive";
                return result;
            }

            const auto epoll = create_epoll_instance();
            std::unordered_map<int, ClientConnection> connections;
            connections.reserve(_config.connections);
            for (std::size_t connection_index = 0; connection_index < _config.connections;
                 ++connection_index)
            {
                try
                {
                    const std::uint64_t player_id = connection_index + 1;
                    const std::uint64_t zone_id = connection_index / _config.players_per_zone + 1;
                    ClientConnection connection{
                        _config.host,
                        _config.port,
                        _config.connect_timeout,
                        ClientWorkload{
                            .scenario = _config.scenario,
                            .player_id = player_id,
                            .zone_id = zone_id,
                        },
                    };

                    const int connection_descriptor = connection.getDescriptor();
                    const bool connected_immediately = connection.isConnected();
                    update_epoll_events(epoll.getDescriptor(), connection, EPOLL_CTL_ADD);

                    const bool inserted =
                        connections.emplace(connection_descriptor, std::move(connection)).second;
                    if (!inserted)
                    {
                        throw std::logic_error{"Duplicate client descriptor"};
                    }

                    if (connected_immediately)
                    {
                        ++result.successful_connections;
                    }
                }
                catch (const std::exception& error)
                {
                    ++result.failed_connections;
                    remember_first_error(result.error, error.what());
                }
            }

            std::array<epoll_event, MAX_READY_EVENTS> events{};

            // 모든 non-blocking connect가 성공, 실패 또는 timeout으로 끝날 때까지 처리한다.
            while (has_connecting_connection(connections))
            {
                const int wait_timeout =
                    get_wait_timeout(earliest_connection_deadline(connections));
                const int ready_event_count = ::epoll_wait(epoll.getDescriptor(),
                                                           events.data(),
                                                           static_cast<int>(events.size()),
                                                           wait_timeout);

                if (ready_event_count == -1)
                {
                    if (errno == EINTR)
                    {
                        continue;
                    }

                    snf::net::throw_system_error("epoll_wait(connect)");
                }

                for (int event_index = 0; event_index < ready_event_count; ++event_index)
                {
                    const epoll_event& event = events[event_index];
                    const auto connection_iterator = connections.find(event.data.fd);
                    if (connection_iterator == connections.end() ||
                        !connection_iterator->second.isConnecting())
                    {
                        continue;
                    }

                    ClientConnection& connection = connection_iterator->second;
                    std::optional<ClientError> connection_error;
                    bool connected = false;

                    if ((event.events & EPOLLOUT) != 0)
                    {
                        auto write_result = connection.handleWritable();
                        connected = write_result.connected;
                        connection_error = std::move(write_result.error);
                    }

                    if (!connection_error &&
                        (event.events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0)
                    {
                        auto read_result = connection.handleReadable();
                        connection_error = std::move(read_result.error);
                    }

                    if (!connection_error && connection.isConnecting() &&
                        (event.events & EPOLLERR) != 0)
                    {
                        connection_error = connection.getSocketError();
                        if (!connection_error)
                        {
                            connection_error = ClientError{
                                .kind = ClientErrorKind::Socket,
                                .message = "Socket reported EPOLLERR while connecting",
                            };
                        }
                    }

                    if (connection_error)
                    {
                        ++result.failed_connections;
                        remember_first_error(result.error, connection_error->message);
                        remove_epoll_events(epoll.getDescriptor(), event.data.fd);
                        connections.erase(connection_iterator);
                    }
                    else
                    {
                        if (connected)
                        {
                            ++result.successful_connections;
                        }

                        update_epoll_events(epoll.getDescriptor(), connection, EPOLL_CTL_MOD);
                    }
                }

                const auto now = std::chrono::steady_clock::now();
                for (auto connection_iterator = connections.begin();
                     connection_iterator != connections.end();)
                {
                    if (!connection_iterator->second.isConnecting() ||
                        now < connection_iterator->second.getDeadline())
                    {
                        ++connection_iterator;
                        continue;
                    }

                    ++result.failed_connections;
                    remember_first_error(result.error, "Connect timeout");
                    remove_epoll_events(epoll.getDescriptor(), connection_iterator->first);
                    connection_iterator = connections.erase(connection_iterator);
                }
            }

            result.maximum_active_connections = connections.size();
            if (connections.empty())
            {
                return result;
            }

            const auto request_timer = create_request_timer();
            register_timer(epoll.getDescriptor(), request_timer.getDescriptor());
            arm_request_timer(request_timer.getDescriptor(), _config.requests_per_second);

            const auto load_started_at = std::chrono::steady_clock::now();
            const auto load_ends_at = load_started_at + _config.duration;
            bool generation_complete = false;

            // duration 동안 요청을 만들고, 종료 뒤에는 이미 시작한 요청만 drain한다.
            while (!connections.empty())
            {
                const auto now = std::chrono::steady_clock::now();
                if (!generation_complete && now >= load_ends_at)
                {
                    generation_complete = true;
                    disarm_request_timer(request_timer.getDescriptor());
                }

                if (generation_complete && all_connections_idle(connections))
                {
                    break;
                }

                const auto wait_deadline =
                    generation_complete
                        ? earliest_connection_deadline(connections)
                        : std::min(load_ends_at, earliest_connection_deadline(connections));
                const int ready_event_count = ::epoll_wait(epoll.getDescriptor(),
                                                           events.data(),
                                                           static_cast<int>(events.size()),
                                                           get_wait_timeout(wait_deadline));

                if (ready_event_count == -1)
                {
                    if (errno == EINTR)
                    {
                        continue;
                    }

                    snf::net::throw_system_error("epoll_wait(load)");
                }

                for (int event_index = 0; event_index < ready_event_count; ++event_index)
                {
                    const epoll_event& event = events[event_index];

                    if (event.data.fd == request_timer.getDescriptor())
                    {
                        consume_timer_expirations(request_timer.getDescriptor());

                        if (!generation_complete && std::chrono::steady_clock::now() < load_ends_at)
                        {
                            for (auto& connection_entry : connections)
                            {
                                ClientConnection& connection = connection_entry.second;
                                if (!connection.canStartRequest())
                                {
                                    continue;
                                }

                                connection.enqueueNextRequest(_config.request_timeout);
                                update_epoll_events(
                                    epoll.getDescriptor(), connection, EPOLL_CTL_MOD);
                            }
                        }

                        continue;
                    }

                    const auto connection_iterator = connections.find(event.data.fd);
                    if (connection_iterator == connections.end())
                    {
                        continue;
                    }

                    ClientConnection& connection = connection_iterator->second;
                    std::optional<ClientError> connection_error;

                    if ((event.events & EPOLLOUT) != 0)
                    {
                        auto write_result = connection.handleWritable();
                        result.sent_requests += write_result.sent_requests;
                        result.sent_bootstrap_requests += write_result.sent_bootstrap_requests;
                        result.sent_gameplay_requests += write_result.sent_gameplay_requests;
                        connection_error = std::move(write_result.error);
                    }

                    if (!connection_error &&
                        (event.events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0)
                    {
                        auto read_result = connection.handleReadable();
                        result.received_responses += read_result.round_trip_times.size();
                        result.received_bootstrap_responses += read_result.bootstrap_responses;
                        result.received_gameplay_responses += read_result.gameplay_responses;
                        result.round_trip_times.insert(result.round_trip_times.end(),
                                                       read_result.round_trip_times.begin(),
                                                       read_result.round_trip_times.end());
                        result.gameplay_round_trip_times.insert(
                            result.gameplay_round_trip_times.end(),
                            read_result.gameplay_round_trip_times.begin(),
                            read_result.gameplay_round_trip_times.end());
                        connection_error = std::move(read_result.error);
                    }

                    if (!connection_error && (event.events & EPOLLERR) != 0)
                    {
                        connection_error = connection.getSocketError();
                        if (!connection_error)
                        {
                            connection_error = ClientError{
                                .kind = ClientErrorKind::Socket,
                                .message = "Socket reported EPOLLERR",
                            };
                        }
                    }

                    if (connection_error)
                    {
                        record_runtime_error(result, *connection_error);
                        remove_epoll_events(epoll.getDescriptor(), event.data.fd);
                        connections.erase(connection_iterator);
                    }
                    else
                    {
                        update_epoll_events(epoll.getDescriptor(), connection, EPOLL_CTL_MOD);
                    }
                }

                const auto timeout_check_time = std::chrono::steady_clock::now();
                for (auto connection_iterator = connections.begin();
                     connection_iterator != connections.end();)
                {
                    const auto deadline = connection_iterator->second.getDeadline();
                    if (deadline == std::chrono::steady_clock::time_point::max() ||
                        timeout_check_time < deadline)
                    {
                        ++connection_iterator;
                        continue;
                    }

                    ++result.request_timeouts;
                    remember_first_error(result.error, "Request timeout");
                    remove_epoll_events(epoll.getDescriptor(), connection_iterator->first);
                    connection_iterator = connections.erase(connection_iterator);
                }
            }

            const bool workload_completed =
                _config.scenario == LoadScenario::Ping
                    ? result.received_gameplay_responses > 0
                    : result.received_gameplay_responses > 0 &&
                          std::ranges::all_of(
                              connections,
                              [](const auto& connection_entry)
                              { return connection_entry.second.hasCompletedBootstrap(); });
            result.success = result.successful_connections == result.requested_connections &&
                             result.failed_connections == 0 && result.request_timeouts == 0 &&
                             result.invalid_responses == 0 && result.socket_errors == 0 &&
                             result.sent_requests == result.received_responses &&
                             workload_completed;
            if (!result.success && result.error.empty())
            {
                result.error = "Load workload did not complete";
            }
            return result;
        }
        catch (const std::exception& error)
        {
            remember_first_error(result.error, error.what());
            return result;
        }
    }
}
