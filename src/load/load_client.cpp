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
#include <unordered_map>
#include <utility>
#include <vector>

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
            .round_trip_times = {},
        };

        try
        {
            if (_config.connections == 0 ||
                _config.connections > std::numeric_limits<std::uint32_t>::max())
            {
                return LoadClientResult{
                    .success = false,
                    .error = "Connection count is out of range",
                    .requested_connections = _config.connections,
                    .successful_connections = 0,
                    .failed_connections = _config.connections,
                    .round_trip_times = {},
                };
            }

            const auto epoll = create_epoll_instance();
            std::unordered_map<int, ClientConnection> connections;
            connections.reserve(_config.connections);
            result.round_trip_times.reserve(_config.connections);

            for (std::size_t connection_index = 0; connection_index < _config.connections;
                 ++connection_index)
            {
                try
                {
                    ClientConnection connection{
                        _config.host,
                        _config.port,
                        static_cast<std::uint32_t>(connection_index + 1),
                        _config.connect_timeout,
                        _config.request_timeout,
                    };

                    const int connection_descriptor = connection.getDescriptor();
                    update_epoll_events(epoll.getDescriptor(), connection, EPOLL_CTL_ADD);

                    const bool inserted =
                        connections.emplace(connection_descriptor, std::move(connection)).second;

                    if (!inserted)
                    {
                        throw std::logic_error{"Duplicate client descriptor"};
                    }
                }
                catch (const std::exception& error)
                {
                    ++result.failed_connections;
                    remember_first_error(result.error, error.what());
                }
            }

            std::array<epoll_event, MAX_READY_EVENTS> events{};

            // 모든 연결이 PONG을 검증하거나 timeout 또는 I/O 오류로 종료될 때까지 실행한다.
            while (!connections.empty())
            {
                auto earliest_deadline = std::chrono::steady_clock::time_point::max();
                for (const auto& connection_entry : connections)
                {
                    earliest_deadline =
                        std::min(earliest_deadline, connection_entry.second.getDeadline());
                }

                const int wait_timeout = get_wait_timeout(earliest_deadline);
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

                    snf::net::throw_system_error("epoll_wait");
                }

                for (int event_index = 0; event_index < ready_event_count; ++event_index)
                {
                    const epoll_event& event = events[event_index];
                    const auto connection_iterator = connections.find(event.data.fd);
                    if (connection_iterator == connections.end())
                    {
                        continue;
                    }

                    ClientConnection& connection = connection_iterator->second;
                    std::optional<std::string> connection_error;

                    if ((event.events & EPOLLOUT) != 0)
                    {
                        connection_error = connection.handleWritable();
                    }

                    if (!connection_error &&
                        (event.events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0)
                    {
                        connection_error = connection.handleReadable();
                    }

                    if (!connection_error && !connection.isComplete() &&
                        (event.events & EPOLLERR) != 0)
                    {
                        connection_error = connection.getSocketError();
                        if (!connection_error)
                        {
                            connection_error = "Socket reported EPOLLERR";
                        }
                    }

                    if (connection.isComplete())
                    {
                        result.round_trip_times.push_back(connection.getRoundTripTime());
                        ++result.successful_connections;
                        remove_epoll_events(epoll.getDescriptor(), event.data.fd);
                        connections.erase(connection_iterator);
                    }
                    else if (connection_error)
                    {
                        ++result.failed_connections;
                        remember_first_error(result.error, *connection_error);
                        remove_epoll_events(epoll.getDescriptor(), event.data.fd);
                        connections.erase(connection_iterator);
                    }
                    else
                    {
                        update_epoll_events(epoll.getDescriptor(), connection, EPOLL_CTL_MOD);
                    }
                }

                const auto now = std::chrono::steady_clock::now();
                for (auto connection_iterator = connections.begin();
                     connection_iterator != connections.end();)
                {
                    if (now < connection_iterator->second.getDeadline())
                    {
                        ++connection_iterator;
                        continue;
                    }

                    ++result.failed_connections;
                    remember_first_error(result.error,
                                         connection_iterator->second.getTimeoutError());
                    remove_epoll_events(epoll.getDescriptor(), connection_iterator->first);
                    connection_iterator = connections.erase(connection_iterator);
                }
            }

            result.success = result.successful_connections == result.requested_connections &&
                             result.failed_connections == 0;
            return result;
        }
        catch (const std::exception& error)
        {
            remember_first_error(result.error, error.what());
            result.failed_connections =
                result.requested_connections - result.successful_connections;
            return result;
        }
    }
}
