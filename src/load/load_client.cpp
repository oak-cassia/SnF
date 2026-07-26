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
#include <sys/epoll.h>
#include <utility>

namespace
{
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
}

namespace snf::load
{
    LoadClient::LoadClient(LoadClientConfig config)
        : _config(std::move(config))
    {
    }

    LoadClientResult LoadClient::run() const
    {
        try
        {
            const auto epoll = create_epoll_instance();
            ClientConnection connection{_config.host, _config.port};
            update_epoll_events(epoll.getDescriptor(), connection, EPOLL_CTL_ADD);

            auto deadline =
                std::chrono::steady_clock::now() +
                (connection.isConnecting() ? _config.connect_timeout : _config.request_timeout);

            std::array<epoll_event, 1> events{};

            // PONG을 검증하거나 connect/request timeout 또는 I/O 오류가 발생할 때까지 실행한다.
            while (!connection.isComplete())
            {
                const int wait_timeout = get_wait_timeout(deadline);
                if (wait_timeout == 0)
                {
                    return LoadClientResult{
                        .error = connection.isConnecting() ? "Connect timeout" : "Request timeout",
                    };
                }

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

                if (ready_event_count == 0)
                {
                    return LoadClientResult{
                        .error = connection.isConnecting() ? "Connect timeout" : "Request timeout",
                    };
                }

                const std::uint32_t event_flags = events.front().events;
                const bool was_connecting = connection.isConnecting();
                std::optional<std::string> error;

                if ((event_flags & EPOLLOUT) != 0)
                {
                    error = connection.handleWritable();
                }

                if (!error && (event_flags & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0)
                {
                    error = connection.handleReadable();
                }

                if (!error && !connection.isComplete() && (event_flags & EPOLLERR) != 0)
                {
                    error = connection.getSocketError();
                    if (!error)
                    {
                        error = "Socket reported EPOLLERR";
                    }
                }

                if (error)
                {
                    return LoadClientResult{.error = std::move(*error)};
                }

                if (was_connecting && !connection.isConnecting())
                {
                    deadline = std::chrono::steady_clock::now() + _config.request_timeout;
                }

                if (!connection.isComplete())
                {
                    update_epoll_events(epoll.getDescriptor(), connection, EPOLL_CTL_MOD);
                }
            }

            return LoadClientResult{
                .success = true,
                .error = {},
                .round_trip_time = connection.getRoundTripTime(),
            };
        }
        catch (const std::exception& error)
        {
            return LoadClientResult{.error = error.what()};
        }
    }
}
