#include "snf/worker/poller.hpp"
#include "snf/net/system_error.hpp"

#include <cerrno>
#include <sys/epoll.h>

namespace snf::worker
{
    Poller::Poller(const std::size_t max_events_per_wait)
    {
        const int epoll_fd = ::epoll_create1(EPOLL_CLOEXEC);
        if (epoll_fd == -1)
        {
            snf::net::throw_system_error("epoll_create1");
        }
        _epoll_fd.init(epoll_fd);
        _epoll_events.resize(max_events_per_wait);
        _events.reserve(max_events_per_wait);
    }

    void Poller::add(const int descriptor, const PollToken token, const PollInterest interest)
    {
        epoll_event ev{};
        ev.data.u64 = encodePollToken(token.kind, token.index, token.generation);
        if (interest.read)
        {
            ev.events |= EPOLLIN | EPOLLRDHUP;
        }
        if (interest.write)
        {
            ev.events |= EPOLLOUT;
        }

        if (::epoll_ctl(_epoll_fd.getDescriptor(), EPOLL_CTL_ADD, descriptor, &ev) == -1)
        {
            snf::net::throw_system_error("epoll_ctl add");
        }
    }

    void Poller::modify(const int descriptor, const PollToken token, const PollInterest interest)
    {
        epoll_event ev{};
        ev.data.u64 = encodePollToken(token.kind, token.index, token.generation);
        if (interest.read)
        {
            ev.events |= EPOLLIN | EPOLLRDHUP;
        }
        if (interest.write)
        {
            ev.events |= EPOLLOUT;
        }

        if (::epoll_ctl(_epoll_fd.getDescriptor(), EPOLL_CTL_MOD, descriptor, &ev) == -1)
        {
            snf::net::throw_system_error("epoll_ctl mod");
        }
    }

    void Poller::remove(const int descriptor)
    {
        if (::epoll_ctl(_epoll_fd.getDescriptor(), EPOLL_CTL_DEL, descriptor, nullptr) == -1)
        {
            snf::net::throw_system_error("epoll_ctl del");
        }
    }

    std::span<const PollEvent> Poller::wait(const std::optional<std::chrono::milliseconds> timeout)
    {
        const int timeout_ms = timeout.has_value() ? static_cast<int>(timeout->count()) : -1;
        const int ready_count = ::epoll_wait(_epoll_fd.getDescriptor(), _epoll_events.data(), static_cast<int>(_epoll_events.size()), timeout_ms);

        if (ready_count == -1)
        {
            if (errno == EINTR)
            {
                return std::span<const PollEvent>{};
            }
            snf::net::throw_system_error("epoll_wait");
        }

        _events.clear();
        for (int i = 0; i < ready_count; ++i)
        {
            const auto& ev = _epoll_events[i];
            const PollEvent event{
                .token = decodePollToken(ev.data.u64),
                .readable = (ev.events & EPOLLIN) != 0,
                .writable = (ev.events & EPOLLOUT) != 0,
                .error = (ev.events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0,
            };
            _events.push_back(event);
        }

        return std::span<const PollEvent>(_events.data(), _events.size());
    }
}
