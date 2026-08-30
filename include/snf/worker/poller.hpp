#pragma once

#include "snf/net/unique_file_descriptor.hpp"
#include "snf/worker/poll_token.hpp"

#include <chrono>
#include <cstddef>
#include <optional>
#include <span>
#include <sys/epoll.h>
#include <vector>

namespace snf::worker
{
    struct PollInterest
    {
        bool read{false};
        bool write{false};

        [[nodiscard]] bool operator==(const PollInterest&) const noexcept = default;
    };

    struct PollEvent
    {
        PollToken token;
        bool readable{false};
        bool writable{false};
        bool error{false};       // EPOLLERR | EPOLLHUP | EPOLLRDHUP (legacy aggregate)
        bool hangup{false};      // EPOLLHUP | EPOLLRDHUP
        bool fatal_error{false}; // EPOLLERR
    };

    // Poller는 owner Worker thread 전용이다.
    // add/modify/remove 실패 시 throw_system_error를 던진다.
    // wait()의 epoll_wait 실패는 EINTR이면 빈 span, 그 외에는 throw.
    // 반환된 span은 다음 wait() 호출 전까지만 유효하다.
    class Poller
    {
    public:
        explicit Poller(std::size_t max_events_per_wait);

        Poller(const Poller&) = delete;
        Poller& operator=(const Poller&) = delete;

        void add(int descriptor, PollToken token, PollInterest interest);
        void modify(int descriptor, PollToken token, PollInterest interest);
        void remove(int descriptor);

        // timeout이 nullopt면 무한 대기. EINTR은 빈 결과로 반환한다(throw하지 않는다).
        [[nodiscard]] std::span<const PollEvent> wait(std::optional<std::chrono::milliseconds> timeout);

    private:
        snf::net::UniqueFileDescriptor _epoll_fd;
        std::vector<epoll_event> _epoll_events;
        std::vector<PollEvent> _events;
    };
}
