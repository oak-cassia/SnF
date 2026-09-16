#include "snf/worker/wakeup.hpp"
#include "snf/net/system_error.hpp"

#include <cerrno>
#include <cstdint>
#include <sys/eventfd.h>
#include <unistd.h>

namespace snf::worker
{
    WakeupHandle::WakeupHandle()
    {
        const int fd = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        if (fd == -1)
        {
            snf::net::throw_system_error("eventfd");
        }
        _fd.init(fd);
    }

    int WakeupHandle::descriptor() const noexcept
    {
        return _fd.getDescriptor();
    }

    void WakeupHandle::notify() noexcept
    {
        const std::uint64_t increment = 1;
        ssize_t bytes_written = ::write(_fd.getDescriptor(), &increment, sizeof(increment));
        if (bytes_written == -1 && errno == EINTR)
        {
            bytes_written = ::write(_fd.getDescriptor(), &increment, sizeof(increment));
        }
        (void)bytes_written;
    }

    void WakeupHandle::consume() noexcept
    {
        std::uint64_t value = 0;
        ssize_t bytes_read = ::read(_fd.getDescriptor(), &value, sizeof(value));
        if (bytes_read == -1 && errno == EINTR)
        {
            bytes_read = ::read(_fd.getDescriptor(), &value, sizeof(value));
        }
        (void)bytes_read;
    }
}
