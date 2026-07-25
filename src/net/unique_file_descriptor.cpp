#include "snf/net/unique_file_descriptor.hpp"

#include <unistd.h>
#include <utility>

namespace snf::net
{
    UniqueFileDescriptor::UniqueFileDescriptor(int file_descriptor) noexcept
        : _fd(file_descriptor)
    {
    }

    UniqueFileDescriptor::~UniqueFileDescriptor()
    {
        init();
    }

    UniqueFileDescriptor::UniqueFileDescriptor(UniqueFileDescriptor&& other) noexcept
        : _fd(other.release())
    {
    }

    UniqueFileDescriptor& UniqueFileDescriptor::operator=(UniqueFileDescriptor&& other) noexcept
    {
        if (this != &other)
        {
            init(other.release());
        }

        return *this;
    }

    int UniqueFileDescriptor::getDescriptor() const noexcept
    {
        return _fd;
    }

    bool UniqueFileDescriptor::isValid() const noexcept
    {
        return _fd != INVALID_FD;
    }

    int UniqueFileDescriptor::release() noexcept
    {
        return std::exchange(_fd, INVALID_FD);
    }

    void UniqueFileDescriptor::init(int fd) noexcept
    {
        if (_fd != fd)
        {
            if (isValid())
            {
                close(_fd);
            }

            _fd = fd;
        }
    }
}
