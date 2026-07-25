#pragma once

#include <cerrno>
#include <fcntl.h>

namespace snf::test
{
    inline bool is_closed(const int file_descriptor)
    {
        errno = 0;
        return ::fcntl(file_descriptor, F_GETFD) == -1 && errno == EBADF;
    }
}
