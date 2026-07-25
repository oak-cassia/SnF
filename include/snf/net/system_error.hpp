#pragma once

#include <cerrno>
#include <system_error>

namespace snf::net
{
    [[noreturn]] inline void throw_system_error(const char* operation)
    {
        const int error_number = errno;
        throw std::system_error{
            error_number,
            std::generic_category(),
            operation
        };
    }
}
