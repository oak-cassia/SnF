#pragma once

#include "snf/net/unique_file_descriptor.hpp"

#include <cstdint>

namespace snf::net
{
    [[nodiscard]] UniqueFileDescriptor create_tcp_listener(std::uint16_t port);
}
