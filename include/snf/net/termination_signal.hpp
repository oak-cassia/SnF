#pragma once

#include "snf/net/unique_file_descriptor.hpp"

namespace snf::net
{
    [[nodiscard]] UniqueFileDescriptor create_termination_signal_listener();
}
