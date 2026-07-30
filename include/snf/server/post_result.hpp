#pragma once

#include "snf/runtime/post_result.hpp"

namespace snf::server
{
    // Server protocol boundaries retain this spelling while the generic
    // scheduler owns the result type.
    using PostResult = snf::runtime::PostResult;
}
