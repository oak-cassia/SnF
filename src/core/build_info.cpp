#include "snf/build_info.hpp"

namespace snf
{
    std::string_view project_name() noexcept
    {
        return "SnF";
    }

    std::string_view project_version() noexcept
    {
        return "0.1.0";
    }
}
