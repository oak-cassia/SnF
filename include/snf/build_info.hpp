#pragma once

#include <string_view>

namespace snf
{
    [[nodiscard]] std::string_view project_name() noexcept;
    [[nodiscard]] std::string_view project_version() noexcept;
}
