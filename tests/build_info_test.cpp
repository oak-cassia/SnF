#include "snf/build_info.hpp"

#include <iostream>
#include <string_view>

namespace {

bool expect_equal(std::string_view actual, std::string_view expected,
                  std::string_view label) {
    if (actual == expected) {
        return true;
    }

    std::cerr << label << ": expected '" << expected << "', got '" << actual
              << "'\n";
    return false;
}

}  // namespace

int main() {
    const bool project_name_is_correct =
        expect_equal(snf::project_name(), "SnF", "project name");
    const bool project_version_is_correct =
        expect_equal(snf::project_version(), "0.1.0", "project version");

    return project_name_is_correct && project_version_is_correct ? 0 : 1;
}
