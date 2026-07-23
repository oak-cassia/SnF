#include "snf/build_info.hpp"

#include <iostream>

int main() {
    std::cout << snf::project_name() << " server scaffold v"
              << snf::project_version() << '\n';
    return 0;
}
