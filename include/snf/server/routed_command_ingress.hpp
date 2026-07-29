#pragma once

#include "snf/server/post_result.hpp"
#include "snf/server/routed_command.hpp"

namespace snf::server
{
    class RoutedCommandIngress
    {
    public:
        virtual ~RoutedCommandIngress() = default;

        [[nodiscard]] virtual PostResult tryPost(RoutedCommand command) = 0;
        virtual void close() noexcept = 0;
        virtual void cancel() noexcept = 0;
    };
}
