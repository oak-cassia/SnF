#include "snf/runtime/runtime_completion.hpp"

#include <cerrno>
#include <stdexcept>
#include <unistd.h>

namespace snf::runtime
{
    RuntimeCompletionCoordinator::RuntimeCompletionCoordinator(const std::uint64_t required_runtime_mask, const int wake_descriptor)
        : _required_runtime_mask(required_runtime_mask)
        , _wake_descriptor(wake_descriptor)
    {
        if (_required_runtime_mask != runtimeMask(RuntimeId::Logic) || _wake_descriptor < 0)
        {
            throw std::invalid_argument{"Invalid logic runtime completion configuration"};
        }
    }

    void RuntimeCompletionCoordinator::notifyDrained(const RuntimeId runtime) noexcept
    {
        _drained_runtime_mask.fetch_or(runtimeMask(runtime), std::memory_order_release);
        signal();
    }

    void RuntimeCompletionCoordinator::notifyFailed(const RuntimeId runtime) noexcept
    {
        _failed_runtime_mask.fetch_or(runtimeMask(runtime), std::memory_order_release);
        signal();
    }

    bool RuntimeCompletionCoordinator::allRequiredRuntimesDrained() const noexcept
    {
        const std::uint64_t drained = _drained_runtime_mask.load(std::memory_order_acquire);
        return (drained & _required_runtime_mask) == _required_runtime_mask;
    }

    bool RuntimeCompletionCoordinator::anyRuntimeFailed() const noexcept
    {
        return _failed_runtime_mask.load(std::memory_order_acquire) != 0;
    }

    void RuntimeCompletionCoordinator::signal() const noexcept
    {
        constexpr std::uint64_t wakeup_value = 1;

        while (::write(_wake_descriptor, &wakeup_value, sizeof(wakeup_value)) == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return;
        }
    }
}
