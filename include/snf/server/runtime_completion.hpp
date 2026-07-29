#pragma once

#include <atomic>
#include <cstdint>

namespace snf::server
{
    enum class RuntimeId : std::uint8_t
    {
        Player = 0,
        World,
        Battle,
        SharedContent,
    };

    [[nodiscard]] constexpr std::uint64_t runtimeMask(const RuntimeId runtime) noexcept
    {
        return std::uint64_t{1} << static_cast<std::uint8_t>(runtime);
    }

    class RuntimeCompletionSink
    {
    public:
        virtual ~RuntimeCompletionSink() = default;

        virtual void notifyDrained(RuntimeId runtime) noexcept = 0;
        virtual void notifyFailed(RuntimeId runtime) noexcept = 0;
    };

    class RuntimeCompletionSource
    {
    public:
        virtual ~RuntimeCompletionSource() = default;

        // These are authoritative level-triggered states. The eventfd wake-up is
        // only a hint, so a coalesced wake cannot lose completion information.
        [[nodiscard]] virtual bool allRequiredRuntimesDrained() const noexcept = 0;
        [[nodiscard]] virtual bool anyRuntimeFailed() const noexcept = 0;
    };

    // Coordinates lifecycle completion independently from the bounded outbound
    // data queue. Multiple notifications are idempotent and cannot be rejected by
    // data-plane backpressure.
    class RuntimeCompletionCoordinator final : public RuntimeCompletionSink,
                                               public RuntimeCompletionSource
    {
    public:
        RuntimeCompletionCoordinator(std::uint64_t required_runtime_mask, int wake_descriptor);

        void notifyDrained(RuntimeId runtime) noexcept override;
        void notifyFailed(RuntimeId runtime) noexcept override;

        [[nodiscard]] bool allRequiredRuntimesDrained() const noexcept override;
        [[nodiscard]] bool anyRuntimeFailed() const noexcept override;

    private:
        void signal() const noexcept;

        const std::uint64_t _required_runtime_mask;
        int _wake_descriptor;
        std::atomic<std::uint64_t> _drained_runtime_mask{0};
        std::atomic<std::uint64_t> _failed_runtime_mask{0};
    };
}
