#pragma once

#include <atomic>
#include <cstdint>

namespace snf::runtime
{
    // Phase 3.8 has one logic scheduler. More ids may be added only when a
    // separate runtime lifecycle is introduced.
    enum class RuntimeId : std::uint8_t
    {
        Logic = 0,
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

        // These are authoritative level-triggered states. The eventfd wake-up
        // is only a hint, so coalesced wakes cannot lose completion facts.
        [[nodiscard]] virtual bool allRequiredRuntimesDrained() const noexcept = 0;
        [[nodiscard]] virtual bool anyRuntimeFailed() const noexcept = 0;
    };

    // Completion is intentionally independent from data-plane backpressure.
    // The mask constructor is retained to make the coordinator explicit, but
    // Phase 3.8 accepts the Logic runtime only.
    class RuntimeCompletionCoordinator final : public RuntimeCompletionSink, public RuntimeCompletionSource
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
