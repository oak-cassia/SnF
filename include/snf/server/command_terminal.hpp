#pragma once

#include "snf/net/connection_id.hpp"

#include <atomic>
#include <cstdint>

namespace snf::server
{
    class CommandLifecycleSink
    {
    public:
        virtual ~CommandLifecycleSink() = default;

        virtual void onCommandReleased(snf::net::ConnectionId connection) noexcept = 0;

        virtual void onCommandAdmissionRejected(snf::net::ConnectionId connection) noexcept = 0;
    };

    class CommandReleaseToken final
    {
    public:
        CommandReleaseToken() noexcept = default;
        CommandReleaseToken(CommandLifecycleSink& sink, snf::net::ConnectionId connection) noexcept;
        ~CommandReleaseToken();

        CommandReleaseToken(const CommandReleaseToken&) = delete;
        CommandReleaseToken& operator=(const CommandReleaseToken&) = delete;
        CommandReleaseToken(CommandReleaseToken&& other) noexcept;
        CommandReleaseToken& operator=(CommandReleaseToken&& other) noexcept;

        [[nodiscard]] bool armed() const noexcept;

    private:
        void release() noexcept;

        CommandLifecycleSink* _sink{nullptr};
        snf::net::ConnectionId _connection{};
    };

    class CountingCommandLifecycleSink final : public CommandLifecycleSink
    {
    public:
        void onCommandReleased(snf::net::ConnectionId connection) noexcept override;
        void onCommandAdmissionRejected(snf::net::ConnectionId connection) noexcept override;

        [[nodiscard]] std::uint64_t releaseCount() const noexcept;
        [[nodiscard]] std::uint64_t admissionRejectionCount() const noexcept;
        [[nodiscard]] std::uint64_t terminalCount() const noexcept;

    private:
        std::atomic<std::uint64_t> _releases{0};
        std::atomic<std::uint64_t> _admission_rejections{0};
        std::atomic<std::uint64_t> _terminals{0};
    };
}
