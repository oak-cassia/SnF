#pragma once

#include "snf/net/connection_id.hpp"

#include <atomic>
#include <cstdint>

namespace snf::server
{
    // Two facts about one command, deliberately kept apart.
    //
    // A release is the credit-return signal: the command holds nothing more, whichever
    // way it ended. It fires for every submission the binding created, including one the
    // runtime refused, because a refused command has to give back the credit it took.
    //
    // An admission rejection is the refusal itself. It is a different fact with a
    // different cause -- a full or closed ingress rather than a command that ran -- so it
    // is reported separately and never folded into the command count.
    //
    // Phase 4.5's per-connection in-flight credit is the consumer the release exists for.
    // This stage produces both signals and counts them.
    class CommandLifecycleSink
    {
    public:
        virtual ~CommandLifecycleSink() = default;

        // Runs on the owning Worker, and while it holds its scheduling mutex, so it must
        // not block, allocate or throw. Returning credit means an atomic and at most one
        // wake-up.
        virtual void onCommandReleased(snf::net::ConnectionId connection) noexcept = 0;

        // Runs on the thread that attempted the post, right after the runtime refused it.
        virtual void onCommandAdmissionRejected(snf::net::ConnectionId connection) noexcept = 0;
    };

    // Move-only, and its destruction is the release.
    //
    // The scheduler already destroys a submission exactly once, whether the command
    // succeeded, threw, was cancelled, was discarded from a mailbox, or was refused by a
    // closed or full ingress. Riding along inside the submission's payload therefore
    // yields exactly-once credit accounting with no scheduler change and no dependence on
    // whether a response was produced.
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

    // The stand-in until a credit owner consumes the release. It exists so the production
    // path has a wired consumer and both counts are observable in a snapshot.
    class CountingCommandLifecycleSink final : public CommandLifecycleSink
    {
    public:
        void onCommandReleased(snf::net::ConnectionId connection) noexcept override;
        void onCommandAdmissionRejected(snf::net::ConnectionId connection) noexcept override;

        [[nodiscard]] std::uint64_t releaseCount() const noexcept;
        [[nodiscard]] std::uint64_t admissionRejectionCount() const noexcept;
        // Commands that were admitted and reached a final result. This is maintained as
        // one atomic balance rather than computed by subtracting the two counters above:
        // relaxed loads from independent atomics could otherwise observe a rejection
        // without its earlier release and wrap an unsigned snapshot.
        [[nodiscard]] std::uint64_t terminalCount() const noexcept;

    private:
        std::atomic<std::uint64_t> _releases{0};
        std::atomic<std::uint64_t> _admission_rejections{0};
        std::atomic<std::uint64_t> _terminals{0};
    };
}
