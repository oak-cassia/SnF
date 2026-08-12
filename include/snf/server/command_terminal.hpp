#pragma once

#include "snf/net/connection_id.hpp"

#include <atomic>
#include <cstdint>

namespace snf::server
{
    // Observes the moment a command reaches its final result, whichever result that
    // is: the effects it needed were applied, or it failed, or it was cancelled, or it
    // was never admitted. A command with no response reaches this point exactly like
    // one that answered.
    //
    // Phase 4.5's per-connection in-flight credit is the consumer this exists for.
    // This stage only produces the signal and counts it.
    class CommandTerminalSink
    {
    public:
        virtual ~CommandTerminalSink() = default;

        // Runs on the owning Worker, and while it holds its scheduling mutex, so it
        // must not block, allocate or throw. Returning credit means an atomic and at
        // most one wake-up.
        virtual void onCommandTerminal(snf::net::ConnectionId connection) noexcept = 0;
    };

    // Move-only, and its destruction is the signal.
    //
    // The scheduler already destroys an accepted submission exactly once, whether the
    // command succeeded, threw, was cancelled, was discarded from a mailbox, or was
    // refused by a closed or full ingress. Riding along inside the submission's
    // payload therefore yields exactly-once terminal accounting with no scheduler
    // change and no dependence on whether a response was produced.
    class CommandTerminalToken final
    {
    public:
        CommandTerminalToken() noexcept = default;
        CommandTerminalToken(CommandTerminalSink& sink, snf::net::ConnectionId connection) noexcept;
        ~CommandTerminalToken();

        CommandTerminalToken(const CommandTerminalToken&) = delete;
        CommandTerminalToken& operator=(const CommandTerminalToken&) = delete;
        CommandTerminalToken(CommandTerminalToken&& other) noexcept;
        CommandTerminalToken& operator=(CommandTerminalToken&& other) noexcept;

        [[nodiscard]] bool armed() const noexcept;

    private:
        void report() noexcept;

        CommandTerminalSink* _sink{nullptr};
        snf::net::ConnectionId _connection{};
    };

    // The stand-in until a credit owner consumes the signal. It exists so the
    // production path has a wired consumer and the count is observable in a snapshot.
    class CountingCommandTerminalSink final : public CommandTerminalSink
    {
    public:
        void onCommandTerminal(snf::net::ConnectionId connection) noexcept override;

        [[nodiscard]] std::uint64_t count() const noexcept;

    private:
        std::atomic<std::uint64_t> _count{0};
    };
}
