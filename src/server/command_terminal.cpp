#include "snf/server/command_terminal.hpp"

#include <utility>

namespace snf::server
{
    CommandReleaseToken::CommandReleaseToken(CommandLifecycleSink& sink, const snf::net::ConnectionId connection) noexcept
        : _sink(&sink)
        , _connection(connection)
    {
    }

    CommandReleaseToken::~CommandReleaseToken()
    {
        release();
    }

    CommandReleaseToken::CommandReleaseToken(CommandReleaseToken&& other) noexcept
        : _sink(std::exchange(other._sink, nullptr))
        , _connection(other._connection)
    {
    }

    CommandReleaseToken& CommandReleaseToken::operator=(CommandReleaseToken&& other) noexcept
    {
        if (this != &other)
        {
            release();
            _sink = std::exchange(other._sink, nullptr);
            _connection = other._connection;
        }

        return *this;
    }

    bool CommandReleaseToken::armed() const noexcept
    {
        return _sink != nullptr;
    }

    void CommandReleaseToken::release() noexcept
    {
        if (_sink != nullptr)
        {
            _sink->onCommandReleased(_connection);
            _sink = nullptr;
        }
    }

    void CountingCommandLifecycleSink::onCommandReleased(const snf::net::ConnectionId connection) noexcept
    {
        static_cast<void>(connection);
        _releases.fetch_add(1, std::memory_order_relaxed);
        // A refused post is rolled back by onCommandAdmissionRejected on this same
        // atomic. Keeping both halves in one modification order prevents the snapshot
        // underflow possible when subtracting two independently observed atomics.
        _terminals.fetch_add(1, std::memory_order_relaxed);
    }

    void CountingCommandLifecycleSink::onCommandAdmissionRejected(const snf::net::ConnectionId connection) noexcept
    {
        static_cast<void>(connection);
        _admission_rejections.fetch_add(1, std::memory_order_relaxed);
        // ActorRuntime destroys the refused submission before returning Full/Closed,
        // so this command's release increment is sequenced before its rollback.
        _terminals.fetch_sub(1, std::memory_order_relaxed);
    }

    std::uint64_t CountingCommandLifecycleSink::releaseCount() const noexcept
    {
        return _releases.load(std::memory_order_relaxed);
    }

    std::uint64_t CountingCommandLifecycleSink::admissionRejectionCount() const noexcept
    {
        return _admission_rejections.load(std::memory_order_relaxed);
    }

    std::uint64_t CountingCommandLifecycleSink::terminalCount() const noexcept
    {
        return _terminals.load(std::memory_order_relaxed);
    }
}
