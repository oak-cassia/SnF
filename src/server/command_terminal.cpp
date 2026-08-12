#include "snf/server/command_terminal.hpp"

#include <utility>

namespace snf::server
{
    CommandTerminalToken::CommandTerminalToken(CommandTerminalSink& sink,
                                               const snf::net::ConnectionId connection) noexcept
        : _sink(&sink)
        , _connection(connection)
    {
    }

    CommandTerminalToken::~CommandTerminalToken()
    {
        report();
    }

    CommandTerminalToken::CommandTerminalToken(CommandTerminalToken&& other) noexcept
        : _sink(std::exchange(other._sink, nullptr))
        , _connection(other._connection)
    {
    }

    CommandTerminalToken& CommandTerminalToken::operator=(CommandTerminalToken&& other) noexcept
    {
        if (this != &other)
        {
            report();
            _sink = std::exchange(other._sink, nullptr);
            _connection = other._connection;
        }

        return *this;
    }

    bool CommandTerminalToken::armed() const noexcept
    {
        return _sink != nullptr;
    }

    void CommandTerminalToken::report() noexcept
    {
        if (_sink != nullptr)
        {
            _sink->onCommandTerminal(_connection);
            _sink = nullptr;
        }
    }

    void
    CountingCommandTerminalSink::onCommandTerminal(const snf::net::ConnectionId connection) noexcept
    {
        static_cast<void>(connection);
        _count.fetch_add(1, std::memory_order_relaxed);
    }

    std::uint64_t CountingCommandTerminalSink::count() const noexcept
    {
        return _count.load(std::memory_order_relaxed);
    }
}
