#pragma once

#include "snf/net/session.hpp"
#include "snf/net/unique_file_descriptor.hpp"

#include <cstdint>
#include <unordered_map>

namespace snf::server
{
    class TcpServer
    {
    public:
        explicit TcpServer(std::uint16_t port);

        TcpServer(const TcpServer&) = delete;
        TcpServer& operator=(const TcpServer&) = delete;

        TcpServer(TcpServer&&) = delete;
        TcpServer& operator=(TcpServer&&) = delete;

        void run();

    private:
        void registerListener() const;
        void acceptPendingClients();
        void handleClientEvent(int client_descriptor, std::uint32_t event_flags);
        void removeSession(int client_descriptor);

        snf::net::UniqueFileDescriptor _listener;
        snf::net::UniqueFileDescriptor _epoll;
        std::unordered_map<int, snf::net::Session> _sessions;
    };
}
