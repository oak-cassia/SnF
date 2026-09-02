#pragma once

#include "snf/net/unique_file_descriptor.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>

#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace snf::test
{
    [[nodiscard]] inline std::uint16_t portOf(const int descriptor)
    {
        sockaddr_in address{};
        socklen_t address_size = sizeof(address);
        assert(::getsockname(descriptor, reinterpret_cast<sockaddr*>(&address), &address_size) == 0);
        return ntohs(address.sin_port);
    }

    [[nodiscard]] inline snf::net::UniqueFileDescriptor connectClient(const std::uint16_t port, const int receive_buffer_size = 0)
    {
        const int descriptor = ::socket(AF_INET, SOCK_STREAM, 0);
        assert(descriptor != -1);
        snf::net::UniqueFileDescriptor client{descriptor};

        timeval timeout{.tv_sec = 2, .tv_usec = 0};
        assert(::setsockopt(descriptor, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
        if (receive_buffer_size > 0)
        {
            assert(::setsockopt(descriptor, SOL_SOCKET, SO_RCVBUF, &receive_buffer_size, sizeof(receive_buffer_size)) == 0);
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        assert(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);

        int result = ::connect(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
        while (result == -1 && errno == EINTR)
        {
            result = ::connect(descriptor, reinterpret_cast<const sockaddr*>(&address), sizeof(address));
        }
        assert(result == 0);
        return client;
    }

    inline void sendAll(const int descriptor, const std::vector<std::byte>& bytes)
    {
        std::size_t offset = 0;
        while (offset < bytes.size())
        {
            const ssize_t sent = ::send(descriptor, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
            if (sent == -1 && errno == EINTR)
            {
                continue;
            }
            assert(sent > 0);
            offset += static_cast<std::size_t>(sent);
        }
    }

    [[nodiscard]] inline std::vector<std::byte> receiveExact(const int descriptor, const std::size_t byte_count)
    {
        std::vector<std::byte> bytes(byte_count);
        std::size_t offset = 0;
        while (offset < byte_count)
        {
            const ssize_t received = ::recv(descriptor, bytes.data() + offset, byte_count - offset, 0);
            if (received == -1 && errno == EINTR)
            {
                continue;
            }
            assert(received > 0);
            offset += static_cast<std::size_t>(received);
        }
        return bytes;
    }
}
