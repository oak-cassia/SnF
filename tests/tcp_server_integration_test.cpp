#include "snf/net/socket_options.hpp"
#include "snf/net/termination_signal.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/server/tcp_server.hpp"

#include <arpa/inet.h>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <span>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

namespace
{
    using namespace std::chrono_literals;

    class RunningServer
    {
    public:
        explicit RunningServer(
            const int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD)
            : _server(0, 200ms)
            , _termination_signal_descriptor(termination_signal_descriptor)
            , _thread(
                  [this]
                  {
                      try
                      {
                          _server.run(_termination_signal_descriptor);
                      }
                      catch (...)
                      {
                          _server_error = std::current_exception();
                      }
                  })
        {
        }

        ~RunningServer()
        {
            if (_thread.joinable())
            {
                _server.requestStop();
                _thread.join();
            }
        }

        RunningServer(const RunningServer&) = delete;
        RunningServer& operator=(const RunningServer&) = delete;

        [[nodiscard]] std::uint16_t getPort() const noexcept
        {
            return _server.getPort();
        }

        void stop()
        {
            _server.requestStop();
            join();
        }

        void join()
        {
            _thread.join();

            if (_server_error)
            {
                std::rethrow_exception(_server_error);
            }
        }

    private:
        snf::server::TcpServer _server;
        int _termination_signal_descriptor;
        std::exception_ptr _server_error;
        std::thread _thread;
    };

    snf::net::UniqueFileDescriptor connect_client(const std::uint16_t port)
    {
        snf::net::UniqueFileDescriptor client_socket{
            ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
        assert(client_socket.isValid());

        timeval timeout{
            .tv_sec = 2,
            .tv_usec = 0,
        };

        assert(::setsockopt(client_socket.getDescriptor(),
                            SOL_SOCKET,
                            SO_RCVTIMEO,
                            &timeout,
                            sizeof(timeout)) == 0);
        assert(::setsockopt(client_socket.getDescriptor(),
                            SOL_SOCKET,
                            SO_SNDTIMEO,
                            &timeout,
                            sizeof(timeout)) == 0);

        snf::net::enable_tcp_no_delay(client_socket.getDescriptor());

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(port);
        assert(::inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) == 1);

        assert(::connect(client_socket.getDescriptor(),
                         reinterpret_cast<const sockaddr*>(&server_address),
                         sizeof(server_address)) == 0);

        return client_socket;
    }

    void send_all(const int socket_descriptor, const std::span<const std::byte> bytes)
    {
        std::size_t sent_byte_count = 0;

        while (sent_byte_count < bytes.size())
        {
            const auto result = ::send(socket_descriptor,
                                       bytes.data() + sent_byte_count,
                                       bytes.size() - sent_byte_count,
                                       MSG_NOSIGNAL);

            if (result > 0)
            {
                sent_byte_count += static_cast<std::size_t>(result);
                continue;
            }

            if (result == -1 && errno == EINTR)
            {
                continue;
            }

            assert(false);
        }
    }

    std::vector<std::byte> receive_exact(const int socket_descriptor,
                                         const std::size_t expected_byte_count)
    {
        std::vector<std::byte> received_bytes(expected_byte_count);
        std::size_t received_byte_count = 0;

        while (received_byte_count < expected_byte_count)
        {
            const auto result = ::recv(socket_descriptor,
                                       received_bytes.data() + received_byte_count,
                                       expected_byte_count - received_byte_count,
                                       0);

            if (result > 0)
            {
                received_byte_count += static_cast<std::size_t>(result);
                continue;
            }

            if (result == -1 && errno == EINTR)
            {
                continue;
            }

            assert(false);
        }

        return received_bytes;
    }

    void assert_pong(const std::vector<std::byte>& response, const snf::protocol::Frame& request)
    {
        snf::protocol::FrameDecoder decoder;
        const auto result = decoder.append(response);

        assert(result.ok());
        assert(result.frames.size() == 1);
        assert(result.frames[0].type == snf::protocol::MessageType::Pong);
        assert(result.frames[0].request_id == request.request_id);
        assert(result.frames[0].payload == request.payload);
    }

    void test_returns_pong_for_ping()
    {
        RunningServer server;
        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 1,
            .payload = {std::byte{0xAA}, std::byte{0xBB}},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);
        send_all(client.getDescriptor(), encoded_request);

        const auto response = receive_exact(client.getDescriptor(), encoded_request.size());
        assert_pong(response, request);

        server.stop();
    }

    void test_decodes_ping_sent_one_byte_at_a_time()
    {
        RunningServer server;
        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 2,
            .payload = {std::byte{0x10}, std::byte{0x20}, std::byte{0x30}},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);

        for (const std::byte byte : encoded_request)
        {
            send_all(client.getDescriptor(), std::span<const std::byte>{&byte, 1});
        }

        const auto response = receive_exact(client.getDescriptor(), encoded_request.size());
        assert_pong(response, request);

        server.stop();
    }

    void test_decodes_multiple_pings_from_one_send()
    {
        RunningServer server;
        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame first_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 3,
            .payload = {std::byte{0x01}},
        };
        const snf::protocol::Frame second_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 4,
            .payload = {std::byte{0x02}, std::byte{0x03}},
        };

        const auto first_encoded_request = snf::protocol::encode_frame(first_request);
        const auto second_encoded_request = snf::protocol::encode_frame(second_request);

        std::vector<std::byte> bundled_requests = first_encoded_request;
        bundled_requests.insert(
            bundled_requests.end(), second_encoded_request.begin(), second_encoded_request.end());

        send_all(client.getDescriptor(), bundled_requests);

        const auto response = receive_exact(client.getDescriptor(), bundled_requests.size());
        snf::protocol::FrameDecoder decoder;
        const auto result = decoder.append(response);

        assert(result.ok());
        assert(result.frames.size() == 2);
        assert(result.frames[0].type == snf::protocol::MessageType::Pong);
        assert(result.frames[0].request_id == first_request.request_id);
        assert(result.frames[0].payload == first_request.payload);
        assert(result.frames[1].type == snf::protocol::MessageType::Pong);
        assert(result.frames[1].request_id == second_request.request_id);
        assert(result.frames[1].payload == second_request.payload);

        server.stop();
    }

    void test_survives_client_close_during_partial_frame()
    {
        RunningServer server;

        {
            const auto partial_client = connect_client(server.getPort());
            const snf::protocol::Frame incomplete_request{
                .type = snf::protocol::MessageType::Ping,
                .request_id = 5,
                .payload = {std::byte{0xAA}},
            };

            const auto encoded_request = snf::protocol::encode_frame(incomplete_request);
            send_all(partial_client.getDescriptor(),
                     std::span<const std::byte>{encoded_request}.first(5));
        }

        const auto next_client = connect_client(server.getPort());
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 6,
            .payload = {},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);
        send_all(next_client.getDescriptor(), encoded_request);
        const auto response = receive_exact(next_client.getDescriptor(), encoded_request.size());
        assert_pong(response, request);

        server.stop();
    }

    void test_request_stop_closes_listener_and_active_sessions()
    {
        RunningServer server;
        const auto port = server.getPort();
        const auto client = connect_client(port);

        const auto start_time = std::chrono::steady_clock::now();
        server.stop();
        const auto elapsed_time = std::chrono::steady_clock::now() - start_time;

        assert(elapsed_time < 1s);

        snf::net::UniqueFileDescriptor connection_attempt{
            ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
        assert(connection_attempt.isValid());

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(port);
        assert(::inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) == 1);

        assert(::connect(connection_attempt.getDescriptor(),
                         reinterpret_cast<const sockaddr*>(&server_address),
                         sizeof(server_address)) == -1);
    }

    void test_termination_signal_stops_server(const int signal_number)
    {
        const auto termination_signal = snf::net::create_termination_signal_listener();
        RunningServer server{termination_signal.getDescriptor()};

        assert(::kill(::getpid(), signal_number) == 0);
        server.join();
    }
}

int main()
{
    test_returns_pong_for_ping();
    test_decodes_ping_sent_one_byte_at_a_time();
    test_decodes_multiple_pings_from_one_send();
    test_survives_client_close_during_partial_frame();
    test_request_stop_closes_listener_and_active_sessions();
    test_termination_signal_stops_server(SIGINT);
    test_termination_signal_stops_server(SIGTERM);
}
