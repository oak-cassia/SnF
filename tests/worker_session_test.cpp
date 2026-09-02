#include "snf/adapter/game_actor_factory.hpp"
#include "snf/adapter/game_request_sink.hpp"
#include "snf/net/tcp_listener.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/worker.hpp"

#include "socket_test_support.hpp"

#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <sys/socket.h>
#include <thread>
#include <vector>

// Stage 11A. The legacy PlayerSessionDirectory was one mutex-guarded map holding
// both directions of the session identity. This runtime splits it: the sink owns
// connection -> player on the owning Worker's thread, and the PlayerActor owns
// player -> connection. These tests pin each outcome that map used to return.
//
//   legacy PlayerAttachResult   ->  who decides now
//   Attached                        sink writes the entry, actor binds
//   AlreadyAttached                 sink, same connection and same player
//   ConnectionConflict              sink, same connection claiming another player
//   PlayerConflict                  PlayerActor, it already holds a connection
//   ProvisionalActivity             cannot happen, pre-auth work creates no actor
//   Closing                         cannot happen, a closed connection posts no frame
namespace
{
    using namespace std::chrono_literals;
    using Clock = std::chrono::steady_clock;
    using snf::test::connectClient;
    using snf::test::portOf;
    using snf::test::receiveExact;
    using snf::test::sendAll;

    [[nodiscard]] std::vector<std::byte> playerIdWire(const std::uint64_t player)
    {
        std::vector<std::byte> payload(8);
        for (std::size_t index = 0; index < payload.size(); ++index)
        {
            payload[index] = static_cast<std::byte>((player >> (8 * (7 - index))) & 0xFFULL);
        }
        return payload;
    }

    [[nodiscard]] snf::protocol::Frame authenticateFrame(const std::uint32_t request_id, const std::uint64_t player)
    {
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Authenticate,
            .request_id = request_id,
            .payload = playerIdWire(player),
        };
    }

    [[nodiscard]] snf::protocol::Frame authenticatedFrame(const std::uint32_t request_id, const std::uint64_t player)
    {
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Authenticated,
            .request_id = request_id,
            .payload = playerIdWire(player),
        };
    }

    [[nodiscard]] snf::worker::WorkerActorConfig sessionActorConfig()
    {
        return snf::worker::WorkerActorConfig{
            .actor_table_capacity = 10,
            .max_mailbox_messages_per_actor = 10,
            .max_mailbox_bytes_per_actor = 2048,
            .max_mailbox_messages_total = 50,
            .max_mailbox_bytes_total = 10240,
            .max_application_timer_bytes_total = 4096,
        };
    }

    [[nodiscard]] snf::worker::WorkerNetworkConfig sessionNetworkConfig()
    {
        snf::worker::WorkerNetworkConfig config{};
        config.table.capacity = 8;
        config.poll_registration_capacity = 9;
        config.max_accepts_per_poll = 8;
        config.receive_chunk_bytes = 1024;
        return config;
    }

    // Owns the Worker, its thread and the sink for one test.
    class SessionHarness final
    {
    public:
        SessionHarness()
        {
            _worker = std::make_unique<snf::worker::Worker>(
                snf::worker::WorkerId{0},
                1,
                snf::worker::WorkerBudgets::defaults(),
                snf::worker::WorkerInboxConfig{},
                sessionNetworkConfig(),
                _sink,
                _actor_config,
                _factory
            );
            _factory.setTimerAdmission(*_worker);
            _sink.setWorker(*_worker);

            auto listener = snf::net::create_tcp_listener(0);
            _port = portOf(listener.getDescriptor());
            _worker->attachListener(std::move(listener));
            _thread = std::thread(
                [this]
                {
                    _worker->run();
                }
            );
        }

        ~SessionHarness()
        {
            stop();
        }

        SessionHarness(const SessionHarness&) = delete;
        SessionHarness& operator=(const SessionHarness&) = delete;

        void stop()
        {
            if (_thread.joinable())
            {
                _worker->requestStop();
                _thread.join();
            }
        }

        [[nodiscard]] std::uint16_t port() const noexcept
        {
            return _port;
        }

        [[nodiscard]] const snf::adapter::GameRequestSink& sink() const noexcept
        {
            return _sink;
        }

        [[nodiscard]] const snf::worker::WorkerMetrics& metrics() const noexcept
        {
            return _worker->metrics();
        }

    private:
        snf::worker::WorkerActorConfig _actor_config{sessionActorConfig()};
        snf::adapter::GameActorFactory _factory;
        snf::adapter::GameRequestSink _sink;
        std::unique_ptr<snf::worker::Worker> _worker;
        std::uint16_t _port{0};
        std::thread _thread;
    };

    void expectFrame(const int descriptor, const snf::protocol::Frame& expected)
    {
        const auto encoded = receiveExact(descriptor, snf::protocol::encode_frame(expected).size());
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(encoded);
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        assert(decoded.frames.front() == expected);
    }

    [[nodiscard]] bool receivesEof(const int descriptor)
    {
        std::byte byte{};
        const ssize_t received = ::recv(descriptor, &byte, sizeof(byte), 0);
        return received == 0;
    }

    // Attached, then AlreadyAttached: re-sending the same player id on the same
    // connection is idempotent and answered again, exactly as the legacy
    // directory allowed it to proceed.
    void test_authenticate_binds_a_session_and_is_idempotent()
    {
        SessionHarness harness;
        auto client = connectClient(harness.port());

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(1, 77)));
        expectFrame(client.getDescriptor(), authenticatedFrame(1, 77));
        assert(harness.sink().sessionCount() == 1);

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(2, 77)));
        expectFrame(client.getDescriptor(), authenticatedFrame(2, 77));
        assert(harness.sink().sessionCount() == 1);

        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    // ConnectionConflict. The sink can answer this one alone, because it holds the
    // connection -> player direction for this very connection.
    void test_second_player_on_the_same_connection_is_a_protocol_error()
    {
        SessionHarness harness;
        auto client = connectClient(harness.port());

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(1, 77)));
        expectFrame(client.getDescriptor(), authenticatedFrame(1, 77));

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(2, 88)));
        assert(receivesEof(client.getDescriptor()));

        harness.stop();
        assert(harness.metrics().network.protocol_errors == 1);
        // The rejected frame must not have moved the session onto the new player.
        assert(harness.sink().sessionCount() == 0);
    }

    // PlayerConflict. The second connection is a different connection, so the sink
    // cannot see the collision; the PlayerActor that already holds a binding does,
    // and closes the intruding connection.
    void test_same_player_on_a_second_connection_is_closed_by_the_actor()
    {
        SessionHarness harness;
        auto first = connectClient(harness.port());
        sendAll(first.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(1, 77)));
        expectFrame(first.getDescriptor(), authenticatedFrame(1, 77));

        auto second = connectClient(harness.port());
        sendAll(second.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(1, 77)));
        assert(receivesEof(second.getDescriptor()));

        // The original session survives the intrusion.
        sendAll(
            first.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::Ping,
                .request_id = 9,
                .payload = {std::byte{0x01}},
            })
        );
        expectFrame(
            first.getDescriptor(),
            snf::protocol::Frame{
                .type = snf::protocol::MessageType::Pong,
                .request_id = 9,
                .payload = {std::byte{0x01}},
            }
        );

        // The intruding connection did get a session entry, because the sink only
        // learns of the conflict from the actor. The graceful close the actor asked
        // for must release it again, leaving only the original session.
        const auto released = Clock::now() + 2s;
        while (harness.sink().sessionCount() != 1 && Clock::now() < released)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(harness.sink().sessionCount() == 1);

        harness.stop();
        // A conflict is an application decision, not a malformed frame.
        assert(harness.metrics().network.protocol_errors == 0);
        assert(harness.metrics().network.graceful_closes >= 1);
        assert(harness.metrics().actor.effect_send_failures == 0);
    }

    // A malformed player id never reaches an actor.
    void test_malformed_authenticate_payloads_are_rejected()
    {
        SessionHarness harness;

        {
            auto client = connectClient(harness.port());
            sendAll(
                client.getDescriptor(),
                snf::protocol::encode_frame(snf::protocol::Frame{
                    .type = snf::protocol::MessageType::Authenticate,
                    .request_id = 1,
                    .payload = {std::byte{0x01}, std::byte{0x02}},
                })
            );
            assert(receivesEof(client.getDescriptor()));
        }
        {
            auto client = connectClient(harness.port());
            sendAll(client.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(1, 0)));
            assert(receivesEof(client.getDescriptor()));
        }

        harness.stop();
        assert(harness.metrics().network.protocol_errors == 2);
        assert(harness.metrics().actor.actor_turns == 0);
        assert(harness.sink().sessionCount() == 0);
    }

    // The sink answers a pre-auth Ping itself, so an unauthenticated connection
    // cannot allocate an actor slot. WorkerMetrics is owner-thread state, so the
    // claim is checked after the Worker has stopped.
    void test_pre_auth_ping_is_answered_without_an_actor()
    {
        SessionHarness harness;
        auto client = connectClient(harness.port());

        sendAll(
            client.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::Ping,
                .request_id = 5,
                .payload = {std::byte{0x42}},
            })
        );
        expectFrame(
            client.getDescriptor(),
            snf::protocol::Frame{
                .type = snf::protocol::MessageType::Pong,
                .request_id = 5,
                .payload = {std::byte{0x42}},
            }
        );
        assert(harness.sink().sessionCount() == 0);

        harness.stop();
        assert(harness.metrics().actor.actor_turns == 0);
        assert(harness.metrics().network.sent_frames == 1);
        assert(harness.metrics().network.protocol_errors == 0);
    }

    // ProvisionalActivity cannot arise: pre-auth work creates no actor, so it
    // cannot stop the connection from authenticating afterwards either.
    void test_pre_auth_ping_does_not_block_authentication()
    {
        SessionHarness harness;
        auto client = connectClient(harness.port());

        sendAll(
            client.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::Ping,
                .request_id = 5,
                .payload = {std::byte{0x42}},
            })
        );
        expectFrame(
            client.getDescriptor(),
            snf::protocol::Frame{
                .type = snf::protocol::MessageType::Pong,
                .request_id = 5,
                .payload = {std::byte{0x42}},
            }
        );

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(6, 77)));
        expectFrame(client.getDescriptor(), authenticatedFrame(6, 77));
        assert(harness.sink().sessionCount() == 1);

        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    // A game frame before authentication is a frame-order violation, matching the
    // legacy requires_persistent_player check that answered InvalidPayload.
    void test_game_frame_before_authentication_is_rejected()
    {
        SessionHarness harness;
        auto client = connectClient(harness.port());

        sendAll(
            client.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::EnterZone,
                .request_id = 1,
                .payload = {std::byte{0x00}},
            })
        );
        assert(receivesEof(client.getDescriptor()));

        harness.stop();
        assert(harness.metrics().network.protocol_errors == 1);
        assert(harness.metrics().actor.actor_turns == 0);
    }

    // The session entry lives exactly as long as its connection.
    void test_disconnect_releases_the_session_entry()
    {
        SessionHarness harness;
        {
            auto client = connectClient(harness.port());
            sendAll(client.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(1, 77)));
            expectFrame(client.getDescriptor(), authenticatedFrame(1, 77));
            assert(harness.sink().sessionCount() == 1);
        }

        const auto deadline = Clock::now() + 2s;
        while (harness.sink().sessionCount() != 0 && Clock::now() < deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(harness.sink().sessionCount() == 0);

        harness.stop();
    }
}

void run_worker_session_tests()
{
    test_authenticate_binds_a_session_and_is_idempotent();
    test_second_player_on_the_same_connection_is_a_protocol_error();
    test_same_player_on_a_second_connection_is_closed_by_the_actor();
    test_malformed_authenticate_payloads_are_rejected();
    test_pre_auth_ping_is_answered_without_an_actor();
    test_pre_auth_ping_does_not_block_authentication();
    test_game_frame_before_authentication_is_rejected();
    test_disconnect_releases_the_session_entry();
}
