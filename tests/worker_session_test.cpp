#include "snf/adapter/game_actor_factory.hpp"
#include "snf/adapter/game_request_sink.hpp"
#include "snf/game/equip_skill.hpp"
#include "snf/game/purchase.hpp"
#include "snf/game/skill_id.hpp"
#include "snf/net/tcp_listener.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/worker.hpp"

#include "socket_test_support.hpp"

#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
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

    // Waits until the sink has released every session, which also means each
    // PlayerConnectionClosedMessage has been dispatched. Turn counts are only
    // deterministic once that has happened, so tests that assert exact counts
    // disconnect first and wait here instead of relying on shutdown ordering.
    void waitForSessionsReleased(const SessionHarness& harness)
    {
        const auto deadline = Clock::now() + 2s;
        while (harness.sink().sessionCount() != 0 && Clock::now() < deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(harness.sink().sessionCount() == 0);
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

        client.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
        // Two frames in, two answered, and exactly two turns: the Authenticate and
        // the connection release. The Ping that preceded them created no actor even
        // in this combined flow.
        assert(harness.metrics().network.received_frames == 2);
        assert(harness.metrics().network.sent_frames == 2);
        assert(harness.metrics().actor.actor_turns == 2);
    }

    // Serving Ping before authentication must not loosen the boundary for
    // anything else. Every other client frame stays a frame-order violation until
    // the connection has a session, matching the legacy requires_persistent_player
    // check that answered InvalidPayload. This test is meant to keep holding as
    // 11B onwards add real routing for these frames.
    void test_only_ping_and_authenticate_cross_the_pre_auth_boundary()
    {
        constexpr std::array<snf::protocol::MessageType, 10> GUARDED{
            snf::protocol::MessageType::EnterZone,
            snf::protocol::MessageType::Move,
            snf::protocol::MessageType::LeaveZone,
            snf::protocol::MessageType::Purchase,
            snf::protocol::MessageType::RoomJoin,
            snf::protocol::MessageType::BattleStart,
            snf::protocol::MessageType::RoomLeave,
            snf::protocol::MessageType::UseSkill,
            snf::protocol::MessageType::SetMoveIntent,
            snf::protocol::MessageType::EquipSkill,
        };

        SessionHarness harness;
        for (const auto type : GUARDED)
        {
            // One connection per frame: a rejected frame is terminal for it.
            auto client = connectClient(harness.port());
            sendAll(
                client.getDescriptor(),
                snf::protocol::encode_frame(snf::protocol::Frame{
                    .type = type,
                    .request_id = 1,
                    .payload = {std::byte{0x00}},
                })
            );
            assert(receivesEof(client.getDescriptor()));
        }

        harness.stop();
        assert(harness.metrics().network.protocol_errors == GUARDED.size());
        assert(harness.metrics().actor.actor_turns == 0);
        assert(harness.sink().sessionCount() == 0);
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

    // The legacy path allowed a player to reconnect after its session ended, and
    // that only works here because the close releases the actor's binding as well
    // as the sink's entry. Without the release the actor would keep refusing the
    // same player as a conflict forever.
    void test_the_same_player_can_reconnect_after_disconnecting()
    {
        SessionHarness harness;
        {
            auto first = connectClient(harness.port());
            sendAll(first.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(1, 77)));
            expectFrame(first.getDescriptor(), authenticatedFrame(1, 77));
        }
        waitForSessionsReleased(harness);

        auto second = connectClient(harness.port());
        sendAll(second.getDescriptor(), snf::protocol::encode_frame(authenticateFrame(2, 77)));
        expectFrame(second.getDescriptor(), authenticatedFrame(2, 77));
        assert(harness.sink().sessionCount() == 1);

        second.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
        assert(harness.metrics().network.graceful_closes == 0);
        assert(harness.metrics().network.sent_frames == 2);
    }

    // Stage 11B. Purchase and EquipSkill are the two commands that belong to the
    // authenticated player itself, and the two the legacy gateway guarded with
    // requires_persistent_player.
    [[nodiscard]] snf::protocol::Frame purchaseFrame(const std::uint32_t request_id, const std::uint64_t key, const std::uint32_t product)
    {
        std::vector<std::byte> payload;
        payload.reserve(12);
        for (std::size_t index = 0; index < 8; ++index)
        {
            payload.push_back(static_cast<std::byte>((key >> (8 * (7 - index))) & 0xFFULL));
        }
        for (std::size_t index = 0; index < 4; ++index)
        {
            payload.push_back(static_cast<std::byte>((product >> (8 * (3 - index))) & 0xFFU));
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Purchase,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    [[nodiscard]] snf::protocol::Frame equipSkillFrame(const std::uint32_t request_id, const std::uint32_t skill_id)
    {
        std::vector<std::byte> payload;
        payload.reserve(4);
        for (std::size_t index = 0; index < 4; ++index)
        {
            payload.push_back(static_cast<std::byte>((skill_id >> (8 * (3 - index))) & 0xFFU));
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::EquipSkill,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    [[nodiscard]] std::uint64_t readBigEndian64(const std::vector<std::byte>& payload, const std::size_t offset)
    {
        std::uint64_t value = 0;
        for (std::size_t index = offset; index < offset + 8; ++index)
        {
            value = (value << 8U) | std::to_integer<std::uint64_t>(payload[index]);
        }
        return value;
    }

    [[nodiscard]] std::uint32_t readBigEndian32(const std::vector<std::byte>& payload, const std::size_t offset)
    {
        std::uint32_t value = 0;
        for (std::size_t index = offset; index < offset + 4; ++index)
        {
            value = (value << 8U) | std::to_integer<std::uint32_t>(payload[index]);
        }
        return value;
    }

    [[nodiscard]] snf::protocol::Frame receiveFrame(const int descriptor, const std::size_t encoded_size)
    {
        const auto encoded = receiveExact(descriptor, encoded_size);
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(encoded);
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        return decoded.frames.front();
    }

    void authenticate(const int descriptor, const std::uint32_t request_id, const std::uint64_t player)
    {
        sendAll(descriptor, snf::protocol::encode_frame(authenticateFrame(request_id, player)));
        expectFrame(descriptor, authenticatedFrame(request_id, player));
    }

    // A committed purchase, then the same idempotency key again. The replay flag
    // proves both frames reached the same domain Player and that its state
    // survived between turns, which is what routing has to deliver.
    void test_purchase_reaches_the_player_and_replays_on_the_same_key()
    {
        constexpr std::size_t PURCHASE_RESULT_PAYLOAD = 30;
        SessionHarness harness;
        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 77);

        const auto encoded_size = snf::protocol::encode_frame(snf::protocol::Frame{
                                                                  .type = snf::protocol::MessageType::PurchaseResult,
                                                                  .request_id = 0,
                                                                  .payload = std::vector<std::byte>(PURCHASE_RESULT_PAYLOAD),
                                                              })
                                      .size();

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(purchaseFrame(2, 0xABCD, snf::server::BASIC_PRODUCT.value)));
        const auto first = receiveFrame(client.getDescriptor(), encoded_size);
        assert(first.type == snf::protocol::MessageType::PurchaseResult);
        assert(first.request_id == 2);
        assert(first.payload.size() == PURCHASE_RESULT_PAYLOAD);
        assert(std::to_integer<std::uint8_t>(first.payload[0]) == static_cast<std::uint8_t>(snf::server::PurchaseStatus::Committed));
        assert(std::to_integer<std::uint8_t>(first.payload[1]) == 0);
        assert(readBigEndian64(first.payload, 2) == 0xABCD);
        assert(readBigEndian32(first.payload, 10) == snf::server::BASIC_PRODUCT.value);
        assert(readBigEndian64(first.payload, 14) == snf::server::INITIAL_CURRENCY_BALANCE - snf::server::BASIC_PRODUCT_PRICE);
        assert(readBigEndian64(first.payload, 22) == snf::server::BASIC_PRODUCT_GRANT_COUNT);

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(purchaseFrame(3, 0xABCD, snf::server::BASIC_PRODUCT.value)));
        const auto replay = receiveFrame(client.getDescriptor(), encoded_size);
        assert(replay.request_id == 3);
        assert(std::to_integer<std::uint8_t>(replay.payload[0]) == static_cast<std::uint8_t>(snf::server::PurchaseStatus::Committed));
        assert(std::to_integer<std::uint8_t>(replay.payload[1]) == 1);
        // The balance did not move a second time.
        assert(readBigEndian64(replay.payload, 14) == snf::server::INITIAL_CURRENCY_BALANCE - snf::server::BASIC_PRODUCT_PRICE);

        client.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
        // Authenticate, two purchases, and the connection release.
        assert(harness.metrics().actor.actor_turns == 4);
        assert(harness.metrics().network.sent_frames == 3);
    }

    // Buying the skill product and then equipping it. Two different commands over
    // one session, sharing the actor's state.
    void test_equip_skill_reaches_the_player_after_the_skill_is_owned()
    {
        constexpr std::size_t EQUIP_RESULT_PAYLOAD = 5;
        SessionHarness harness;
        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 77);

        const auto equip_size = snf::protocol::encode_frame(snf::protocol::Frame{
                                                                .type = snf::protocol::MessageType::EquipSkillResult,
                                                                .request_id = 0,
                                                                .payload = std::vector<std::byte>(EQUIP_RESULT_PAYLOAD),
                                                            })
                                    .size();

        // Not owned yet.
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(equipSkillFrame(2, snf::server::ARCANE_BOLT_SKILL_ID.value)));
        const auto refused = receiveFrame(client.getDescriptor(), equip_size);
        assert(refused.type == snf::protocol::MessageType::EquipSkillResult);
        assert(refused.request_id == 2);
        assert(std::to_integer<std::uint8_t>(refused.payload[0]) == static_cast<std::uint8_t>(snf::server::EquipSkillStatus::SkillNotOwned));

        const auto purchase_size = snf::protocol::encode_frame(snf::protocol::Frame{
                                                                   .type = snf::protocol::MessageType::PurchaseResult,
                                                                   .request_id = 0,
                                                                   .payload = std::vector<std::byte>(30),
                                                               })
                                       .size();
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(purchaseFrame(3, 0x1234, snf::server::ARCANE_BOLT_PRODUCT.value)));
        const auto purchased = receiveFrame(client.getDescriptor(), purchase_size);
        assert(std::to_integer<std::uint8_t>(purchased.payload[0]) == static_cast<std::uint8_t>(snf::server::PurchaseStatus::Committed));

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(equipSkillFrame(4, snf::server::ARCANE_BOLT_SKILL_ID.value)));
        const auto equipped = receiveFrame(client.getDescriptor(), equip_size);
        assert(equipped.request_id == 4);
        assert(std::to_integer<std::uint8_t>(equipped.payload[0]) == static_cast<std::uint8_t>(snf::server::EquipSkillStatus::Equipped));
        assert(readBigEndian32(equipped.payload, 1) == snf::server::ARCANE_BOLT_SKILL_ID.value);

        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    // A malformed payload is a protocol violation, so it never reaches an actor.
    void test_malformed_player_command_payloads_never_reach_an_actor()
    {
        SessionHarness harness;

        const std::vector<snf::protocol::Frame> malformed{
            // Purchase: wrong length, zero key, zero product.
            snf::protocol::Frame{.type = snf::protocol::MessageType::Purchase, .request_id = 1, .payload = {std::byte{0x01}}},
            purchaseFrame(1, 0, snf::server::BASIC_PRODUCT.value),
            purchaseFrame(1, 0xABCD, 0),
            // EquipSkill: wrong length, zero skill.
            snf::protocol::Frame{.type = snf::protocol::MessageType::EquipSkill, .request_id = 1, .payload = {std::byte{0x01}}},
            equipSkillFrame(1, 0),
        };

        for (const auto& frame : malformed)
        {
            auto client = connectClient(harness.port());
            authenticate(client.getDescriptor(), 1, 77);
            sendAll(client.getDescriptor(), snf::protocol::encode_frame(frame));
            assert(receivesEof(client.getDescriptor()));
            // Each iteration reuses the same player, which only works because the
            // close released the actor's binding.
            waitForSessionsReleased(harness);
        }

        harness.stop();
        assert(harness.metrics().network.protocol_errors == malformed.size());
        // Only the Authenticate of each iteration was answered: no malformed frame
        // ever produced a domain response.
        assert(harness.metrics().network.sent_frames == malformed.size());
        // Authenticate plus release per iteration, and nothing else.
        assert(harness.metrics().actor.actor_turns == malformed.size() * 2);
    }
}

#define SNF_RUN_SESSION_TEST(fn)                                                                                                                     \
    do                                                                                                                                               \
    {                                                                                                                                                \
        (fn)();                                                                                                                                      \
        std::cout << "  - " #fn " PASSED" << std::endl;                                                                                              \
    } while (false)

void run_worker_session_tests()
{
    SNF_RUN_SESSION_TEST(test_authenticate_binds_a_session_and_is_idempotent);
    SNF_RUN_SESSION_TEST(test_second_player_on_the_same_connection_is_a_protocol_error);
    SNF_RUN_SESSION_TEST(test_same_player_on_a_second_connection_is_closed_by_the_actor);
    SNF_RUN_SESSION_TEST(test_malformed_authenticate_payloads_are_rejected);
    SNF_RUN_SESSION_TEST(test_pre_auth_ping_is_answered_without_an_actor);
    SNF_RUN_SESSION_TEST(test_pre_auth_ping_does_not_block_authentication);
    SNF_RUN_SESSION_TEST(test_only_ping_and_authenticate_cross_the_pre_auth_boundary);
    SNF_RUN_SESSION_TEST(test_disconnect_releases_the_session_entry);
    SNF_RUN_SESSION_TEST(test_the_same_player_can_reconnect_after_disconnecting);
    SNF_RUN_SESSION_TEST(test_purchase_reaches_the_player_and_replays_on_the_same_key);
    SNF_RUN_SESSION_TEST(test_equip_skill_reaches_the_player_after_the_skill_is_owned);
    SNF_RUN_SESSION_TEST(test_malformed_player_command_payloads_never_reach_an_actor);
}
