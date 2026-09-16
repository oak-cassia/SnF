#include "snf/adapter/game_actor_factory.hpp"
#include "snf/adapter/game_request_sink.hpp"
#include "snf/game/equip_skill.hpp"
#include "snf/game/purchase.hpp"
#include "snf/game/skill_id.hpp"
#include "snf/game/zone_result.hpp"
#include "snf/net/tcp_listener.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/worker/worker.hpp"
#include "snf/worker/worker_group.hpp"

#include "socket_test_support.hpp"

#include <array>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <optional>
#include <sys/socket.h>
#include <thread>
#include <vector>

namespace snf::worker
{
    struct WorkerGroupTestAccess
    {
        [[nodiscard]] static Worker& worker(WorkerGroup& group, const std::size_t index)
        {
            return *group._workers[index];
        }
    };
}

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
            : SessionHarness(snf::server::RoomConfig{})
        {
        }

        explicit SessionHarness(const snf::server::RoomConfig& room_config)
        {
            _factory.setRoomConfig(room_config);
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

    class MultiWorkerSessionHarness final
    {
    public:
        MultiWorkerSessionHarness()
        {
            snf::worker::WorkerGroupConfig config{};
            config.worker_count = WORKER_COUNT;
            config.max_workers = WORKER_COUNT;
            config.port = 0;
            config.network = sessionNetworkConfig();
            config.actor = sessionActorConfig();

            _group = std::make_unique<snf::worker::WorkerGroup>(
                config,
                [this](const snf::worker::WorkerId id) -> std::unique_ptr<snf::worker::RequestSink>
                {
                    auto sink = std::make_unique<snf::adapter::GameRequestSink>();
                    _sinks[id.value] = sink.get();
                    return sink;
                },
                [this](const snf::worker::WorkerId id) -> std::unique_ptr<snf::worker::ActorFactory>
                {
                    auto factory = std::make_unique<snf::adapter::GameActorFactory>();
                    _factories[id.value] = factory.get();
                    return factory;
                }
            );

            for (std::size_t index = 0; index < WORKER_COUNT; ++index)
            {
                auto& worker = snf::worker::WorkerGroupTestAccess::worker(*_group, index);
                _sinks[index]->setWorker(worker);
                _factories[index]->setTimerAdmission(worker);
            }
            _group->start();
        }

        ~MultiWorkerSessionHarness()
        {
            stop();
        }

        MultiWorkerSessionHarness(const MultiWorkerSessionHarness&) = delete;
        MultiWorkerSessionHarness& operator=(const MultiWorkerSessionHarness&) = delete;

        void stop()
        {
            if (_group && _group->isRunning())
            {
                _group->requestStop();
                _group->join();
            }
        }

        [[nodiscard]] std::uint16_t port() const noexcept
        {
            return _group->port();
        }

        [[nodiscard]] std::size_t sessionCount() const noexcept
        {
            std::size_t count = 0;
            for (const auto* sink : _sinks)
            {
                count += sink->sessionCount();
            }
            return count;
        }

        [[nodiscard]] std::uint64_t remoteTellsSent() const noexcept
        {
            std::uint64_t count = 0;
            for (std::size_t index = 0; index < WORKER_COUNT; ++index)
            {
                count += _group->worker(index).metrics().actor.remote_tells_sent;
            }
            return count;
        }

    private:
        static constexpr std::size_t WORKER_COUNT = 2;
        std::array<snf::adapter::GameRequestSink*, WORKER_COUNT> _sinks{};
        std::array<snf::adapter::GameActorFactory*, WORKER_COUNT> _factories{};
        std::unique_ptr<snf::worker::WorkerGroup> _group;
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
        assert(harness.metrics().actor.actor_turns - harness.metrics().actor.lifecycle_turns - harness.metrics().actor.resumed_turns == 2);
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

    [[nodiscard]] std::uint16_t readBigEndian16(const std::vector<std::byte>& payload, const std::size_t offset)
    {
        std::uint16_t value = 0;
        for (std::size_t index = offset; index < offset + 2; ++index)
        {
            value = static_cast<std::uint16_t>((value << 8U) | std::to_integer<std::uint16_t>(payload[index]));
        }
        return value;
    }

    [[nodiscard]] std::int32_t readBigEndianSigned32(const std::vector<std::byte>& payload, const std::size_t offset)
    {
        return static_cast<std::int32_t>(readBigEndian32(payload, offset));
    }

    [[nodiscard]] snf::protocol::Frame enterZoneFrame(
        const std::uint32_t request_id,
        const std::uint64_t zone,
        const std::int32_t x,
        const std::int32_t y
    )
    {
        std::vector<std::byte> payload;
        payload.reserve(16);
        for (std::size_t index = 0; index < 8; ++index)
        {
            payload.push_back(static_cast<std::byte>((zone >> (8 * (7 - index))) & 0xFFULL));
        }
        const auto ux = static_cast<std::uint32_t>(x);
        for (std::size_t index = 0; index < 4; ++index)
        {
            payload.push_back(static_cast<std::byte>((ux >> (8 * (3 - index))) & 0xFFU));
        }
        const auto uy = static_cast<std::uint32_t>(y);
        for (std::size_t index = 0; index < 4; ++index)
        {
            payload.push_back(static_cast<std::byte>((uy >> (8 * (3 - index))) & 0xFFU));
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::EnterZone,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    [[nodiscard]] snf::protocol::Frame moveFrame(const std::uint32_t request_id, const std::int32_t x, const std::int32_t y)
    {
        std::vector<std::byte> payload;
        payload.reserve(8);
        const auto ux = static_cast<std::uint32_t>(x);
        for (std::size_t index = 0; index < 4; ++index)
        {
            payload.push_back(static_cast<std::byte>((ux >> (8 * (3 - index))) & 0xFFU));
        }
        const auto uy = static_cast<std::uint32_t>(y);
        for (std::size_t index = 0; index < 4; ++index)
        {
            payload.push_back(static_cast<std::byte>((uy >> (8 * (3 - index))) & 0xFFU));
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Move,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    [[nodiscard]] snf::protocol::Frame leaveZoneFrame(const std::uint32_t request_id)
    {
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::LeaveZone,
            .request_id = request_id,
            .payload = {},
        };
    }

    [[nodiscard]] snf::protocol::Frame roomJoinFrame(const std::uint32_t request_id, const std::uint64_t room_id)
    {
        std::vector<std::byte> payload;
        payload.reserve(8);
        for (std::size_t index = 0; index < 8; ++index)
        {
            payload.push_back(static_cast<std::byte>((room_id >> (8 * (7 - index))) & 0xFFULL));
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::RoomJoin,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    [[nodiscard]] snf::protocol::Frame battleStartFrame(const std::uint32_t request_id, const std::uint64_t room_id)
    {
        std::vector<std::byte> payload;
        payload.reserve(8);
        for (std::size_t index = 0; index < 8; ++index)
        {
            payload.push_back(static_cast<std::byte>((room_id >> (8 * (7 - index))) & 0xFFULL));
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::BattleStart,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    [[nodiscard]] snf::protocol::Frame roomLeaveFrame(const std::uint32_t request_id)
    {
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::RoomLeave,
            .request_id = request_id,
            .payload = {},
        };
    }

    [[nodiscard]] snf::protocol::Frame useSkillFrame(
        const std::uint32_t request_id,
        const std::uint64_t room_id,
        const std::uint32_t skill_id,
        const std::uint64_t sequence
    )
    {
        std::vector<std::byte> payload;
        payload.reserve(20);
        for (std::size_t index = 0; index < 8; ++index)
        {
            payload.push_back(static_cast<std::byte>((room_id >> (8 * (7 - index))) & 0xFFULL));
        }
        for (std::size_t index = 0; index < 4; ++index)
        {
            payload.push_back(static_cast<std::byte>((skill_id >> (8 * (3 - index))) & 0xFFU));
        }
        for (std::size_t index = 0; index < 8; ++index)
        {
            payload.push_back(static_cast<std::byte>((sequence >> (8 * (7 - index))) & 0xFFULL));
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::UseSkill,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    [[nodiscard]] snf::protocol::Frame setMoveIntentFrame(
        const std::uint32_t request_id,
        const std::uint64_t room_id,
        const std::uint8_t direction,
        const std::uint64_t sequence
    )
    {
        std::vector<std::byte> payload;
        payload.reserve(17);
        for (std::size_t index = 0; index < 8; ++index)
        {
            payload.push_back(static_cast<std::byte>((room_id >> (8 * (7 - index))) & 0xFFULL));
        }
        payload.push_back(static_cast<std::byte>(direction));
        for (std::size_t index = 0; index < 8; ++index)
        {
            payload.push_back(static_cast<std::byte>((sequence >> (8 * (7 - index))) & 0xFFULL));
        }
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::SetMoveIntent,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    [[nodiscard]] std::size_t zoneReplyEncodedSize(const std::size_t visible_count = 0)
    {
        constexpr std::size_t FIXED_PAYLOAD = 27;
        return snf::protocol::encode_frame(snf::protocol::Frame{
                                               .type = snf::protocol::MessageType::ZoneEntered,
                                               .request_id = 0,
                                               .payload = std::vector<std::byte>(FIXED_PAYLOAD + visible_count * 8),
                                           })
            .size();
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

    [[nodiscard]] snf::protocol::Frame receiveDecodedFrame(const int descriptor)
    {
        const auto length_bytes = receiveExact(descriptor, snf::protocol::FRAME_LENGTH_FIELD_SIZE);
        const auto body_size = readBigEndian32(length_bytes, 0);
        const auto body_bytes = receiveExact(descriptor, body_size);

        std::vector<std::byte> frame_bytes;
        frame_bytes.reserve(snf::protocol::FRAME_LENGTH_FIELD_SIZE + body_size);
        frame_bytes.insert(frame_bytes.end(), length_bytes.begin(), length_bytes.end());
        frame_bytes.insert(frame_bytes.end(), body_bytes.begin(), body_bytes.end());

        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(frame_bytes);
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
        assert(harness.metrics().actor.actor_turns - harness.metrics().actor.lifecycle_turns - harness.metrics().actor.resumed_turns == 4);
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
        assert(
            harness.metrics().actor.actor_turns - harness.metrics().actor.lifecycle_turns - harness.metrics().actor.resumed_turns ==
            malformed.size() * 2
        );
    }

    // 27-byte zone frame layout round trip: status=Applied(0), zone id,
    // route_epoch == 1, signed x/y round trip. Moved and ZoneLeft keep epoch
    // unchanged.
    void test_enter_move_leave_round_trip()
    {
        SessionHarness harness;
        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 42);

        const auto size_0 = zoneReplyEncodedSize(0);

        // EnterZone with negative x (legacy test parity check)
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, -3, 42)));
        const auto entered = receiveFrame(client.getDescriptor(), size_0);
        assert(entered.type == snf::protocol::MessageType::ZoneEntered);
        assert(entered.request_id == 2);
        assert(entered.payload.size() == 27);
        assert(std::to_integer<std::uint8_t>(entered.payload[0]) == static_cast<std::uint8_t>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndian64(entered.payload, 1) == 100);
        assert(readBigEndian64(entered.payload, 9) == 1); // route_epoch == 1
        assert(readBigEndianSigned32(entered.payload, 17) == -3);
        assert(readBigEndianSigned32(entered.payload, 21) == 42);
        assert(readBigEndian16(entered.payload, 25) == 0); // visible_count == 0

        // Move
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(moveFrame(3, -10, 50)));
        const auto moved = receiveFrame(client.getDescriptor(), size_0);
        assert(moved.type == snf::protocol::MessageType::Moved);
        assert(moved.request_id == 3);
        assert(moved.payload.size() == 27);
        assert(std::to_integer<std::uint8_t>(moved.payload[0]) == static_cast<std::uint8_t>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndian64(moved.payload, 1) == 100);
        assert(readBigEndian64(moved.payload, 9) == 1); // epoch invariant
        assert(readBigEndianSigned32(moved.payload, 17) == -10);
        assert(readBigEndianSigned32(moved.payload, 21) == 50);
        assert(readBigEndian16(moved.payload, 25) == 0);

        // LeaveZone
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(leaveZoneFrame(4)));
        const auto left = receiveFrame(client.getDescriptor(), size_0);
        assert(left.type == snf::protocol::MessageType::ZoneLeft);
        assert(left.request_id == 4);
        assert(left.payload.size() == 27);
        assert(std::to_integer<std::uint8_t>(left.payload[0]) == static_cast<std::uint8_t>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndian64(left.payload, 1) == 100);
        assert(readBigEndian64(left.payload, 9) == 1); // epoch invariant
        assert(readBigEndianSigned32(left.payload, 17) == -10);
        assert(readBigEndianSigned32(left.payload, 21) == 50);
        assert(readBigEndian16(left.payload, 25) == 0);

        client.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    // Two players in the same zone within AOI radius: the second player's
    // response includes the first in visible_players. Crucially pins that there is
    // no broadcast: the first player receives no unsolicited frame.
    void test_aoi_visibility_and_no_broadcast()
    {
        SessionHarness harness;
        auto client1 = connectClient(harness.port());
        authenticate(client1.getDescriptor(), 1, 10);

        auto client2 = connectClient(harness.port());
        authenticate(client2.getDescriptor(), 1, 20);

        const auto size_0 = zoneReplyEncodedSize(0);
        const auto size_1 = zoneReplyEncodedSize(1);

        sendAll(client1.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 0, 0)));
        const auto entered1 = receiveFrame(client1.getDescriptor(), size_0);
        assert(entered1.type == snf::protocol::MessageType::ZoneEntered);
        assert(readBigEndian16(entered1.payload, 25) == 0);

        sendAll(client2.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 5, 5)));
        const auto entered2 = receiveFrame(client2.getDescriptor(), size_1);
        assert(entered2.type == snf::protocol::MessageType::ZoneEntered);
        assert(readBigEndian16(entered2.payload, 25) == 1);
        assert(readBigEndian64(entered2.payload, 27) == 10); // client1's player id

        // Confirm no broadcast was sent to client 1.
        std::this_thread::sleep_for(20ms);
        std::byte unexpected{};
        const ssize_t received = ::recv(client1.getDescriptor(), &unexpected, 1, MSG_DONTWAIT);
        assert(received == -1 && (errno == EAGAIN || errno == EWOULDBLOCK));

        client1.init();
        client2.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    // Re-entering the same zone is AlreadyPresent(1) with epoch and position unchanged.
    void test_same_zone_reenter_is_already_present_with_same_epoch()
    {
        SessionHarness harness;
        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 77);

        const auto size_0 = zoneReplyEncodedSize(0);

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 10, 20)));
        const auto first = receiveFrame(client.getDescriptor(), size_0);
        assert(std::to_integer<std::uint8_t>(first.payload[0]) == static_cast<std::uint8_t>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndian64(first.payload, 9) == 1);

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(3, 100, 99, 99)));
        const auto second = receiveFrame(client.getDescriptor(), size_0);
        assert(second.type == snf::protocol::MessageType::ZoneEntered);
        assert(second.request_id == 3);
        assert(std::to_integer<std::uint8_t>(second.payload[0]) == static_cast<std::uint8_t>(snf::server::ZoneCommandStatus::AlreadyPresent));
        assert(readBigEndian64(second.payload, 1) == 100);
        assert(readBigEndian64(second.payload, 9) == 1); // epoch unchanged
        // Existing position preserved by Zone
        assert(readBigEndianSigned32(second.payload, 17) == 10);
        assert(readBigEndianSigned32(second.payload, 21) == 20);

        client.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    // A source Move accepted before the handoff must drain first, then the target
    // route is published at the new epoch and remains usable through LeaveZone.
    void test_cross_zone_handoff_orders_source_move_and_serves_target()
    {
        MultiWorkerSessionHarness harness;
        const auto placement_seed = sessionActorConfig().placement_seed;
        std::uint64_t player = 70;
        while (snf::worker::ownerOf({snf::worker::ActorKind::Player, player}, 2, placement_seed) != snf::worker::WorkerId{0})
        {
            ++player;
        }
        std::uint64_t source_zone = 100;
        while (snf::worker::ownerOf({snf::worker::ActorKind::Zone, source_zone}, 2, placement_seed) != snf::worker::WorkerId{1})
        {
            ++source_zone;
        }
        std::uint64_t target_zone = source_zone + 1;
        while (snf::worker::ownerOf({snf::worker::ActorKind::Zone, target_zone}, 2, placement_seed) != snf::worker::WorkerId{1})
        {
            ++target_zone;
        }

        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, player);

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, source_zone, 10, 20)));
        const auto first = receiveDecodedFrame(client.getDescriptor());
        assert(std::to_integer<std::uint8_t>(first.payload[0]) == static_cast<std::uint8_t>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndian64(first.payload, 1) == source_zone);
        assert(readBigEndian64(first.payload, 9) == 1);

        const auto source_move = snf::protocol::encode_frame(moveFrame(3, 15, 25));
        const auto target_enter = snf::protocol::encode_frame(enterZoneFrame(4, target_zone, 30, 40));
        std::vector<std::byte> pipelined;
        pipelined.insert(pipelined.end(), source_move.begin(), source_move.end());
        pipelined.insert(pipelined.end(), target_enter.begin(), target_enter.end());
        sendAll(client.getDescriptor(), pipelined);

        const auto moved_source = receiveDecodedFrame(client.getDescriptor());
        assert(moved_source.type == snf::protocol::MessageType::Moved);
        assert(moved_source.request_id == 3);
        assert(moved_source.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndian64(moved_source.payload, 1) == source_zone);
        assert(readBigEndian64(moved_source.payload, 9) == 1);
        assert(readBigEndianSigned32(moved_source.payload, 17) == 15);
        assert(readBigEndianSigned32(moved_source.payload, 21) == 25);

        const auto entered_target = receiveDecodedFrame(client.getDescriptor());
        assert(entered_target.type == snf::protocol::MessageType::ZoneEntered);
        assert(entered_target.request_id == 4);
        assert(entered_target.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndian64(entered_target.payload, 1) == target_zone);
        assert(readBigEndian64(entered_target.payload, 9) == 2);
        assert(readBigEndianSigned32(entered_target.payload, 17) == 30);
        assert(readBigEndianSigned32(entered_target.payload, 21) == 40);

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(moveFrame(5, 35, 45)));
        const auto moved_target = receiveDecodedFrame(client.getDescriptor());
        assert(moved_target.type == snf::protocol::MessageType::Moved);
        assert(moved_target.request_id == 5);
        assert(moved_target.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndian64(moved_target.payload, 1) == target_zone);
        assert(readBigEndian64(moved_target.payload, 9) == 2);

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(leaveZoneFrame(6)));
        const auto left_target = receiveDecodedFrame(client.getDescriptor());
        assert(left_target.type == snf::protocol::MessageType::ZoneLeft);
        assert(left_target.request_id == 6);
        assert(left_target.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndian64(left_target.payload, 1) == target_zone);
        assert(readBigEndian64(left_target.payload, 9) == 2);

        client.init();
        const auto deadline = Clock::now() + 2s;
        while (harness.sessionCount() != 0 && Clock::now() < deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(harness.sessionCount() == 0);
        harness.stop();
        assert(harness.remoteTellsSent() > 0);
    }

    // Move or LeaveZone without an active zone closes the connection.
    void test_move_and_leave_without_current_zone_closes_connection()
    {
        SessionHarness harness;

        // Move without zone
        {
            auto client = connectClient(harness.port());
            authenticate(client.getDescriptor(), 1, 11);
            sendAll(client.getDescriptor(), snf::protocol::encode_frame(moveFrame(2, 10, 20)));
            assert(receivesEof(client.getDescriptor()));
            waitForSessionsReleased(harness);
        }

        // LeaveZone without zone
        {
            auto client = connectClient(harness.port());
            authenticate(client.getDescriptor(), 1, 12);
            sendAll(client.getDescriptor(), snf::protocol::encode_frame(leaveZoneFrame(2)));
            assert(receivesEof(client.getDescriptor()));
            waitForSessionsReleased(harness);
        }

        harness.stop();
        assert(harness.metrics().network.invariant_violations == 0);
    }

    // Zone id 0 must be rejected by PlayerActor before telling ZoneActor,
    // avoiding downstream throw and invariant violation.
    void test_enter_zone_zero_closes_connection_without_worker_exception()
    {
        SessionHarness harness;
        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 33);

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 0, 1, 1)));
        assert(receivesEof(client.getDescriptor()));

        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.invariant_violations == 0);
    }

    // Malformed payloads for zone commands are protocol errors and never reach an actor.
    void test_malformed_zone_payloads_never_reach_an_actor()
    {
        SessionHarness harness;

        const std::vector<snf::protocol::Frame> malformed{
            // EnterZone: too short (15), too long (17), empty (0).
            snf::protocol::Frame{.type = snf::protocol::MessageType::EnterZone, .request_id = 1, .payload = std::vector<std::byte>(15)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::EnterZone, .request_id = 1, .payload = std::vector<std::byte>(17)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::EnterZone, .request_id = 1, .payload = {}},
            // Move: too short (7), too long (9), empty (0).
            snf::protocol::Frame{.type = snf::protocol::MessageType::Move, .request_id = 1, .payload = std::vector<std::byte>(7)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::Move, .request_id = 1, .payload = std::vector<std::byte>(9)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::Move, .request_id = 1, .payload = {}},
            // LeaveZone: non-empty payload.
            snf::protocol::Frame{.type = snf::protocol::MessageType::LeaveZone, .request_id = 1, .payload = {std::byte{0x01}}},
        };

        for (const auto& frame : malformed)
        {
            auto client = connectClient(harness.port());
            authenticate(client.getDescriptor(), 1, 55);
            sendAll(client.getDescriptor(), snf::protocol::encode_frame(frame));
            assert(receivesEof(client.getDescriptor()));
            waitForSessionsReleased(harness);
        }

        harness.stop();
        assert(harness.metrics().network.protocol_errors == malformed.size());
        assert(harness.metrics().network.sent_frames == malformed.size());
        assert(
            harness.metrics().actor.actor_turns - harness.metrics().actor.lifecycle_turns - harness.metrics().actor.resumed_turns ==
            malformed.size() * 2
        );
    }

    // When a connection closes, an implicit LeaveZone is dispatched to the zone.
    // Verified by checking that a remaining player's AOI no longer sees the disconnected player.
    void test_implicit_leave_on_disconnect_removes_player_from_zone()
    {
        SessionHarness harness;
        auto client1 = connectClient(harness.port());
        authenticate(client1.getDescriptor(), 1, 100);

        auto client2 = connectClient(harness.port());
        authenticate(client2.getDescriptor(), 1, 200);

        const auto size_0 = zoneReplyEncodedSize(0);
        const auto size_1 = zoneReplyEncodedSize(1);

        sendAll(client1.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 0, 0)));
        const auto entered1 = receiveFrame(client1.getDescriptor(), size_0);
        assert(entered1.type == snf::protocol::MessageType::ZoneEntered);

        sendAll(client2.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 10, 10)));
        const auto entered2 = receiveFrame(client2.getDescriptor(), size_1);
        assert(readBigEndian16(entered2.payload, 25) == 1);
        assert(readBigEndian64(entered2.payload, 27) == 100);

        // Disconnect client 1
        client1.init();

        // Wait until client 1 session is released in sink
        const auto release_deadline = Clock::now() + 2s;
        while (harness.sink().sessionCount() != 1 && Clock::now() < release_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(harness.sink().sessionCount() == 1);

        // Poll Move until the ZoneActor has processed the implicit LeaveZone and visible_count drops to 0.
        // The polling deadline is separated from the session release wait to prevent cascaded timeout exhaustion.
        const auto aoi_deadline = Clock::now() + 2s;
        std::uint16_t visible_count = 1;
        std::uint32_t move_request_id = 3;
        while (Clock::now() < aoi_deadline)
        {
            sendAll(client2.getDescriptor(), snf::protocol::encode_frame(moveFrame(move_request_id, 11, 11)));
            const auto moved = receiveDecodedFrame(client2.getDescriptor());
            assert(moved.type == snf::protocol::MessageType::Moved);
            assert(moved.request_id == move_request_id);
            visible_count = readBigEndian16(moved.payload, 25);
            if (visible_count == 0)
            {
                break;
            }
            ++move_request_id;
        }
        assert(visible_count == 0);

        client2.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    void test_reconnect_restores_last_zone_position()
    {
        SessionHarness harness;

        auto client1 = connectClient(harness.port());
        authenticate(client1.getDescriptor(), 1, 200);

        const auto size_0 = zoneReplyEncodedSize(0);

        // Enter zone 100 at position (10, 20)
        sendAll(client1.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 10, 20)));
        const auto entered1 = receiveFrame(client1.getDescriptor(), size_0);
        assert(entered1.type == snf::protocol::MessageType::ZoneEntered);
        assert(entered1.request_id == 2);
        assert(readBigEndianSigned32(entered1.payload, 17) == 10);
        assert(readBigEndianSigned32(entered1.payload, 21) == 20);

        // Move to (55, 66)
        sendAll(client1.getDescriptor(), snf::protocol::encode_frame(moveFrame(3, 55, 66)));
        const auto moved = receiveFrame(client1.getDescriptor(), size_0);
        assert(moved.type == snf::protocol::MessageType::Moved);
        assert(moved.request_id == 3);
        assert(readBigEndianSigned32(moved.payload, 17) == 55);
        assert(readBigEndianSigned32(moved.payload, 21) == 66);

        // Disconnect client 1
        client1.init();

        // Wait until session is released
        const auto release_deadline = Clock::now() + 2s;
        while (harness.sink().sessionCount() != 0 && Clock::now() < release_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(harness.sink().sessionCount() == 0);

        // Reconnect client as player 200
        auto client1_reconnected = connectClient(harness.port());
        authenticate(client1_reconnected.getDescriptor(), 1, 200);

        // Client requests enter zone 100 with dummy position (0, 0)
        sendAll(client1_reconnected.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(4, 100, 0, 0)));
        const auto re_entered = receiveFrame(client1_reconnected.getDescriptor(), size_0);
        assert(re_entered.type == snf::protocol::MessageType::ZoneEntered);
        assert(re_entered.request_id == 4);
        assert(re_entered.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
        // Location R1: position restored to (55, 66), not (0, 0)
        assert(readBigEndianSigned32(re_entered.payload, 17) == 55);
        assert(readBigEndianSigned32(re_entered.payload, 21) == 66);

        client1_reconnected.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    void test_room_join_and_leave_over_session()
    {
        SessionHarness harness;

        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 301);

        const auto size_0 = zoneReplyEncodedSize(0);

        // Enter zone 100 at position (10, 20)
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 10, 20)));
        const auto entered = receiveFrame(client.getDescriptor(), size_0);
        assert(entered.type == snf::protocol::MessageType::ZoneEntered);
        assert(entered.request_id == 2);

        // Join room 1
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(roomJoinFrame(3, 1)));
        const auto joined = receiveDecodedFrame(client.getDescriptor());
        assert(joined.type == snf::protocol::MessageType::RoomJoined);
        assert(joined.request_id == 3);
        assert(joined.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));
        assert(joined.payload[1] == static_cast<std::byte>(snf::server::RoomPhase::Waiting));
        assert(readBigEndian64(joined.payload, 2) == 1);

        // Send Move while in room: receives InRoom status, connection kept alive
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(moveFrame(4, 50, 50)));
        const auto moved_in_room = receiveDecodedFrame(client.getDescriptor());
        assert(moved_in_room.type == snf::protocol::MessageType::Moved);
        assert(moved_in_room.request_id == 4);
        assert(moved_in_room.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::InRoom));

        // Leave room
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(roomLeaveFrame(5)));
        const auto returned = receiveDecodedFrame(client.getDescriptor());
        assert(returned.type == snf::protocol::MessageType::ReturnedToZone);
        assert(readBigEndian64(returned.payload, 0) == 100);
        assert(readBigEndianSigned32(returned.payload, 8) == 10);
        assert(readBigEndianSigned32(returned.payload, 12) == 20);

        // Can move in zone 100 again!
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(moveFrame(6, 15, 25)));
        const auto moved_after = receiveFrame(client.getDescriptor(), size_0);
        assert(moved_after.type == snf::protocol::MessageType::Moved);
        assert(moved_after.request_id == 6);
        assert(moved_after.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndianSigned32(moved_after.payload, 17) == 15);
        assert(readBigEndianSigned32(moved_after.payload, 21) == 25);

        client.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    [[nodiscard]] snf::protocol::Frame receiveNextNonDigestFrame(const int descriptor)
    {
        while (true)
        {
            auto frame = receiveDecodedFrame(descriptor);
            if (frame.type != snf::protocol::MessageType::BattleDigest)
            {
                return frame;
            }
        }
    }

    void test_battle_lifecycle_over_session()
    {
        SessionHarness harness;

        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 302);

        const auto size_0 = zoneReplyEncodedSize(0);

        // Enter zone 100
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 10, 20)));
        const auto entered = receiveFrame(client.getDescriptor(), size_0);
        assert(entered.type == snf::protocol::MessageType::ZoneEntered);

        // Join room 2
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(roomJoinFrame(3, 2)));
        const auto joined = receiveNextNonDigestFrame(client.getDescriptor());
        assert(joined.type == snf::protocol::MessageType::RoomJoined);
        assert(joined.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));

        // Start battle
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(battleStartFrame(4, 2)));
        const auto started = receiveNextNonDigestFrame(client.getDescriptor());
        assert(started.type == snf::protocol::MessageType::BattleStarted);
        assert(started.request_id == 4);
        assert(started.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));
        assert(started.payload[1] == static_cast<std::byte>(snf::server::RoomPhase::Running));

        // Use skill
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(useSkillFrame(5, 2, snf::server::SLASH_SKILL_ID.value, 1)));
        const auto skill_ack = receiveNextNonDigestFrame(client.getDescriptor());
        assert(skill_ack.type == snf::protocol::MessageType::SkillAcknowledged);
        assert(skill_ack.request_id == 5);
        assert(skill_ack.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));

        // Set move intent
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(setMoveIntentFrame(6, 2, static_cast<std::uint8_t>(snf::server::MoveDirection::North), 1)));
        const auto move_ack = receiveNextNonDigestFrame(client.getDescriptor());
        assert(move_ack.type == snf::protocol::MessageType::MoveAcknowledged);
        assert(move_ack.request_id == 6);
        assert(move_ack.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));

        // Leave room while battle is active
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(roomLeaveFrame(7)));
        const auto returned = receiveNextNonDigestFrame(client.getDescriptor());
        assert(returned.type == snf::protocol::MessageType::ReturnedToZone);
        assert(readBigEndian64(returned.payload, 0) == 100);

        client.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    void test_boss_defeat_or_timeout_returns_to_zone_over_session()
    {
        snf::server::RoomConfig config;
        config.wave_count = 1;
        config.battle_duration = std::chrono::milliseconds{100};
        config.boss_spawn_after = std::chrono::milliseconds{50};
        config.tick_interval = std::chrono::milliseconds{20};
        config.wave_interval = std::chrono::milliseconds{5000};
        SessionHarness harness(config);

        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 303);

        const auto size_0 = zoneReplyEncodedSize(0);

        // Enter zone 100 at position (10, 20)
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 10, 20)));
        const auto entered = receiveFrame(client.getDescriptor(), size_0);
        assert(entered.type == snf::protocol::MessageType::ZoneEntered);
        assert(entered.request_id == 2);

        // Join room 3
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(roomJoinFrame(3, 3)));
        const auto joined = receiveDecodedFrame(client.getDescriptor());
        assert(joined.type == snf::protocol::MessageType::RoomJoined);
        assert(joined.request_id == 3);
        assert(joined.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));

        // Start battle
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(battleStartFrame(4, 3)));
        const auto started = receiveDecodedFrame(client.getDescriptor());
        assert(started.type == snf::protocol::MessageType::BattleStarted);
        assert(started.request_id == 4);
        assert(started.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));

        // Wait for battle terminal outcome and returned to zone frame
        const auto timeout_deadline = Clock::now() + 5s;
        bool got_battle_terminal = false;
        bool got_returned_to_zone = false;
        while (Clock::now() < timeout_deadline && (!got_battle_terminal || !got_returned_to_zone))
        {
            const auto frame = receiveDecodedFrame(client.getDescriptor());
            if (frame.type == snf::protocol::MessageType::BattleDigest)
            {
                continue;
            }
            if (frame.type == snf::protocol::MessageType::BattleFailed || frame.type == snf::protocol::MessageType::BattleCleared)
            {
                got_battle_terminal = true;
            }
            else if (frame.type == snf::protocol::MessageType::ReturnedToZone)
            {
                got_returned_to_zone = true;
                assert(readBigEndian64(frame.payload, 0) == 100);
                assert(readBigEndianSigned32(frame.payload, 8) == 10);
                assert(readBigEndianSigned32(frame.payload, 12) == 20);
            }
        }
        assert(got_battle_terminal);
        assert(got_returned_to_zone);

        client.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    void test_room_join_zero_closes_connection()
    {
        SessionHarness harness;

        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 304);

        const auto size_0 = zoneReplyEncodedSize(0);

        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 100, 10, 20)));
        const auto entered = receiveFrame(client.getDescriptor(), size_0);
        assert(entered.type == snf::protocol::MessageType::ZoneEntered);

        // Join room 0 -> PlayerActor closes connection gracefully
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(roomJoinFrame(3, 0)));
        assert(receivesEof(client.getDescriptor()));

        client.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
    }

    void test_malformed_room_payloads_rejected()
    {
        SessionHarness harness;

        const std::vector<snf::protocol::Frame> malformed{
            // RoomJoin: too short (7), too long (9), empty (0).
            snf::protocol::Frame{.type = snf::protocol::MessageType::RoomJoin, .request_id = 1, .payload = std::vector<std::byte>(7)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::RoomJoin, .request_id = 1, .payload = std::vector<std::byte>(9)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::RoomJoin, .request_id = 1, .payload = {}},
            // BattleStart: too short (7), too long (9), empty (0).
            snf::protocol::Frame{.type = snf::protocol::MessageType::BattleStart, .request_id = 1, .payload = std::vector<std::byte>(7)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::BattleStart, .request_id = 1, .payload = std::vector<std::byte>(9)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::BattleStart, .request_id = 1, .payload = {}},
            // RoomLeave: non-empty payload.
            snf::protocol::Frame{.type = snf::protocol::MessageType::RoomLeave, .request_id = 1, .payload = {std::byte{0x01}}},
            // UseSkill: too short (19), too long (21), empty (0).
            snf::protocol::Frame{.type = snf::protocol::MessageType::UseSkill, .request_id = 1, .payload = std::vector<std::byte>(19)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::UseSkill, .request_id = 1, .payload = std::vector<std::byte>(21)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::UseSkill, .request_id = 1, .payload = {}},
            // SetMoveIntent: too short (16), too long (18), empty (0), invalid direction (99).
            snf::protocol::Frame{.type = snf::protocol::MessageType::SetMoveIntent, .request_id = 1, .payload = std::vector<std::byte>(16)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::SetMoveIntent, .request_id = 1, .payload = std::vector<std::byte>(18)},
            snf::protocol::Frame{.type = snf::protocol::MessageType::SetMoveIntent, .request_id = 1, .payload = {}},
            snf::protocol::Frame{.type = snf::protocol::MessageType::SetMoveIntent, .request_id = 1, .payload = std::vector<std::byte>(17, std::byte{99})},
        };

        for (const auto& frame : malformed)
        {
            auto client = connectClient(harness.port());
            authenticate(client.getDescriptor(), 1, 56);
            sendAll(client.getDescriptor(), snf::protocol::encode_frame(frame));
            assert(receivesEof(client.getDescriptor()));
            waitForSessionsReleased(harness);
        }

        harness.stop();
        assert(harness.metrics().network.protocol_errors == malformed.size());
    }

    void test_pipelined_zone_moves_over_session()
    {
        SessionHarness harness;
        auto client = connectClient(harness.port());
        authenticate(client.getDescriptor(), 1, 57);

        // Enter Zone 1
        sendAll(client.getDescriptor(), snf::protocol::encode_frame(enterZoneFrame(2, 1, 10, 20)));
        const auto entered = receiveDecodedFrame(client.getDescriptor());
        assert(entered.type == snf::protocol::MessageType::ZoneEntered);
        assert(entered.request_id == 2);
        assert(entered.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));

        // Pipeline two moves in a single TCP write
        const auto move1 = snf::protocol::encode_frame(moveFrame(3, 11, 21));
        const auto move2 = snf::protocol::encode_frame(moveFrame(4, 12, 22));
        std::vector<std::byte> combined;
        combined.insert(combined.end(), move1.begin(), move1.end());
        combined.insert(combined.end(), move2.begin(), move2.end());
        sendAll(client.getDescriptor(), combined);

        // Read both replies back
        const auto reply1 = receiveDecodedFrame(client.getDescriptor());
        assert(reply1.type == snf::protocol::MessageType::Moved);
        assert(reply1.request_id == 3);
        assert(reply1.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndianSigned32(reply1.payload, 17) == 11);
        assert(readBigEndianSigned32(reply1.payload, 21) == 21);

        const auto reply2 = receiveDecodedFrame(client.getDescriptor());
        assert(reply2.type == snf::protocol::MessageType::Moved);
        assert(reply2.request_id == 4);
        assert(reply2.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
        assert(readBigEndianSigned32(reply2.payload, 17) == 12);
        assert(readBigEndianSigned32(reply2.payload, 21) == 22);

        client.init();
        waitForSessionsReleased(harness);
        harness.stop();
        assert(harness.metrics().network.protocol_errors == 0);
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
    SNF_RUN_SESSION_TEST(test_enter_move_leave_round_trip);
    SNF_RUN_SESSION_TEST(test_aoi_visibility_and_no_broadcast);
    SNF_RUN_SESSION_TEST(test_same_zone_reenter_is_already_present_with_same_epoch);
    SNF_RUN_SESSION_TEST(test_cross_zone_handoff_orders_source_move_and_serves_target);
    SNF_RUN_SESSION_TEST(test_move_and_leave_without_current_zone_closes_connection);
    SNF_RUN_SESSION_TEST(test_enter_zone_zero_closes_connection_without_worker_exception);
    SNF_RUN_SESSION_TEST(test_malformed_zone_payloads_never_reach_an_actor);
    SNF_RUN_SESSION_TEST(test_implicit_leave_on_disconnect_removes_player_from_zone);
    SNF_RUN_SESSION_TEST(test_reconnect_restores_last_zone_position);
    SNF_RUN_SESSION_TEST(test_room_join_and_leave_over_session);
    SNF_RUN_SESSION_TEST(test_battle_lifecycle_over_session);
    SNF_RUN_SESSION_TEST(test_boss_defeat_or_timeout_returns_to_zone_over_session);
    SNF_RUN_SESSION_TEST(test_room_join_zero_closes_connection);
    SNF_RUN_SESSION_TEST(test_malformed_room_payloads_rejected);
    SNF_RUN_SESSION_TEST(test_pipelined_zone_moves_over_session);
}
