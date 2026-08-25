#include "snf/game/arena.hpp"
#include "snf/game/skill_id.hpp"
#include "snf/net/socket_options.hpp"
#include "snf/net/termination_signal.hpp"
#include "snf/net/unique_file_descriptor.hpp"
#include "snf/protocol/frame_codec.hpp"
#include "snf/runtime/runtime_completion.hpp"
#include "snf/server/game_server.hpp"

#include <arpa/inet.h>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <functional>
#include <future>
#include <iostream>
#include <mutex>
#include <numeric>
#include <optional>
#include <poll.h>
#include <span>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <unordered_map>
#include <utility>
#include <vector>

import snf.game.skill_catalog;

namespace
{
    using namespace std::chrono_literals;

    class RecordingFrameIngress final : public snf::server::FrameIngress
    {
    public:
        [[nodiscard]] snf::server::FramePostResult tryPost(snf::server::FrameEnvelope) override
        {
            return snf::server::FramePostResult::Closed;
        }

        [[nodiscard]] snf::server::PostResult tryPostConnectionClosed(snf::server::ConnectionClosed closed) override
        {
            {
                std::lock_guard lock{connection_closes_mutex};
                connection_closes.push_back(closed);
            }
            const std::size_t attempt = lifecycle_attempts.fetch_add(1);
            return attempt < lifecycle_results.size() ? lifecycle_results[attempt] : lifecycle_fallback;
        }

        void close() noexcept override
        {
            closed = true;
        }

        void cancel() noexcept override
        {
            cancelled = true;
        }

        [[nodiscard]] std::size_t distinctConnectionCloseCount() const
        {
            std::lock_guard lock{connection_closes_mutex};
            std::size_t distinct = 0;
            for (std::size_t index = 0; index < connection_closes.size(); ++index)
            {
                bool seen = false;
                for (std::size_t previous = 0; previous < index; ++previous)
                {
                    if (connection_closes[previous].connection == connection_closes[index].connection)
                    {
                        seen = true;
                        break;
                    }
                }
                if (!seen)
                {
                    ++distinct;
                }
            }
            return distinct;
        }

        bool closed{false};
        bool cancelled{false};
        std::vector<snf::server::PostResult> lifecycle_results;
        snf::server::PostResult lifecycle_fallback{snf::server::PostResult::Accepted};
        mutable std::mutex connection_closes_mutex;
        std::vector<snf::server::ConnectionClosed> connection_closes;
        std::atomic<std::size_t> lifecycle_attempts{0};
    };

    snf::net::UniqueFileDescriptor make_eventfd()
    {
        const int descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        assert(descriptor != -1);
        return snf::net::UniqueFileDescriptor{descriptor};
    }

    class ControlledSaveRepository final : public snf::server::PlayerRepository, public snf::server::PlayerRepositoryDiagnostics
    {
    public:
        ControlledSaveRepository()
            : _watchdog(
                  [this](const std::stop_token stop_token) noexcept
                  {
                      runWatchdog(stop_token);
                  }
              )
        {
        }

        ~ControlledSaveRepository() override
        {
            _watchdog.request_stop();
            _changed.notify_all();
        }

        void asyncLoad(const snf::server::PlayerId player, snf::server::PlayerLoadCompletion completion) override
        {
            snf::server::PlayerLoadResult result;
            {
                std::lock_guard lock{_mutex};
                ++_accepted;
                if (const auto record = _records.find(player); record != _records.end())
                {
                    result.record = record->second;
                }
            }
            completion(std::move(result));
        }

        void asyncSave(snf::server::PlayerRecord record, snf::server::PlayerSaveCompletion completion) override
        {
            {
                std::lock_guard lock{_mutex};
                ++_accepted;
                if (_hold_first_save)
                {
                    _hold_first_save = false;
                    _pending_record = std::move(record);
                    _pending_save = std::move(completion);
                    _changed.notify_all();
                    return;
                }
                _records.insert_or_assign(record.player, std::move(record));
            }
            completion(successfulSave());
        }

        [[nodiscard]] std::optional<snf::server::PlayerRecord> find(const snf::server::PlayerId player) const override
        {
            std::lock_guard lock{_mutex};
            const auto record = _records.find(player);
            return record == _records.end() ? std::nullopt : std::optional{record->second};
        }

        [[nodiscard]] snf::server::PlayerRepositoryStats stats() const override
        {
            std::lock_guard lock{_mutex};
            return snf::server::PlayerRepositoryStats{
                .accepted = _accepted,
                .rejected = 0,
                .queue_depth = _pending_save ? 1U : 0U,
                .queue_high_water_mark = _hold_first_save ? 0U : 1U,
                .operation_failures = 0,
                .operation_latency_nanoseconds = {},
            };
        }

        [[nodiscard]] bool waitForPendingSave(const std::chrono::milliseconds timeout)
        {
            std::unique_lock lock{_mutex};
            return _changed.wait_for(
                lock,
                timeout,
                [this]
                {
                    return static_cast<bool>(_pending_save);
                }
            );
        }

        [[nodiscard]] bool releasePendingSave()
        {
            snf::server::PlayerSaveCompletion completion;
            {
                std::lock_guard lock{_mutex};
                if (!_pending_record || !_pending_save)
                {
                    return false;
                }
                _records.insert_or_assign(_pending_record->player, std::move(*_pending_record));
                _pending_record.reset();
                completion = std::move(_pending_save);
                _changed.notify_all();
            }
            completion(successfulSave());
            return true;
        }

        [[nodiscard]] std::uint64_t watchdogReleaseCount() const noexcept
        {
            return _watchdog_releases.load(std::memory_order_acquire);
        }

    private:
        [[nodiscard]] static snf::server::PlayerSaveResult successfulSave() noexcept
        {
            return snf::server::PlayerSaveResult{
                .status = snf::server::PlayerRepositoryStatus::Success,
            };
        }

        void runWatchdog(const std::stop_token stop_token) noexcept
        {
            snf::server::PlayerSaveCompletion completion;
            {
                std::unique_lock lock{_mutex};
                if (!_changed.wait(
                        lock,
                        stop_token,
                        [this]
                        {
                            return static_cast<bool>(_pending_save);
                        }
                    ))
                {
                    return;
                }
                if (_changed.wait_for(
                        lock,
                        stop_token,
                        5s,
                        [this]
                        {
                            return !_pending_save;
                        }
                    ))
                {
                    return;
                }
                if (!_pending_record || !_pending_save)
                {
                    return;
                }
                _records.insert_or_assign(_pending_record->player, std::move(*_pending_record));
                _pending_record.reset();
                completion = std::move(_pending_save);
                _watchdog_releases.fetch_add(1, std::memory_order_release);
            }

            try
            {
                completion(successfulSave());
            }
            catch (...)
            {
            }
        }

        mutable std::mutex _mutex;
        std::condition_variable_any _changed;
        std::unordered_map<snf::server::PlayerId, snf::server::PlayerRecord, snf::server::PlayerIdHash> _records;
        std::uint64_t _accepted{0};
        bool _hold_first_save{true};
        std::optional<snf::server::PlayerRecord> _pending_record;
        snf::server::PlayerSaveCompletion _pending_save;
        std::atomic<std::uint64_t> _watchdog_releases{0};
        std::jthread _watchdog;
    };

    class RunningServer
    {
    public:
        explicit RunningServer(const int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD)
            : RunningServer(
                  snf::server::GameServerConfig{
                      .port = 0,
                      .shutdown_grace_period = 200ms,
                      .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                      .client_send_buffer_size = std::nullopt,
                  },
                  termination_signal_descriptor
              )
        {
        }

        explicit RunningServer(
            snf::server::GameServerConfig config, const int termination_signal_descriptor = snf::net::UniqueFileDescriptor::INVALID_FD
        )
            : _server(config)
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
                          try
                          {
                              std::rethrow_exception(_server_error);
                          }
                          catch (const std::exception& error)
                          {
                              std::cerr << "[server] run() threw: " << error.what() << '\n';
                          }
                          catch (...)
                          {
                              std::cerr << "[server] run() threw a non-standard exception\n";
                          }
                      }
                  }
              )
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

        [[nodiscard]] const snf::server::GameServerStats& getStats() const noexcept
        {
            return _server.getStats();
        }

        [[nodiscard]] snf::runtime::ActorRuntimeStats getActorRuntimeStats() const
        {
            return _server.getActorRuntimeStats();
        }

        [[nodiscard]] std::optional<snf::server::PlayerRecord> getPlayerRecord(const snf::server::PlayerId player) const
        {
            return _server.getPlayerRecord(player);
        }

        [[nodiscard]] snf::server::ZoneActorBindingStats getZoneActorStats() const noexcept
        {
            return _server.getZoneActorStats();
        }

        [[nodiscard]] snf::server::PlayerActorBindingStats getPlayerActorStats() const noexcept
        {
            return _server.getPlayerActorStats();
        }

        [[nodiscard]] snf::server::RoomActorBindingStats getRoomActorStats() const noexcept
        {
            return _server.getRoomActorStats();
        }

        [[nodiscard]] snf::server::ServerMetricsSnapshot getMetricsSnapshot() const
        {
            return _server.getMetricsSnapshot();
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
        snf::server::GameServer _server;
        int _termination_signal_descriptor;
        std::exception_ptr _server_error;
        std::thread _thread;
    };

    snf::net::UniqueFileDescriptor connect_client(const std::uint16_t port, const std::optional<int> receive_buffer_size = std::nullopt)
    {
        snf::net::UniqueFileDescriptor client_socket{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
        assert(client_socket.isValid());

        timeval timeout{
            .tv_sec = 2,
            .tv_usec = 0,
        };

        assert(::setsockopt(client_socket.getDescriptor(), SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);

        if (receive_buffer_size)
        {
            assert(::setsockopt(client_socket.getDescriptor(), SOL_SOCKET, SO_RCVBUF, &*receive_buffer_size, sizeof(*receive_buffer_size)) == 0);
        }
        assert(::setsockopt(client_socket.getDescriptor(), SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);

        snf::net::enable_tcp_no_delay(client_socket.getDescriptor());

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(port);
        assert(::inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) == 1);

        assert(::connect(client_socket.getDescriptor(), reinterpret_cast<const sockaddr*>(&server_address), sizeof(server_address)) == 0);

        return client_socket;
    }

    void send_all(const int socket_descriptor, const std::span<const std::byte> bytes)
    {
        std::size_t sent_byte_count = 0;

        while (sent_byte_count < bytes.size())
        {
            const auto result = ::send(socket_descriptor, bytes.data() + sent_byte_count, bytes.size() - sent_byte_count, MSG_NOSIGNAL);

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

    void send_until_complete_or_closed(const int socket_descriptor, const std::span<const std::byte> bytes)
    {
        std::size_t sent_byte_count = 0;

        while (sent_byte_count < bytes.size())
        {
            const auto result = ::send(socket_descriptor, bytes.data() + sent_byte_count, bytes.size() - sent_byte_count, MSG_NOSIGNAL);

            if (result > 0)
            {
                sent_byte_count += static_cast<std::size_t>(result);
                continue;
            }

            if (result == -1 && errno == EINTR)
            {
                continue;
            }

            if (result == -1 && (errno == EPIPE || errno == ECONNRESET))
            {
                return;
            }

            assert(false);
        }
    }

    std::vector<std::byte> receive_exact(const int socket_descriptor, const std::size_t expected_byte_count)
    {
        std::vector<std::byte> received_bytes(expected_byte_count);
        std::size_t received_byte_count = 0;

        while (received_byte_count < expected_byte_count)
        {
            const auto result = ::recv(socket_descriptor, received_bytes.data() + received_byte_count, expected_byte_count - received_byte_count, 0);

            if (result > 0)
            {
                received_byte_count += static_cast<std::size_t>(result);
                continue;
            }

            if (result == -1 && errno == EINTR)
            {
                continue;
            }

            std::cerr << "[recv] fd " << socket_descriptor << " wanted " << expected_byte_count << ", got " << received_byte_count << ", result "
                      << result << ", errno " << errno << " (" << std::strerror(errno) << ")\n";
            assert(false && "receive_exact: recv failed or timed out");
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

    std::vector<std::byte> player_id_payload(const std::uint64_t value)
    {
        std::vector<std::byte> payload(8);
        std::uint64_t remaining = value;
        for (std::size_t index = payload.size(); index > 0; --index)
        {
            payload[index - 1] = static_cast<std::byte>(remaining & 0xFFU);
            remaining >>= 8U;
        }
        return payload;
    }

    void append_u32(std::vector<std::byte>& payload, const std::uint32_t value)
    {
        payload.push_back(static_cast<std::byte>((value >> 24U) & 0xFFU));
        payload.push_back(static_cast<std::byte>((value >> 16U) & 0xFFU));
        payload.push_back(static_cast<std::byte>((value >> 8U) & 0xFFU));
        payload.push_back(static_cast<std::byte>(value & 0xFFU));
    }

    void append_u64(std::vector<std::byte>& payload, const std::uint64_t value)
    {
        append_u32(payload, static_cast<std::uint32_t>(value >> 32U));
        append_u32(payload, static_cast<std::uint32_t>(value));
    }

    std::uint32_t read_u32(const std::vector<std::byte>& payload, const std::size_t offset)
    {
        return (std::to_integer<std::uint32_t>(payload[offset]) << 24U) | (std::to_integer<std::uint32_t>(payload[offset + 1]) << 16U) |
               (std::to_integer<std::uint32_t>(payload[offset + 2]) << 8U) | std::to_integer<std::uint32_t>(payload[offset + 3]);
    }

    std::uint16_t read_u16(const std::vector<std::byte>& payload, const std::size_t offset)
    {
        return static_cast<std::uint16_t>(
            (std::to_integer<std::uint16_t>(payload[offset]) << 8U) | std::to_integer<std::uint16_t>(payload[offset + 1])
        );
    }

    std::uint64_t read_u64(const std::vector<std::byte>& payload, const std::size_t offset)
    {
        return (static_cast<std::uint64_t>(read_u32(payload, offset)) << 32U) | read_u32(payload, offset + 4);
    }

    snf::protocol::Frame authentication_frame(const std::uint32_t request_id, const std::uint64_t player_id)
    {
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Authenticate,
            .request_id = request_id,
            .payload = player_id_payload(player_id),
        };
    }

    snf::protocol::Frame purchase_frame(const std::uint32_t request_id, const std::uint64_t idempotency_key, const std::uint32_t product = 1)
    {
        std::vector<std::byte> payload;
        append_u64(payload, idempotency_key);
        append_u32(payload, product);
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::Purchase,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    snf::protocol::Frame room_frame(const snf::protocol::MessageType type, const std::uint32_t request_id, const std::uint64_t room)
    {
        return snf::protocol::Frame{
            .type = type,
            .request_id = request_id,
            .payload = player_id_payload(room),
        };
    }

    snf::protocol::Frame set_move_intent_frame(
        const std::uint32_t request_id, const std::uint64_t room, const snf::server::MoveDirection direction, const std::uint64_t sequence
    )
    {
        std::vector<std::byte> payload = player_id_payload(room);
        payload.push_back(static_cast<std::byte>(direction));
        append_u64(payload, sequence);
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::SetMoveIntent,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    snf::protocol::Frame
    use_skill_frame(const std::uint32_t request_id, const std::uint64_t room, const snf::server::SkillId skill, const std::uint64_t sequence)
    {
        std::vector<std::byte> payload = player_id_payload(room);
        append_u32(payload, skill.value);
        append_u64(payload, sequence);
        return snf::protocol::Frame{
            .type = snf::protocol::MessageType::UseSkill,
            .request_id = request_id,
            .payload = std::move(payload),
        };
    }

    snf::protocol::Frame receive_room_frame(const int socket_descriptor, const std::size_t payload_size)
    {
        const std::size_t frame_size = snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE + payload_size;
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(receive_exact(socket_descriptor, frame_size));
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        return decoded.frames[0];
    }

    snf::protocol::Frame receive_frame(const int socket_descriptor)
    {
        std::vector<std::byte> encoded = receive_exact(socket_descriptor, snf::protocol::FRAME_LENGTH_FIELD_SIZE);
        const std::size_t body_size = read_u32(encoded, 0);
        std::vector<std::byte> body = receive_exact(socket_descriptor, body_size);
        encoded.insert(encoded.end(), body.begin(), body.end());

        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(encoded);
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        return decoded.frames.front();
    }

    [[nodiscard]] std::size_t battle_event_size(const snf::server::BattleEventKind kind)
    {
        switch (kind)
        {
        case snf::server::BattleEventKind::EnemySpawned:
            return 1 + 4 + 1 + 8;
        case snf::server::BattleEventKind::EnemyDamaged:
            return 1 + 4 + 8 + 4 + 8 + 8;
        case snf::server::BattleEventKind::EnemyDied:
            return 1 + 4;
        case snf::server::BattleEventKind::SkillWhiffed:
            return 1 + 8 + 4;
        case snf::server::BattleEventKind::ArenaStarted:
            return 1 + 4 + 4;
        case snf::server::BattleEventKind::ParticipantSpawned:
            return 1 + 8 + 4 + 4 + 8;
        case snf::server::BattleEventKind::ParticipantMoved:
            return 1 + 8 + 4 + 4;
        case snf::server::BattleEventKind::EnemyPositioned:
            return 1 + 4 + 4 + 4;
        case snf::server::BattleEventKind::ParticipantDamaged:
            return 1 + 8 + 4 + 8 + 8;
        case snf::server::BattleEventKind::ParticipantDied:
        case snf::server::BattleEventKind::ParticipantLeft:
            return 1 + 8;
        }
        assert(false && "unknown BattleEventKind");
        return 0;
    }

    [[nodiscard]] std::vector<std::pair<snf::server::BattleEventKind, std::size_t>> digest_events(const snf::protocol::Frame& frame)
    {
        assert(frame.type == snf::protocol::MessageType::BattleDigest);
        assert(frame.payload.size() >= 11);
        const std::size_t event_count = read_u16(frame.payload, 9);
        std::size_t offset = 11;
        std::vector<std::pair<snf::server::BattleEventKind, std::size_t>> events;
        events.reserve(event_count);
        for (std::size_t index = 0; index < event_count; ++index)
        {
            assert(offset < frame.payload.size());
            const auto kind = static_cast<snf::server::BattleEventKind>(std::to_integer<std::uint8_t>(frame.payload[offset]));
            const std::size_t size = battle_event_size(kind);
            assert(size <= frame.payload.size() - offset);
            events.emplace_back(kind, offset);
            offset += size;
        }
        assert(offset == frame.payload.size());
        return events;
    }

    [[nodiscard]] std::optional<std::size_t> digest_event_offset(const snf::protocol::Frame& frame, const snf::server::BattleEventKind wanted)
    {
        if (frame.type != snf::protocol::MessageType::BattleDigest)
        {
            return std::nullopt;
        }
        for (const auto& [kind, offset] : digest_events(frame))
        {
            if (kind == wanted)
            {
                return offset;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] snf::protocol::Frame receive_until(const int socket_descriptor, const std::function<bool(const snf::protocol::Frame&)>& wanted)
    {
        constexpr std::size_t MAX_SKIPPED_FRAMES = 1024;
        for (std::size_t index = 0; index < MAX_SKIPPED_FRAMES; ++index)
        {
            snf::protocol::Frame frame = receive_frame(socket_descriptor);
            if (wanted(frame))
            {
                return frame;
            }
        }
        assert(false && "wanted frame did not arrive");
        return snf::protocol::Frame{};
    }

    [[nodiscard]] snf::protocol::Frame receive_until_type(
        const int socket_descriptor, const snf::protocol::MessageType type, const std::optional<std::uint32_t> request_id = std::nullopt
    )
    {
        return receive_until(
            socket_descriptor,
            [type, request_id](const snf::protocol::Frame& frame)
            {
                return frame.type == type && (!request_id || frame.request_id == *request_id);
            }
        );
    }

    [[nodiscard]] snf::protocol::Frame receive_until_digest_event(const int socket_descriptor, const snf::server::BattleEventKind kind)
    {
        return receive_until(
            socket_descriptor,
            [kind](const snf::protocol::Frame& frame)
            {
                return digest_event_offset(frame, kind).has_value();
            }
        );
    }

    void assert_authenticated(const std::vector<std::byte>& response, const std::uint32_t request_id, const std::uint64_t player_id)
    {
        snf::protocol::FrameDecoder decoder;
        const auto result = decoder.append(response);

        assert(result.ok());
        assert(result.frames.size() == 1);
        assert(result.frames[0].type == snf::protocol::MessageType::Authenticated);
        assert(result.frames[0].request_id == request_id);
        assert(result.frames[0].payload == player_id_payload(player_id));
    }

    snf::protocol::Frame receive_zone_response(const int socket_descriptor)
    {
        constexpr std::size_t ZONE_RESPONSE_PAYLOAD_SIZE = 27;
        constexpr std::size_t ZONE_RESPONSE_FRAME_SIZE =
            snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE + ZONE_RESPONSE_PAYLOAD_SIZE;
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(receive_exact(socket_descriptor, ZONE_RESPONSE_FRAME_SIZE));
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        return decoded.frames.front();
    }

    snf::protocol::Frame receive_purchase_response(const int socket_descriptor)
    {
        constexpr std::size_t PURCHASE_RESPONSE_PAYLOAD_SIZE = 30;
        constexpr std::size_t PURCHASE_RESPONSE_FRAME_SIZE =
            snf::protocol::FRAME_LENGTH_FIELD_SIZE + snf::protocol::MIN_BODY_SIZE + PURCHASE_RESPONSE_PAYLOAD_SIZE;
        snf::protocol::FrameDecoder decoder;
        const auto decoded = decoder.append(receive_exact(socket_descriptor, PURCHASE_RESPONSE_FRAME_SIZE));
        assert(decoded.ok());
        assert(decoded.frames.size() == 1);
        return decoded.frames.front();
    }

    void assert_purchase_response(
        const snf::protocol::Frame& response,
        const std::uint32_t request_id,
        const snf::server::PurchaseStatus status,
        const bool replayed,
        const std::uint64_t key,
        const std::uint32_t product,
        const std::uint64_t balance,
        const std::uint64_t item_count
    )
    {
        assert(response.type == snf::protocol::MessageType::PurchaseResult);
        assert(response.request_id == request_id);
        assert(response.payload.size() == 30);
        assert(std::to_integer<std::uint8_t>(response.payload[0]) == static_cast<std::uint8_t>(status));
        assert(std::to_integer<std::uint8_t>(response.payload[1]) == (replayed ? 1 : 0));
        assert(read_u64(response.payload, 2) == key);
        assert(read_u32(response.payload, 10) == product);
        assert(read_u64(response.payload, 14) == balance);
        assert(read_u64(response.payload, 22) == item_count);
    }

    void receive_until_closed(const int socket_descriptor)
    {
        std::array<std::byte, 65536> receive_buffer{};

        while (true)
        {
            const auto result = ::recv(socket_descriptor, receive_buffer.data(), receive_buffer.size(), 0);

            if (result > 0)
            {
                continue;
            }

            if (result == 0 || (result == -1 && errno == ECONNRESET))
            {
                return;
            }

            if (result == -1 && errno == EINTR)
            {
                continue;
            }

            assert(false);
        }
    }

    std::size_t actor_count(const snf::runtime::ActorRuntimeStats& stats)
    {
        return std::accumulate(
            stats.workers.begin(),
            stats.workers.end(),
            std::size_t{0},
            [](const std::size_t total, const snf::runtime::ActorRuntimeWorkerStats& worker)
            {
                return total + worker.actor_count;
            }
        );
    }

    std::uint64_t evicted_actor_count(const snf::runtime::ActorRuntimeStats& stats)
    {
        return std::accumulate(
            stats.workers.begin(),
            stats.workers.end(),
            std::uint64_t{0},
            [](const std::uint64_t total, const snf::runtime::ActorRuntimeWorkerStats& worker)
            {
                return total + worker.evicted_actors;
            }
        );
    }

    std::uint64_t queue_wait_sample_count(const snf::runtime::ActorRuntimeStats& stats)
    {
        return std::accumulate(
            stats.workers.begin(),
            stats.workers.end(),
            std::uint64_t{0},
            [](const std::uint64_t total, const snf::runtime::ActorRuntimeWorkerStats& worker)
            {
                return total + worker.queue_wait_nanoseconds.sample_count;
            }
        );
    }

    std::uint64_t suspended_command_count(const snf::runtime::ActorRuntimeStats& stats)
    {
        return std::accumulate(
            stats.workers.begin(),
            stats.workers.end(),
            std::uint64_t{0},
            [](const std::uint64_t total, const snf::runtime::ActorRuntimeWorkerStats& worker)
            {
                return total + worker.suspended_commands;
            }
        );
    }

    void test_saturated_outbound_answers_every_request_and_still_drains()
    {
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 2s,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
            .actor_worker_count = 1,
            .actor_queue_capacity_per_worker = 64,
            .outbound_queue_capacity = 1,
        }};
        const auto client = connect_client(server.getPort());

        constexpr std::uint32_t REQUEST_COUNT = 8;
        std::vector<std::byte> bundled_requests;
        for (std::uint32_t request_id = 1; request_id <= REQUEST_COUNT; ++request_id)
        {
            const auto encoded = snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::Ping,
                .request_id = request_id,
                .payload = {},
            });
            bundled_requests.insert(bundled_requests.end(), encoded.begin(), encoded.end());
        }

        send_all(client.getDescriptor(), bundled_requests);

        for (std::uint32_t request_id = 1; request_id <= REQUEST_COUNT; ++request_id)
        {
            const snf::protocol::Frame request{
                .type = snf::protocol::MessageType::Ping,
                .request_id = request_id,
                .payload = {},
            };
            assert_pong(receive_exact(client.getDescriptor(), snf::protocol::encode_frame(request).size()), request);
        }

        server.stop();

        const auto metrics = server.getMetricsSnapshot();
        assert(metrics.counters.outbound_admission_failures == 0);
        assert(metrics.counters.actor_queue_overflows == 0);
        assert(metrics.network.pending_outbound_reservations == 0);
        assert(metrics.network.reserved_outbound_slots == 0);
        assert(metrics.network.current_outbound_queue_depth == 0);
        assert(actor_count(server.getActorRuntimeStats()) == 0);
        assert(suspended_command_count(server.getActorRuntimeStats()) > 0);
        assert(metrics.command_terminals == REQUEST_COUNT);
    }

    void test_collects_baseline_saturation_metrics_for_a_round_trip()
    {
        RunningServer server;
        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 1,
            .payload = {std::byte{0x01}},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);
        send_all(client.getDescriptor(), encoded_request);
        assert_pong(receive_exact(client.getDescriptor(), encoded_request.size()), request);

        server.stop();

        const auto metrics = server.getMetricsSnapshot();
        assert(metrics.counters.received_frames == 1);
        assert(metrics.network.reactor_turn_nanoseconds.sample_count > 0);
        assert(metrics.network.reactor_turn_nanoseconds.max >= metrics.network.reactor_turn_nanoseconds.p99);
        assert(metrics.network.outbound_queue_depth.sample_count > 0);
        assert(metrics.network.session_pending_send_bytes.sample_count == 1);
        assert(metrics.network.outbound_queue_wait_nanoseconds.sample_count == 1);
        assert(metrics.network.outbound_queue_wait_nanoseconds.max > 0);
        assert(metrics.network.session_pending_send_bytes.max == encoded_request.size());
        assert(metrics.network.outbound_queue_high_water_mark >= 1);
        assert(metrics.network.session_count == 0);
        assert(metrics.network.sessions_with_pending_send == 0);
        assert(metrics.network.total_pending_send_bytes == 0);
        assert(queue_wait_sample_count(metrics.actor_runtime) == 1);
        assert(metrics.command_terminals == 1);
        assert(suspended_command_count(metrics.actor_runtime) == 0);
        assert(metrics.network.reserved_outbound_slots == 0);
        assert(metrics.network.pending_outbound_reservations == 0);
    }

    void test_reports_metrics_periodically_while_running()
    {
        std::atomic<std::size_t> report_count{0};
        std::atomic<std::uint64_t> reported_reactor_turns{0};
        std::atomic<std::size_t> reported_sessions{0};

        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .metrics_report_interval = 5ms,
            .metrics_reporter =
                [&](const snf::server::ServerMetricsSnapshot& metrics)
            {
                reported_reactor_turns.store(metrics.network.reactor_turn_nanoseconds.sample_count);
                reported_sessions.store(metrics.network.session_count);
                report_count.fetch_add(1);
            },
        }};

        const auto client = connect_client(server.getPort());

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 7,
            .payload = {std::byte{0x02}},
        };

        const auto encoded_request = snf::protocol::encode_frame(request);
        send_all(client.getDescriptor(), encoded_request);
        assert_pong(receive_exact(client.getDescriptor(), encoded_request.size()), request);

        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (report_count.load() < 3 && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(1ms);
        }

        assert(report_count.load() >= 3);
        assert(reported_reactor_turns.load() > 0);
        assert(reported_sessions.load() == 1);

        server.stop();
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

        const auto& stats = server.getStats();
        assert(stats.accepted_connections == 1);
        assert(stats.closed_connections == 1);
        assert(stats.received_frames == 1);
        assert(stats.sent_frames == 1);
        assert(stats.protocol_errors == 0);
    }

    void test_authenticates_one_session_and_allows_reconnect_after_passivation()
    {
        RunningServer server;
        constexpr std::uint64_t player_id = 77;

        auto first = connect_client(server.getPort());
        const auto first_auth = authentication_frame(100, player_id);
        const auto first_auth_bytes = snf::protocol::encode_frame(first_auth);
        send_all(first.getDescriptor(), first_auth_bytes);
        assert_authenticated(receive_exact(first.getDescriptor(), first_auth_bytes.size()), first_auth.request_id, player_id);

        const auto ping = snf::protocol::Frame{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 101,
            .payload = {std::byte{0xAA}},
        };
        const auto ping_bytes = snf::protocol::encode_frame(ping);
        send_all(first.getDescriptor(), ping_bytes);
        assert_pong(receive_exact(first.getDescriptor(), ping_bytes.size()), ping);

        const auto duplicate = connect_client(server.getPort());
        const auto duplicate_auth = authentication_frame(102, player_id);
        send_all(duplicate.getDescriptor(), snf::protocol::encode_frame(duplicate_auth));
        receive_until_closed(duplicate.getDescriptor());

        first.init();
        const auto passivation_deadline = std::chrono::steady_clock::now() + 1s;
        while (actor_count(server.getActorRuntimeStats()) != 0 && std::chrono::steady_clock::now() < passivation_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(actor_count(server.getActorRuntimeStats()) == 0);
        const auto first_saved = server.getPlayerRecord(snf::server::PlayerId{.value = player_id});
        assert(first_saved.has_value());
        assert(first_saved->handled_command_count == 2);

        auto reconnected = connect_client(server.getPort());
        const auto reconnect_auth = authentication_frame(103, player_id);
        const auto reconnect_bytes = snf::protocol::encode_frame(reconnect_auth);
        send_all(reconnected.getDescriptor(), reconnect_bytes);
        assert_authenticated(receive_exact(reconnected.getDescriptor(), reconnect_bytes.size()), reconnect_auth.request_id, player_id);

        const auto restored_ping = snf::protocol::Frame{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 104,
            .payload = {std::byte{0xBB}},
        };
        const auto restored_ping_bytes = snf::protocol::encode_frame(restored_ping);
        send_all(reconnected.getDescriptor(), restored_ping_bytes);
        assert_pong(receive_exact(reconnected.getDescriptor(), restored_ping_bytes.size()), restored_ping);

        reconnected.init();
        const auto second_passivation_deadline = std::chrono::steady_clock::now() + 1s;
        while (actor_count(server.getActorRuntimeStats()) != 0 && std::chrono::steady_clock::now() < second_passivation_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(actor_count(server.getActorRuntimeStats()) == 0);
        const auto second_saved = server.getPlayerRecord(snf::server::PlayerId{.value = player_id});
        assert(second_saved.has_value());
        assert(second_saved->handled_command_count == 4);

        server.stop();
    }

    void test_reconnect_waits_while_the_previous_session_is_closing()
    {
        ControlledSaveRepository* repository = nullptr;
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
            .player_repository_factory =
                [&repository]
            {
                auto controlled = std::make_unique<ControlledSaveRepository>();
                repository = controlled.get();
                return controlled;
            },
        }};
        assert(repository != nullptr);
        constexpr std::uint64_t player_id = 78;
        const snf::server::PlayerId player{.value = player_id};

        auto first = connect_client(server.getPort());
        const auto first_auth = authentication_frame(105, player_id);
        const auto first_auth_bytes = snf::protocol::encode_frame(first_auth);
        send_all(first.getDescriptor(), first_auth_bytes);
        assert_authenticated(receive_exact(first.getDescriptor(), first_auth_bytes.size()), first_auth.request_id, player_id);

        const auto first_ping = snf::protocol::Frame{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 106,
            .payload = {std::byte{0xAC}},
        };
        const auto first_ping_bytes = snf::protocol::encode_frame(first_ping);
        send_all(first.getDescriptor(), first_ping_bytes);
        assert_pong(receive_exact(first.getDescriptor(), first_ping_bytes.size()), first_ping);

        first.init();
        assert(repository->waitForPendingSave(1s));
        assert(actor_count(server.getActorRuntimeStats()) == 1);

        const auto blocked = connect_client(server.getPort());
        const auto blocked_auth = authentication_frame(107, player_id);
        send_all(blocked.getDescriptor(), snf::protocol::encode_frame(blocked_auth));
        receive_until_closed(blocked.getDescriptor());
        assert(actor_count(server.getActorRuntimeStats()) == 1);

        assert(repository->releasePendingSave());
        const auto first_passivation_deadline = std::chrono::steady_clock::now() + 1s;
        while (actor_count(server.getActorRuntimeStats()) != 0 && std::chrono::steady_clock::now() < first_passivation_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(actor_count(server.getActorRuntimeStats()) == 0);

        auto reconnected = connect_client(server.getPort());
        const auto reconnect_auth = authentication_frame(108, player_id);
        const auto reconnect_auth_bytes = snf::protocol::encode_frame(reconnect_auth);
        send_all(reconnected.getDescriptor(), reconnect_auth_bytes);
        assert_authenticated(receive_exact(reconnected.getDescriptor(), reconnect_auth_bytes.size()), reconnect_auth.request_id, player_id);

        const auto restored_ping = snf::protocol::Frame{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 109,
            .payload = {std::byte{0xAD}},
        };
        const auto restored_ping_bytes = snf::protocol::encode_frame(restored_ping);
        send_all(reconnected.getDescriptor(), restored_ping_bytes);
        assert_pong(receive_exact(reconnected.getDescriptor(), restored_ping_bytes.size()), restored_ping);

        reconnected.init();
        const auto second_passivation_deadline = std::chrono::steady_clock::now() + 1s;
        while (actor_count(server.getActorRuntimeStats()) != 0 && std::chrono::steady_clock::now() < second_passivation_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(actor_count(server.getActorRuntimeStats()) == 0);
        const auto saved = server.getPlayerRecord(player);
        assert(saved.has_value());
        assert(saved->handled_command_count == 4);
        assert(repository->watchdogReleaseCount() == 0);

        server.stop();
    }

    void test_live_purchase_is_memory_authoritative_and_flushes()
    {
        RunningServer server;
        constexpr std::uint64_t player_id = 79;
        const snf::server::PlayerId player{.value = player_id};

        auto client = connect_client(server.getPort());
        const auto auth = authentication_frame(110, player_id);
        const auto auth_bytes = snf::protocol::encode_frame(auth);
        send_all(client.getDescriptor(), auth_bytes);
        assert_authenticated(receive_exact(client.getDescriptor(), auth_bytes.size()), auth.request_id, player_id);

        const auto first = purchase_frame(111, 1);
        send_all(client.getDescriptor(), snf::protocol::encode_frame(first));
        assert_purchase_response(
            receive_purchase_response(client.getDescriptor()), first.request_id, snf::server::PurchaseStatus::Committed, false, 1, 1, 900, 1
        );

        const auto duplicate = purchase_frame(112, 1);
        send_all(client.getDescriptor(), snf::protocol::encode_frame(duplicate));
        assert_purchase_response(
            receive_purchase_response(client.getDescriptor()), duplicate.request_id, snf::server::PurchaseStatus::Committed, true, 1, 1, 900, 1
        );

        const auto lost_response = purchase_frame(113, 2);
        send_all(client.getDescriptor(), snf::protocol::encode_frame(lost_response));
        client.init();

        const auto commit_deadline = std::chrono::steady_clock::now() + 1s;
        std::optional<snf::server::PlayerRecord> committed;
        while (std::chrono::steady_clock::now() < commit_deadline)
        {
            committed = server.getPlayerRecord(player);
            if (committed && committed->currency_balance == 800 && committed->purchased_item_count == 2)
            {
                break;
            }
            std::this_thread::sleep_for(1ms);
        }
        assert(committed.has_value());
        assert(committed->currency_balance == 800);
        assert(committed->purchased_item_count == 2);

        const auto passivation_deadline = std::chrono::steady_clock::now() + 1s;
        while (actor_count(server.getActorRuntimeStats()) != 0 && std::chrono::steady_clock::now() < passivation_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(actor_count(server.getActorRuntimeStats()) == 0);

        auto reconnected = connect_client(server.getPort());
        const auto reconnect_auth = authentication_frame(114, player_id);
        const auto reconnect_auth_bytes = snf::protocol::encode_frame(reconnect_auth);
        send_all(reconnected.getDescriptor(), reconnect_auth_bytes);
        assert_authenticated(receive_exact(reconnected.getDescriptor(), reconnect_auth_bytes.size()), reconnect_auth.request_id, player_id);

        const auto retry = purchase_frame(115, 2);
        send_all(reconnected.getDescriptor(), snf::protocol::encode_frame(retry));
        assert_purchase_response(
            receive_purchase_response(reconnected.getDescriptor()), retry.request_id, snf::server::PurchaseStatus::Committed, false, 2, 1, 700, 3
        );

        const auto unknown = purchase_frame(118, 4, 999);
        send_all(reconnected.getDescriptor(), snf::protocol::encode_frame(unknown));
        assert_purchase_response(
            receive_purchase_response(reconnected.getDescriptor()),
            unknown.request_id,
            snf::server::PurchaseStatus::ProductNotFound,
            false,
            4,
            999,
            700,
            3
        );

        reconnected.init();
        server.stop();
        const auto metrics = server.getMetricsSnapshot();
        assert(metrics.player_persistence.snapshots_accepted >= 1);
        assert(metrics.player_persistence.saves_succeeded >= 1);
        assert(metrics.player_persistence.final_saves >= 1);
        const auto saved = server.getPlayerRecord(player);
        assert(saved.has_value());
        assert(saved->currency_balance == 700);
        assert(saved->purchased_item_count == 3);
    }

    void test_two_players_kill_a_boss_and_are_told_without_asking()
    {
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
            .room_battle_duration = 2s,
            .room_clear_experience = 300,
            .room_boss_health = snf::server::BASE_ATTACK,
            .room_tick_interval = 5ms,
            .room_wave_interval = 100ms,
            .room_wave_count = 1,
            .room_minions_per_wave = 1,
            .room_minion_health = snf::server::BASE_ATTACK,
            .room_boss_spawn_after = 200ms,
            .max_room_spawned_enemies = 2,
            .room_arena_width = 20,
            .room_arena_height = 20,
            .room_participant_spawn_spacing = 2,
            .room_minion_spawn_radius = 5,
        }};
        constexpr std::uint64_t room = 77;
        constexpr std::uint64_t zone = 88;
        constexpr std::uint64_t first_player = 610;
        constexpr std::uint64_t second_player = 611;

        auto first = connect_client(server.getPort());
        auto second = connect_client(server.getPort());
        for (const auto& [socket, player, request_id] :
             {std::tuple{std::ref(first), first_player, std::uint32_t{300}}, std::tuple{std::ref(second), second_player, std::uint32_t{301}}})
        {
            const auto frame = authentication_frame(request_id, player);
            const auto bytes = snf::protocol::encode_frame(frame);
            send_all(socket.get().getDescriptor(), bytes);
            assert_authenticated(receive_exact(socket.get().getDescriptor(), bytes.size()), request_id, player);
        }

        for (const auto& [socket, request_id, y] :
             {std::tuple{std::ref(first), std::uint32_t{310}, std::uint32_t{20}},
              std::tuple{std::ref(second), std::uint32_t{311}, std::uint32_t{5000}}})
        {
            std::vector<std::byte> enter_payload = player_id_payload(zone);
            append_u32(enter_payload, 10);
            append_u32(enter_payload, y);
            send_all(
                socket.get().getDescriptor(),
                snf::protocol::encode_frame(snf::protocol::Frame{
                    .type = snf::protocol::MessageType::EnterZone,
                    .request_id = request_id,
                    .payload = std::move(enter_payload),
                })
            );
            const auto entered = receive_zone_response(socket.get().getDescriptor());
            assert(entered.type == snf::protocol::MessageType::ZoneEntered);
            assert(entered.request_id == request_id);
            assert(entered.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
            assert(read_u64(entered.payload, 1) == zone);
            assert(read_u64(entered.payload, 9) == 1);
        }

        constexpr std::size_t ROOM_REPLY_PAYLOAD_SIZE = 1 + 1 + 8;
        for (const auto& [socket, request_id] : {std::pair{std::ref(first), std::uint32_t{302}}, std::pair{std::ref(second), std::uint32_t{303}}})
        {
            send_all(socket.get().getDescriptor(), snf::protocol::encode_frame(room_frame(snf::protocol::MessageType::RoomJoin, request_id, room)));
            const auto joined = receive_room_frame(socket.get().getDescriptor(), ROOM_REPLY_PAYLOAD_SIZE);
            assert(joined.type == snf::protocol::MessageType::RoomJoined);
            assert(joined.request_id == request_id);
            assert(joined.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));
            assert(joined.payload[1] == static_cast<std::byte>(snf::server::RoomPhase::Waiting));
        }

        send_all(
            first.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::Move,
                .request_id = 312,
                .payload =
                    []
                {
                    std::vector<std::byte> payload;
                    append_u32(payload, 99);
                    append_u32(payload, 99);
                    return payload;
                }(),
            })
        );
        const auto refused_move = receive_zone_response(first.getDescriptor());
        assert(refused_move.type == snf::protocol::MessageType::Moved);
        assert(refused_move.request_id == 312);
        assert(refused_move.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::InRoom));

        send_all(first.getDescriptor(), snf::protocol::encode_frame(room_frame(snf::protocol::MessageType::BattleStart, 304, room)));
        const auto started = receive_room_frame(first.getDescriptor(), ROOM_REPLY_PAYLOAD_SIZE);
        assert(started.type == snf::protocol::MessageType::BattleStarted);
        assert(started.request_id == 304);
        assert(started.payload[1] == static_cast<std::byte>(snf::server::RoomPhase::Running));

        const auto first_wave = receive_frame(first.getDescriptor());
        const auto observed_first_wave = receive_frame(second.getDescriptor());
        assert(first_wave.type == snf::protocol::MessageType::BattleDigest);
        assert(first_wave.request_id == snf::protocol::UNSOLICITED_REQUEST_ID);
        assert(first_wave.payload == observed_first_wave.payload);
        assert(first_wave.payload[8] == static_cast<std::byte>(snf::server::RoomPhase::Running));
        assert(read_u16(first_wave.payload, 9) == 5);
        const auto arena_offset = digest_event_offset(first_wave, snf::server::BattleEventKind::ArenaStarted);
        const auto first_enemy_offset = digest_event_offset(first_wave, snf::server::BattleEventKind::EnemySpawned);
        const auto first_position_offset = digest_event_offset(first_wave, snf::server::BattleEventKind::EnemyPositioned);
        assert(arena_offset && read_u32(first_wave.payload, *arena_offset + 1) == 20 && read_u32(first_wave.payload, *arena_offset + 5) == 20);
        assert(first_enemy_offset && read_u32(first_wave.payload, *first_enemy_offset + 1) == 1);
        assert(first_wave.payload[*first_enemy_offset + 5] == static_cast<std::byte>(snf::server::EnemyKind::Minion));
        assert(read_u64(first_wave.payload, *first_enemy_offset + 6) == snf::server::BASE_ATTACK);
        assert(first_position_offset && read_u32(first_wave.payload, *first_position_offset + 1) == 1);

        send_all(first.getDescriptor(), snf::protocol::encode_frame(set_move_intent_frame(313, room, snf::server::MoveDirection::NorthEast, 1)));
        const auto move_ack = receive_until_type(first.getDescriptor(), snf::protocol::MessageType::MoveAcknowledged, 313);
        assert(move_ack.payload == (std::vector<std::byte>{std::byte{0}, std::byte{1}}));
        const auto movement = receive_until_digest_event(first.getDescriptor(), snf::server::BattleEventKind::ParticipantMoved);
        const auto observed_movement = receive_until_digest_event(second.getDescriptor(), snf::server::BattleEventKind::ParticipantMoved);
        assert(movement.payload == observed_movement.payload);
        send_all(first.getDescriptor(), snf::protocol::encode_frame(set_move_intent_frame(314, room, snf::server::MoveDirection::Stop, 2)));
        static_cast<void>(receive_until_type(first.getDescriptor(), snf::protocol::MessageType::MoveAcknowledged, 314));

        const auto use_skill_frame = [](const std::uint32_t request_id, const std::uint64_t sequence)
        {
            std::vector<std::byte> payload = player_id_payload(room);
            append_u32(payload, snf::server::SLASH.value);
            append_u64(payload, sequence);
            return snf::protocol::Frame{
                .type = snf::protocol::MessageType::UseSkill,
                .request_id = request_id,
                .payload = std::move(payload),
            };
        };

        send_all(first.getDescriptor(), snf::protocol::encode_frame(use_skill_frame(305, 1)));
        const auto first_ack = receive_until_type(first.getDescriptor(), snf::protocol::MessageType::SkillAcknowledged, 305);
        assert(first_ack.type == snf::protocol::MessageType::SkillAcknowledged);
        assert(first_ack.request_id == 305);
        assert(
            (first_ack.payload ==
             std::vector<std::byte>{
                 static_cast<std::byte>(snf::server::RoomCommandStatus::Applied),
                 static_cast<std::byte>(snf::server::RoomPhase::Running),
             })
        );

        const auto minion_killed = receive_until_digest_event(first.getDescriptor(), snf::server::BattleEventKind::EnemyDied);
        const auto observed_minion_killed = receive_until_digest_event(second.getDescriptor(), snf::server::BattleEventKind::EnemyDied);
        assert(minion_killed.type == snf::protocol::MessageType::BattleDigest);
        assert(minion_killed.payload == observed_minion_killed.payload);
        assert(read_u16(minion_killed.payload, 9) == 2);
        const auto minion_damage_offset = digest_event_offset(minion_killed, snf::server::BattleEventKind::EnemyDamaged);
        const auto minion_death_offset = digest_event_offset(minion_killed, snf::server::BattleEventKind::EnemyDied);
        assert(minion_damage_offset && read_u32(minion_killed.payload, *minion_damage_offset + 1) == 1);
        assert(read_u64(minion_killed.payload, *minion_damage_offset + 5) == first_player);
        assert(read_u64(minion_killed.payload, *minion_damage_offset + 17) == snf::server::BASE_ATTACK);
        assert(read_u64(minion_killed.payload, *minion_damage_offset + 25) == 0);
        assert(minion_death_offset && read_u32(minion_killed.payload, *minion_death_offset + 1) == 1);

        const auto boss_spawned = receive_until_digest_event(first.getDescriptor(), snf::server::BattleEventKind::EnemySpawned);
        const auto observed_boss_spawned = receive_until_digest_event(second.getDescriptor(), snf::server::BattleEventKind::EnemySpawned);
        assert(boss_spawned.type == snf::protocol::MessageType::BattleDigest);
        assert(boss_spawned.payload == observed_boss_spawned.payload);
        assert(read_u16(boss_spawned.payload, 9) == 2);
        const auto boss_spawn_offset = digest_event_offset(boss_spawned, snf::server::BattleEventKind::EnemySpawned);
        assert(boss_spawn_offset && read_u32(boss_spawned.payload, *boss_spawn_offset + 1) == 2);
        assert(boss_spawned.payload[*boss_spawn_offset + 5] == static_cast<std::byte>(snf::server::EnemyKind::Boss));

        send_all(second.getDescriptor(), snf::protocol::encode_frame(use_skill_frame(307, 1)));
        const auto killing_ack = receive_until_type(second.getDescriptor(), snf::protocol::MessageType::SkillAcknowledged, 307);
        assert(killing_ack.type == snf::protocol::MessageType::SkillAcknowledged);
        assert(killing_ack.request_id == 307);
        assert(killing_ack.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));
        assert(killing_ack.payload[1] == static_cast<std::byte>(snf::server::RoomPhase::Cleared));

        const auto killing_digest = receive_until_digest_event(second.getDescriptor(), snf::server::BattleEventKind::EnemyDied);
        const auto observed_killing_digest = receive_until_digest_event(first.getDescriptor(), snf::server::BattleEventKind::EnemyDied);
        assert(killing_digest.type == snf::protocol::MessageType::BattleDigest);
        assert(killing_digest.payload == observed_killing_digest.payload);
        assert(killing_digest.payload[8] == static_cast<std::byte>(snf::server::RoomPhase::Cleared));
        assert(read_u16(killing_digest.payload, 9) == 2);
        const auto boss_death_offset = digest_event_offset(killing_digest, snf::server::BattleEventKind::EnemyDied);
        assert(boss_death_offset && read_u32(killing_digest.payload, *boss_death_offset + 1) == 2);

        constexpr std::size_t RETURN_PAYLOAD_SIZE = 8 + 4 + 4;
        for (const auto& [socket, y] : {std::pair{std::ref(first), std::int32_t{20}}, std::pair{std::ref(second), std::int32_t{5000}}})
        {
            const auto cleared = receive_until_type(socket.get().getDescriptor(), snf::protocol::MessageType::BattleCleared);
            assert(cleared.type == snf::protocol::MessageType::BattleCleared);
            assert(cleared.request_id == snf::protocol::UNSOLICITED_REQUEST_ID);
            assert(cleared.payload == player_id_payload(300));

            const auto returned = receive_room_frame(socket.get().getDescriptor(), RETURN_PAYLOAD_SIZE);
            assert(returned.type == snf::protocol::MessageType::ReturnedToZone);
            assert(returned.request_id == snf::protocol::UNSOLICITED_REQUEST_ID);
            assert(read_u64(returned.payload, 0) == zone);
            assert(static_cast<std::int32_t>(read_u32(returned.payload, 8)) == 10);
            assert(static_cast<std::int32_t>(read_u32(returned.payload, 12)) == y);
        }

        for (const auto& [socket, request_id, y] :
             {std::tuple{std::ref(first), std::uint32_t{320}, std::int32_t{21}}, std::tuple{std::ref(second), std::uint32_t{321}, std::int32_t{5001}}
             })
        {
            std::vector<std::byte> move_payload;
            append_u32(move_payload, 11);
            append_u32(move_payload, static_cast<std::uint32_t>(y));
            send_all(
                socket.get().getDescriptor(),
                snf::protocol::encode_frame(snf::protocol::Frame{
                    .type = snf::protocol::MessageType::Move,
                    .request_id = request_id,
                    .payload = std::move(move_payload),
                })
            );
            const auto moved = receive_zone_response(socket.get().getDescriptor());
            assert(moved.type == snf::protocol::MessageType::Moved);
            assert(moved.request_id == request_id);
            assert(moved.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
            assert(read_u64(moved.payload, 1) == zone);
            assert(read_u64(moved.payload, 9) == 2);
            assert(static_cast<std::int32_t>(read_u32(moved.payload, 17)) == 11);
            assert(static_cast<std::int32_t>(read_u32(moved.payload, 21)) == y);
        }

        const auto room_metrics = server.getRoomActorStats();
        assert(room_metrics.tick_execution_nanoseconds.sample_count > 0);
        assert(room_metrics.tick_publish_nanoseconds.sample_count == room_metrics.tick_execution_nanoseconds.sample_count);
        assert(room_metrics.tick_turn_nanoseconds.sample_count == room_metrics.tick_execution_nanoseconds.sample_count);
        assert(room_metrics.tick_schedule_rejections == 0);
        assert(room_metrics.deadline_schedule_rejections == 0);
        assert(room_metrics.grant_tell_rejections == 0);
        const auto player_metrics = server.getPlayerActorStats();
        assert(player_metrics.reward_snapshot_admission_rejections == 0);
        assert(player_metrics.reward_snapshot_retry_giveups == 0);
        assert(player_metrics.grant_load_failures == 0);
    }

    void test_participant_defeat_reports_its_reason_and_returns_the_player_to_the_zone()
    {
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
            .room_battle_duration = 2s,
            .room_boss_health = 1000,
            .room_tick_interval = 5ms,
            .room_wave_interval = 100ms,
            .room_wave_count = 1,
            .room_minions_per_wave = 1,
            .room_minion_health = 1000,
            .room_boss_spawn_after = 500ms,
            .max_room_spawned_enemies = 2,
            .room_arena_width = 10,
            .room_arena_height = 10,
            .room_participant_spawn_spacing = 2,
            .room_minion_spawn_radius = 1,
            .room_minion_move_speed = 1,
            .room_minion_attack_damage = snf::server::BASE_HEALTH,
            .room_minion_attack_range = 2,
            .room_minion_attack_cooldown = 5ms,
        }};
        constexpr std::uint64_t player = 612;
        constexpr std::uint64_t zone = 89;
        constexpr std::uint64_t room = 78;
        auto client = connect_client(server.getPort());

        const auto auth = authentication_frame(330, player);
        const auto auth_bytes = snf::protocol::encode_frame(auth);
        send_all(client.getDescriptor(), auth_bytes);
        assert_authenticated(receive_exact(client.getDescriptor(), auth_bytes.size()), auth.request_id, player);

        std::vector<std::byte> enter_payload = player_id_payload(zone);
        append_u32(enter_payload, 17);
        append_u32(enter_payload, 23);
        send_all(
            client.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::EnterZone,
                .request_id = 331,
                .payload = std::move(enter_payload),
            })
        );
        const auto entered = receive_zone_response(client.getDescriptor());
        assert(entered.type == snf::protocol::MessageType::ZoneEntered);
        assert(entered.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));

        constexpr std::size_t ROOM_REPLY_PAYLOAD_SIZE = 1 + 1 + 8;
        send_all(client.getDescriptor(), snf::protocol::encode_frame(room_frame(snf::protocol::MessageType::RoomJoin, 332, room)));
        const auto joined = receive_room_frame(client.getDescriptor(), ROOM_REPLY_PAYLOAD_SIZE);
        assert(joined.type == snf::protocol::MessageType::RoomJoined);
        assert(joined.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));

        send_all(client.getDescriptor(), snf::protocol::encode_frame(room_frame(snf::protocol::MessageType::BattleStart, 333, room)));
        const auto started = receive_room_frame(client.getDescriptor(), ROOM_REPLY_PAYLOAD_SIZE);
        assert(started.type == snf::protocol::MessageType::BattleStarted);
        assert(started.payload[1] == static_cast<std::byte>(snf::server::RoomPhase::Running));

        const auto initial = receive_until_digest_event(client.getDescriptor(), snf::server::BattleEventKind::EnemySpawned);
        assert(initial.payload[8] == static_cast<std::byte>(snf::server::RoomPhase::Running));

        const auto defeat = receive_until_digest_event(client.getDescriptor(), snf::server::BattleEventKind::ParticipantDied);
        assert(defeat.payload[8] == static_cast<std::byte>(snf::server::RoomPhase::Failed));
        const auto damaged_offset = digest_event_offset(defeat, snf::server::BattleEventKind::ParticipantDamaged);
        const auto died_offset = digest_event_offset(defeat, snf::server::BattleEventKind::ParticipantDied);
        assert(damaged_offset && read_u64(defeat.payload, *damaged_offset + 1) == player);
        assert(read_u64(defeat.payload, *damaged_offset + 13) == snf::server::BASE_HEALTH);
        assert(read_u64(defeat.payload, *damaged_offset + 21) == 0);
        assert(died_offset && read_u64(defeat.payload, *died_offset + 1) == player);

        const auto failed = receive_until_type(client.getDescriptor(), snf::protocol::MessageType::BattleFailed);
        assert(failed.request_id == snf::protocol::UNSOLICITED_REQUEST_ID);
        assert(failed.payload.size() == 10);
        assert(read_u64(failed.payload, 0) == 0);
        assert(failed.payload[8] == std::byte{0});
        assert(failed.payload[9] == static_cast<std::byte>(snf::server::BattleFailureReason::ParticipantsDefeated));

        constexpr std::size_t RETURN_PAYLOAD_SIZE = 8 + 4 + 4;
        const auto returned = receive_room_frame(client.getDescriptor(), RETURN_PAYLOAD_SIZE);
        assert(returned.type == snf::protocol::MessageType::ReturnedToZone);
        assert(read_u64(returned.payload, 0) == zone);
        assert(read_u32(returned.payload, 8) == 17);
        assert(read_u32(returned.payload, 12) == 23);
    }

    void test_authenticated_player_enters_moves_and_leaves_a_zone()
    {
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
            .zone_tick_interval = 5ms,
        }};
        const auto client = connect_client(server.getPort());
        constexpr std::uint64_t player_id = 90;
        constexpr std::uint64_t zone_id = 5;

        const auto auth = authentication_frame(200, player_id);
        const auto auth_bytes = snf::protocol::encode_frame(auth);
        send_all(client.getDescriptor(), auth_bytes);
        assert_authenticated(receive_exact(client.getDescriptor(), auth_bytes.size()), auth.request_id, player_id);

        std::vector<std::byte> enter_payload = player_id_payload(zone_id);
        append_u32(enter_payload, 10);
        append_u32(enter_payload, 20);
        const snf::protocol::Frame enter{
            .type = snf::protocol::MessageType::EnterZone,
            .request_id = 201,
            .payload = std::move(enter_payload),
        };
        send_all(client.getDescriptor(), snf::protocol::encode_frame(enter));
        const auto entered = receive_zone_response(client.getDescriptor());
        assert(entered.type == snf::protocol::MessageType::ZoneEntered);
        assert(entered.request_id == enter.request_id);
        assert(entered.payload[0] == std::byte{0});
        assert(read_u64(entered.payload, 1) == zone_id);
        const std::uint64_t route_epoch = read_u64(entered.payload, 9);
        assert(route_epoch == 1);
        assert(static_cast<std::int32_t>(read_u32(entered.payload, 17)) == 10);
        assert(static_cast<std::int32_t>(read_u32(entered.payload, 21)) == 20);

        const auto tick_deadline = std::chrono::steady_clock::now() + 1s;
        while (server.getZoneActorStats().tick_execution_nanoseconds.sample_count == 0 && std::chrono::steady_clock::now() < tick_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(server.getZoneActorStats().tick_execution_nanoseconds.sample_count >= 1);

        std::vector<std::byte> move_payload;
        append_u32(move_payload, static_cast<std::uint32_t>(-3));
        append_u32(move_payload, 40);
        const snf::protocol::Frame move{
            .type = snf::protocol::MessageType::Move,
            .request_id = 202,
            .payload = std::move(move_payload),
        };
        send_all(client.getDescriptor(), snf::protocol::encode_frame(move));
        const auto moved = receive_zone_response(client.getDescriptor());
        assert(moved.type == snf::protocol::MessageType::Moved);
        assert(moved.request_id == move.request_id);
        assert(read_u64(moved.payload, 9) == route_epoch);
        assert(static_cast<std::int32_t>(read_u32(moved.payload, 17)) == -3);
        assert(static_cast<std::int32_t>(read_u32(moved.payload, 21)) == 40);

        const snf::protocol::Frame leave{
            .type = snf::protocol::MessageType::LeaveZone,
            .request_id = 203,
            .payload = {},
        };
        send_all(client.getDescriptor(), snf::protocol::encode_frame(leave));
        const auto left = receive_zone_response(client.getDescriptor());
        assert(left.type == snf::protocol::MessageType::ZoneLeft);
        assert(left.request_id == leave.request_id);
        assert(read_u64(left.payload, 9) == route_epoch);

        const auto cancellation_deadline = std::chrono::steady_clock::now() + 1s;
        while (std::chrono::steady_clock::now() < cancellation_deadline)
        {
            std::size_t active = 0;
            for (const auto& w : server.getActorRuntimeStats().workers)
            {
                active += w.active_timers;
            }
            if (active == 0)
            {
                break;
            }
            std::this_thread::sleep_for(1ms);
        }
        std::size_t total_active = 0;
        for (const auto& w : server.getActorRuntimeStats().workers)
        {
            total_active += w.active_timers;
        }
        assert(total_active == 0);

        server.stop();
    }

    void test_authenticated_player_handoffs_between_zones_and_publishes_target_route()
    {
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
            .zone_tick_interval = 5ms,
            .max_zone_handoffs = 1,
            .max_zone_handoff_completions_per_turn = 1,
        }};
        const auto client = connect_client(server.getPort());
        constexpr std::uint64_t player_id = 93;
        constexpr std::uint64_t source_zone = 8;
        constexpr std::uint64_t target_zone = 9;

        const auto auth = authentication_frame(400, player_id);
        const auto auth_bytes = snf::protocol::encode_frame(auth);
        send_all(client.getDescriptor(), auth_bytes);
        assert_authenticated(receive_exact(client.getDescriptor(), auth_bytes.size()), auth.request_id, player_id);

        std::vector<std::byte> source_payload = player_id_payload(source_zone);
        append_u32(source_payload, 10);
        append_u32(source_payload, 20);
        send_all(
            client.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::EnterZone,
                .request_id = 401,
                .payload = std::move(source_payload),
            })
        );
        const auto source_entered = receive_zone_response(client.getDescriptor());
        assert(source_entered.type == snf::protocol::MessageType::ZoneEntered);
        assert(read_u64(source_entered.payload, 1) == source_zone);
        assert(read_u64(source_entered.payload, 9) == 1);

        std::vector<std::byte> target_payload = player_id_payload(target_zone);
        append_u32(target_payload, static_cast<std::uint32_t>(-50));
        append_u32(target_payload, 60);
        send_all(
            client.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::EnterZone,
                .request_id = 402,
                .payload = std::move(target_payload),
            })
        );
        const auto target_entered = receive_zone_response(client.getDescriptor());
        assert(target_entered.type == snf::protocol::MessageType::ZoneEntered);
        assert(target_entered.request_id == 402);
        assert(target_entered.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));
        assert(read_u64(target_entered.payload, 1) == target_zone);
        assert(read_u64(target_entered.payload, 9) == 2);
        assert(static_cast<std::int32_t>(read_u32(target_entered.payload, 17)) == -50);
        assert(static_cast<std::int32_t>(read_u32(target_entered.payload, 21)) == 60);

        std::vector<std::byte> move_payload;
        append_u32(move_payload, 70);
        append_u32(move_payload, static_cast<std::uint32_t>(-80));
        send_all(
            client.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::Move,
                .request_id = 403,
                .payload = std::move(move_payload),
            })
        );
        const auto moved = receive_zone_response(client.getDescriptor());
        assert(moved.type == snf::protocol::MessageType::Moved);
        assert(read_u64(moved.payload, 1) == target_zone);
        assert(read_u64(moved.payload, 9) == 2);

        send_all(
            client.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::LeaveZone,
                .request_id = 404,
                .payload = {},
            })
        );
        const auto left = receive_zone_response(client.getDescriptor());
        assert(left.type == snf::protocol::MessageType::ZoneLeft);
        assert(read_u64(left.payload, 1) == target_zone);
        assert(read_u64(left.payload, 9) == 2);

        server.stop();
        const auto metrics = server.getMetricsSnapshot();
        assert(metrics.zone_handoffs.handoffs_started == 1);
        assert(metrics.zone_handoffs.handoffs_completed == 1);
        assert(metrics.zone_handoffs.pending_handoffs == 0);
        assert(metrics.zone_handoffs_saga.transition_nanoseconds.sample_count == 1);
        assert(metrics.zone_handoffs_saga.pending == 0);
        assert(metrics.zone_transition_channel.completions_published == 2);
        assert(metrics.zone_transition_channel.completions_consumed == 2);
        assert(metrics.zone_transition_channel.reservations == 0);
        assert(metrics.command_terminals == 5);
    }

    void test_peer_disconnect_evicts_the_player_actor()
    {
        RunningServer server;
        {
            const auto client = connect_client(server.getPort());
            const snf::protocol::Frame request{
                .type = snf::protocol::MessageType::Ping,
                .request_id = 11,
                .payload = {},
            };
            const auto encoded_request = snf::protocol::encode_frame(request);
            send_all(client.getDescriptor(), encoded_request);
            assert_pong(receive_exact(client.getDescriptor(), encoded_request.size()), request);
        }

        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (actor_count(server.getActorRuntimeStats()) != 0 && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(5ms);
        }

        const auto stats = server.getActorRuntimeStats();
        assert(actor_count(stats) == 0);
        assert(evicted_actor_count(stats) == 1);
        server.stop();
    }

    void test_zone_position_survives_disconnect_save_and_reconnect()
    {
        RunningServer server;
        constexpr std::uint64_t player_id = 91;
        constexpr std::uint64_t zone_id = 7;

        {
            auto client = connect_client(server.getPort());
            const auto auth = authentication_frame(300, player_id);
            const auto auth_bytes = snf::protocol::encode_frame(auth);
            send_all(client.getDescriptor(), auth_bytes);
            assert_authenticated(receive_exact(client.getDescriptor(), auth_bytes.size()), auth.request_id, player_id);

            std::vector<std::byte> enter_payload = player_id_payload(zone_id);
            append_u32(enter_payload, 1);
            append_u32(enter_payload, 2);
            send_all(
                client.getDescriptor(),
                snf::protocol::encode_frame(snf::protocol::Frame{
                    .type = snf::protocol::MessageType::EnterZone,
                    .request_id = 301,
                    .payload = std::move(enter_payload),
                })
            );
            static_cast<void>(receive_zone_response(client.getDescriptor()));

            std::vector<std::byte> move_payload;
            append_u32(move_payload, static_cast<std::uint32_t>(-30));
            append_u32(move_payload, 45);
            send_all(
                client.getDescriptor(),
                snf::protocol::encode_frame(snf::protocol::Frame{
                    .type = snf::protocol::MessageType::Move,
                    .request_id = 302,
                    .payload = std::move(move_payload),
                })
            );
            static_cast<void>(receive_zone_response(client.getDescriptor()));
            client.init();
        }

        const snf::server::PlayerId player{.value = player_id};
        const auto save_deadline = std::chrono::steady_clock::now() + 1s;
        std::optional<snf::server::PlayerRecord> saved;
        while (std::chrono::steady_clock::now() < save_deadline)
        {
            saved = server.getPlayerRecord(player);
            if (saved && saved->last_location)
            {
                break;
            }
            std::this_thread::sleep_for(1ms);
        }
        assert(saved.has_value());
        assert(
            (saved->last_location ==
             snf::server::PlayerLocation{
                 .zone = snf::server::ZoneId{.value = zone_id},
                 .position = {.x = -30, .y = 45},
             })
        );

        const auto passivation_deadline = std::chrono::steady_clock::now() + 1s;
        while (actor_count(server.getActorRuntimeStats()) != 0 && std::chrono::steady_clock::now() < passivation_deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(actor_count(server.getActorRuntimeStats()) == 0);

        const auto reconnected = connect_client(server.getPort());
        const auto auth = authentication_frame(303, player_id);
        const auto auth_bytes = snf::protocol::encode_frame(auth);
        send_all(reconnected.getDescriptor(), auth_bytes);
        assert_authenticated(receive_exact(reconnected.getDescriptor(), auth_bytes.size()), auth.request_id, player_id);

        std::vector<std::byte> enter_payload = player_id_payload(zone_id);
        append_u32(enter_payload, 999);
        append_u32(enter_payload, 999);
        send_all(
            reconnected.getDescriptor(),
            snf::protocol::encode_frame(snf::protocol::Frame{
                .type = snf::protocol::MessageType::EnterZone,
                .request_id = 304,
                .payload = std::move(enter_payload),
            })
        );
        const auto restored = receive_zone_response(reconnected.getDescriptor());
        assert(restored.type == snf::protocol::MessageType::ZoneEntered);
        assert(static_cast<std::int32_t>(read_u32(restored.payload, 17)) == -30);
        assert(static_cast<std::int32_t>(read_u32(restored.payload, 21)) == 45);

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
        bundled_requests.insert(bundled_requests.end(), second_encoded_request.begin(), second_encoded_request.end());

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
            send_all(partial_client.getDescriptor(), std::span<const std::byte>{encoded_request}.first(5));
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

    void test_closes_connection_for_an_unregistered_message()
    {
        RunningServer server;
        const auto invalid_client = connect_client(server.getPort());
        const snf::protocol::Frame invalid_request{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 7,
            .payload = {},
        };

        send_all(invalid_client.getDescriptor(), snf::protocol::encode_frame(invalid_request));
        receive_until_closed(invalid_client.getDescriptor());

        const auto healthy_client = connect_client(server.getPort());
        const snf::protocol::Frame healthy_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 8,
            .payload = {},
        };
        const auto healthy_encoded_request = snf::protocol::encode_frame(healthy_request);
        send_all(healthy_client.getDescriptor(), healthy_encoded_request);
        assert_pong(receive_exact(healthy_client.getDescriptor(), healthy_encoded_request.size()), healthy_request);

        server.stop();
        assert(server.getStats().protocol_errors >= 1);
        assert(actor_count(server.getActorRuntimeStats()) == 0);
    }

    void test_overflowed_actor_queue_closes_only_that_connection()
    {
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
            .client_send_buffer_size = std::nullopt,
            .actor_worker_count = 2,
            .actor_queue_capacity_per_worker = 1,
            .outbound_queue_capacity = 1,
        }};
        const auto overloaded_client = connect_client(server.getPort());
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 9,
            .payload = {},
        };
        const auto encoded_request = snf::protocol::encode_frame(request);
        std::vector<std::byte> bundled_requests;
        constexpr int REQUEST_COUNT = 32;
        bundled_requests.reserve(encoded_request.size() * REQUEST_COUNT);
        for (int request_index = 0; request_index < REQUEST_COUNT; ++request_index)
        {
            bundled_requests.insert(bundled_requests.end(), encoded_request.begin(), encoded_request.end());
        }

        send_all(overloaded_client.getDescriptor(), bundled_requests);
        receive_until_closed(overloaded_client.getDescriptor());

        const auto healthy_client = connect_client(server.getPort());
        const snf::protocol::Frame healthy_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 10,
            .payload = {std::byte{0x01}},
        };
        const auto healthy_encoded_request = snf::protocol::encode_frame(healthy_request);
        send_all(healthy_client.getDescriptor(), healthy_encoded_request);
        assert_pong(receive_exact(healthy_client.getDescriptor(), healthy_encoded_request.size()), healthy_request);

        server.stop();
        assert(server.getStats().actor_queue_overflows >= 1);
        assert(server.getStats().stale_outbound_actions >= 1);
    }

    void test_request_stop_closes_listener_and_active_sessions()
    {
        RunningServer server;
        const auto port = server.getPort();
        const auto client = connect_client(port);
        constexpr std::uint64_t player_id = 155;
        const auto auth = authentication_frame(1550, player_id);
        const auto auth_bytes = snf::protocol::encode_frame(auth);
        send_all(client.getDescriptor(), auth_bytes);
        assert_authenticated(receive_exact(client.getDescriptor(), auth_bytes.size()), auth.request_id, player_id);

        const snf::protocol::Frame ping{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 1551,
            .payload = {std::byte{0x55}},
        };
        const auto ping_bytes = snf::protocol::encode_frame(ping);
        send_all(client.getDescriptor(), ping_bytes);
        assert_pong(receive_exact(client.getDescriptor(), ping_bytes.size()), ping);

        const auto start_time = std::chrono::steady_clock::now();
        server.stop();
        const auto elapsed_time = std::chrono::steady_clock::now() - start_time;

        assert(elapsed_time < 1s);
        const auto saved = server.getPlayerRecord(snf::server::PlayerId{.value = player_id});
        assert(saved.has_value());
        assert(saved->handled_command_count == 2);

        snf::net::UniqueFileDescriptor connection_attempt{::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0)};
        assert(connection_attempt.isValid());

        sockaddr_in server_address{};
        server_address.sin_family = AF_INET;
        server_address.sin_port = htons(port);
        assert(::inet_pton(AF_INET, "127.0.0.1", &server_address.sin_addr) == 1);

        assert(::connect(connection_attempt.getDescriptor(), reinterpret_cast<const sockaddr*>(&server_address), sizeof(server_address)) == -1);
    }

    void test_closes_slow_client_when_send_queue_exceeds_limit()
    {
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 7,
            .payload = std::vector<std::byte>(snf::protocol::MAX_PAYLOAD_SIZE, std::byte{0xAA}),
        };
        const auto encoded_request = snf::protocol::encode_frame(request);

        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = encoded_request.size(),
            .client_send_buffer_size = 1024,
        }};
        const auto slow_client = connect_client(server.getPort(), 1024);

        std::vector<std::byte> bundled_requests;
        constexpr int REQUEST_COUNT = 32;
        bundled_requests.reserve(encoded_request.size() * REQUEST_COUNT);
        for (int request_index = 0; request_index < REQUEST_COUNT; ++request_index)
        {
            bundled_requests.insert(bundled_requests.end(), encoded_request.begin(), encoded_request.end());
        }
        send_until_complete_or_closed(slow_client.getDescriptor(), bundled_requests);
        receive_until_closed(slow_client.getDescriptor());

        const auto healthy_client = connect_client(server.getPort());
        const snf::protocol::Frame healthy_request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 8,
            .payload = {},
        };
        const auto healthy_encoded_request = snf::protocol::encode_frame(healthy_request);
        send_all(healthy_client.getDescriptor(), healthy_encoded_request);
        const auto response = receive_exact(healthy_client.getDescriptor(), healthy_encoded_request.size());
        assert_pong(response, healthy_request);

        server.stop();
    }

    void test_slow_battle_client_is_closed_while_healthy_participants_clear_the_room()
    {
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = 200ms,
            .max_pending_send_bytes = 32 * 1024,
            .client_send_buffer_size = 8 * 1024,
            .room_battle_duration = 10s,
            .room_boss_health = snf::server::BASE_ATTACK,
            .room_tick_interval = 5ms,
            .room_wave_interval = 1s,
            .room_wave_count = 0,
            .room_minions_per_wave = 0,
            .room_minion_health = snf::server::BASE_ATTACK,
            .room_boss_spawn_after = 500ms,
            .max_room_spawned_enemies = 1,
            .room_arena_width = 20,
            .room_arena_height = 4000,
            .room_player_move_speed = 1,
            .room_participant_spawn_spacing = 2,
            .room_minion_spawn_radius = 5,
        }};
        constexpr std::uint64_t room = 901;
        constexpr std::array<std::uint64_t, 4> players{910, 911, 912, 913};
        constexpr std::size_t SLOW_INDEX = 0;
        constexpr std::size_t FIRST_HEALTHY_INDEX = 1;
        constexpr std::size_t ROOM_REPLY_PAYLOAD_SIZE = 1 + 1 + 8;

        std::array<snf::net::UniqueFileDescriptor, 4> clients{
            connect_client(server.getPort(), 1024),
            connect_client(server.getPort()),
            connect_client(server.getPort()),
            connect_client(server.getPort()),
        };
        const timeval healthy_receive_timeout{
            .tv_sec = 5,
            .tv_usec = 0,
        };
        for (std::size_t index = FIRST_HEALTHY_INDEX; index < clients.size(); ++index)
        {
            assert(
                ::setsockopt(
                    clients[index].getDescriptor(), SOL_SOCKET, SO_RCVTIMEO, &healthy_receive_timeout, sizeof(healthy_receive_timeout)
                ) == 0
            );
        }

        for (std::size_t index = 0; index < clients.size(); ++index)
        {
            const auto auth = authentication_frame(400 + static_cast<std::uint32_t>(index), players[index]);
            const auto auth_bytes = snf::protocol::encode_frame(auth);
            send_all(clients[index].getDescriptor(), auth_bytes);
            assert_authenticated(receive_exact(clients[index].getDescriptor(), auth_bytes.size()), auth.request_id, players[index]);

            std::vector<std::byte> enter_payload = player_id_payload(920 + index);
            append_u32(enter_payload, 10);
            append_u32(enter_payload, 10);
            send_all(
                clients[index].getDescriptor(),
                snf::protocol::encode_frame(snf::protocol::Frame{
                    .type = snf::protocol::MessageType::EnterZone,
                    .request_id = 410 + static_cast<std::uint32_t>(index),
                    .payload = std::move(enter_payload),
                })
            );
            const auto entered = receive_zone_response(clients[index].getDescriptor());
            assert(entered.type == snf::protocol::MessageType::ZoneEntered);
            assert(entered.payload[0] == static_cast<std::byte>(snf::server::ZoneCommandStatus::Applied));

            send_all(
                clients[index].getDescriptor(),
                snf::protocol::encode_frame(room_frame(snf::protocol::MessageType::RoomJoin, 420 + static_cast<std::uint32_t>(index), room))
            );
            const auto joined = receive_room_frame(clients[index].getDescriptor(), ROOM_REPLY_PAYLOAD_SIZE);
            assert(joined.type == snf::protocol::MessageType::RoomJoined);
            assert(joined.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));
        }

        send_all(
            clients[FIRST_HEALTHY_INDEX].getDescriptor(), snf::protocol::encode_frame(room_frame(snf::protocol::MessageType::BattleStart, 430, room))
        );
        const auto started = receive_room_frame(clients[FIRST_HEALTHY_INDEX].getDescriptor(), ROOM_REPLY_PAYLOAD_SIZE);
        assert(started.type == snf::protocol::MessageType::BattleStarted);
        assert(started.payload[1] == static_cast<std::byte>(snf::server::RoomPhase::Running));

        struct HealthyObservation
        {
            std::uint64_t last_digest_sequence{0};
            std::uint64_t participant_left_sequence{0};
            bool saw_boss{false};
            std::optional<std::uint32_t> boss_id;
            std::optional<snf::server::ArenaPosition> boss_position;
            std::optional<snf::server::ArenaPosition> caster_position;
            bool participant_in_boss_range{false};
            bool skill_acknowledged{false};
            bool saw_enemy_damaged{false};
            bool saw_enemy_died{false};
            bool saw_clear{false};
        };
        std::array<HealthyObservation, 3> observations{};

        const auto observe = [&players](const snf::protocol::Frame& frame, HealthyObservation& observation)
        {
            if (frame.type == snf::protocol::MessageType::BattleDigest)
            {
                const std::uint64_t sequence = read_u64(frame.payload, 0);
                assert(sequence == observation.last_digest_sequence + 1);
                observation.last_digest_sequence = sequence;

                for (const auto& [kind, offset] : digest_events(frame))
                {
                    if (kind == snf::server::BattleEventKind::ParticipantLeft)
                    {
                        assert(read_u64(frame.payload, offset + 1) == players[SLOW_INDEX]);
                        observation.participant_left_sequence = sequence;
                    }
                    else if (kind == snf::server::BattleEventKind::EnemySpawned &&
                             frame.payload[offset + 5] == static_cast<std::byte>(snf::server::EnemyKind::Boss))
                    {
                        observation.saw_boss = true;
                        observation.boss_id = read_u32(frame.payload, offset + 1);
                    }
                    else if (kind == snf::server::BattleEventKind::EnemyPositioned && observation.boss_id &&
                             read_u32(frame.payload, offset + 1) == *observation.boss_id)
                    {
                        observation.boss_position = snf::server::ArenaPosition{
                            .x = read_u32(frame.payload, offset + 5),
                            .y = read_u32(frame.payload, offset + 9),
                        };
                    }
                    else if (kind == snf::server::BattleEventKind::ParticipantMoved &&
                             read_u64(frame.payload, offset + 1) == players[FIRST_HEALTHY_INDEX])
                    {
                        observation.caster_position = snf::server::ArenaPosition{
                            .x = read_u32(frame.payload, offset + 9),
                            .y = read_u32(frame.payload, offset + 13),
                        };
                    }
                    else if (kind == snf::server::BattleEventKind::EnemyDamaged)
                    {
                        observation.saw_enemy_damaged = true;
                    }
                    else if (kind == snf::server::BattleEventKind::EnemyDied)
                    {
                        observation.saw_enemy_died = true;
                    }
                }
                if (observation.boss_position && observation.caster_position)
                {
                    observation.participant_in_boss_range =
                        observation.participant_in_boss_range ||
                        snf::server::squaredDistance(*observation.boss_position, *observation.caster_position) <=
                            static_cast<std::uint64_t>(snf::server::SLASH_RANGE) * snf::server::SLASH_RANGE;
                }
            }
            else if (frame.type == snf::protocol::MessageType::SkillAcknowledged)
            {
                assert(frame.payload.size() == 2);
                assert(frame.payload[0] == static_cast<std::byte>(snf::server::RoomCommandStatus::Applied));
                assert(frame.payload[1] == static_cast<std::byte>(snf::server::RoomPhase::Cleared));
                observation.skill_acknowledged = true;
            }
            else if (frame.type == snf::protocol::MessageType::BattleCleared)
            {
                assert(frame.request_id == snf::protocol::UNSOLICITED_REQUEST_ID);
                observation.saw_clear = true;
            }
        };

        for (std::size_t index = 0; index < clients.size(); ++index)
        {
            const auto initial = receive_frame(clients[index].getDescriptor());
            assert(initial.type == snf::protocol::MessageType::BattleDigest);
            assert(read_u64(initial.payload, 0) == 1);
            if (index != SLOW_INDEX)
            {
                observe(initial, observations[index - FIRST_HEALTHY_INDEX]);
            }
        }

        send_all(
            clients[FIRST_HEALTHY_INDEX].getDescriptor(),
            snf::protocol::encode_frame(set_move_intent_frame(440, room, snf::server::MoveDirection::North, 1))
        );

        const auto ready_to_cast = [](const HealthyObservation& observation)
        {
            return observation.participant_left_sequence != 0 && observation.saw_boss && observation.participant_in_boss_range;
        };

        std::array<std::promise<void>, 3> ready_promises;
        std::array<std::future<void>, 3> ready_futures;
        std::array<std::future<HealthyObservation>, 3> readers;
        for (std::size_t healthy_index = 0; healthy_index < observations.size(); ++healthy_index)
        {
            ready_futures[healthy_index] = ready_promises[healthy_index].get_future();
            readers[healthy_index] = std::async(
                std::launch::async,
                [&, healthy_index]
                {
                    HealthyObservation& observation = observations[healthy_index];
                    bool reported_ready = false;
                    while (!observation.saw_clear)
                    {
                        observe(receive_frame(clients[healthy_index + FIRST_HEALTHY_INDEX].getDescriptor()), observation);
                        if (!reported_ready && ready_to_cast(observation))
                        {
                            ready_promises[healthy_index].set_value();
                            reported_ready = true;
                        }
                    }
                    return observation;
                }
            );
        }

        for (auto& ready : ready_futures)
        {
            ready.get();
        }

        send_all(clients[FIRST_HEALTHY_INDEX].getDescriptor(), snf::protocol::encode_frame(use_skill_frame(450, room, snf::server::SLASH, 1)));

        std::array<HealthyObservation, 3> completed_observations;
        for (std::size_t healthy_index = 0; healthy_index < readers.size(); ++healthy_index)
        {
            completed_observations[healthy_index] = readers[healthy_index].get();
        }
        assert(completed_observations.front().skill_acknowledged);
        for (const HealthyObservation& observation : completed_observations)
        {
            assert(observation.saw_enemy_damaged);
            assert(observation.saw_enemy_died);
            assert(observation.saw_clear);
            assert(observation.last_digest_sequence > observation.participant_left_sequence);
        }

        assert(server.getStats().closed_connections == 1);
        server.stop();
        assert(server.getStats().accepted_connections == 4);
        assert(server.getStats().closed_connections == 4);
    }

    void test_shutdown_forces_slow_client_closed_after_grace_period()
    {
        constexpr auto shutdown_grace_period = 150ms;
        RunningServer server{snf::server::GameServerConfig{
            .port = 0,
            .shutdown_grace_period = shutdown_grace_period,
            .max_pending_send_bytes = 8 * 1024 * 1024,
            .client_send_buffer_size = 1024,
        }};
        const auto slow_client = connect_client(server.getPort(), 1024);

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 9,
            .payload = std::vector<std::byte>(snf::protocol::MAX_PAYLOAD_SIZE, std::byte{0xBB}),
        };
        const auto encoded_request = snf::protocol::encode_frame(request);
        std::vector<std::byte> bundled_requests;
        constexpr int REQUEST_COUNT = 64;
        bundled_requests.reserve(encoded_request.size() * REQUEST_COUNT);

        for (int request_index = 0; request_index < REQUEST_COUNT; ++request_index)
        {
            bundled_requests.insert(bundled_requests.end(), encoded_request.begin(), encoded_request.end());
        }

        send_all(slow_client.getDescriptor(), bundled_requests);

        pollfd response_poll{
            .fd = slow_client.getDescriptor(),
            .events = POLLIN,
            .revents = 0,
        };
        assert(::poll(&response_poll, 1, 1000) == 1);
        assert((response_poll.revents & POLLIN) != 0);

        const auto stop_started_at = std::chrono::steady_clock::now();
        server.stop();
        const auto stop_duration = std::chrono::steady_clock::now() - stop_started_at;

        assert(stop_duration >= shutdown_grace_period / 2);
        assert(stop_duration < 1s);
    }

    void test_termination_signal_stops_server(const int signal_number)
    {
        const auto termination_signal = snf::net::create_termination_signal_listener();
        RunningServer server{termination_signal.getDescriptor()};

        assert(::kill(::getpid(), signal_number) == 0);
        server.join();
    }

    void test_actor_runtime_failure_aborts_without_waiting_for_grace_period()
    {
        RecordingFrameIngress ingress;
        const auto outbound_event = make_eventfd();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 8, .max_slots_per_connection = 8}, outbound_event.getDescriptor()
        };
        snf::runtime::RuntimeCompletionCoordinator runtime_completion{
            snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic), outbound_event.getDescriptor()
        };
        snf::server::TcpServer server{
            snf::server::TcpServerConfig{
                .port = 0,
                .shutdown_grace_period = 5s,
                .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                .client_send_buffer_size = std::nullopt,
            },
            ingress,
            outbound,
            runtime_completion,
            outbound_event.getDescriptor()
        };

        std::promise<void> server_finished;
        const auto finished = server_finished.get_future();
        std::exception_ptr server_error;
        std::thread server_thread{[&]
                                  {
                                      try
                                      {
                                          server.run();
                                      }
                                      catch (...)
                                      {
                                          server_error = std::current_exception();
                                      }
                                      server_finished.set_value();
                                  }};

        runtime_completion.notifyFailed(snf::runtime::RuntimeId::Logic);
        assert(finished.wait_for(1s) == std::future_status::ready);
        server_thread.join();

        assert(server_error == nullptr);
        assert(ingress.closed);
        assert(ingress.cancelled);
    }

    void test_retries_a_full_connection_closed_post_without_duplicate_after_acceptance()
    {
        RecordingFrameIngress ingress;
        ingress.lifecycle_results = {
            snf::server::PostResult::Full,
            snf::server::PostResult::Full,
            snf::server::PostResult::Accepted,
        };
        const auto outbound_event = make_eventfd();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 8, .max_slots_per_connection = 8}, outbound_event.getDescriptor()
        };
        snf::runtime::RuntimeCompletionCoordinator runtime_completion{
            snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic), outbound_event.getDescriptor()
        };
        snf::server::TcpServer server{
            snf::server::TcpServerConfig{
                .port = 0,
                .shutdown_grace_period = 200ms,
                .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                .client_send_buffer_size = std::nullopt,
            },
            ingress,
            outbound,
            runtime_completion,
            outbound_event.getDescriptor()
        };

        std::exception_ptr server_error;
        std::thread server_thread{[&]
                                  {
                                      try
                                      {
                                          server.run();
                                      }
                                      catch (...)
                                      {
                                          server_error = std::current_exception();
                                      }
                                  }};
        {
            const auto client = connect_client(server.getPort());
        }

        const auto deadline = std::chrono::steady_clock::now() + 1s;
        while (ingress.lifecycle_attempts.load() != 3 && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(1ms);
        }
        assert(ingress.lifecycle_attempts.load() == 3);

        runtime_completion.notifyDrained(snf::runtime::RuntimeId::Logic);
        server.requestStop();
        server_thread.join();

        assert(server_error == nullptr);
        assert(ingress.connection_closes.size() == 3);
        const auto& first = ingress.connection_closes.front();
        for (const auto& closed : ingress.connection_closes)
        {
            assert(closed.connection == first.connection);
            assert(closed.cause == snf::server::ConnectionCloseCause::PeerClosed);
        }
    }

    void test_bounds_pending_connection_closes_and_rejects_new_connections_at_capacity()
    {
        RecordingFrameIngress ingress;
        ingress.lifecycle_fallback = snf::server::PostResult::Full;
        const auto outbound_event = make_eventfd();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 8, .max_slots_per_connection = 8}, outbound_event.getDescriptor()
        };
        snf::runtime::RuntimeCompletionCoordinator runtime_completion{
            snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic), outbound_event.getDescriptor()
        };
        snf::server::TcpServer server{
            snf::server::TcpServerConfig{
                .port = 0,
                .shutdown_grace_period = 200ms,
                .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                .client_send_buffer_size = std::nullopt,
                .connection_lifecycle_capacity = 2,
            },
            ingress,
            outbound,
            runtime_completion,
            outbound_event.getDescriptor()
        };

        std::exception_ptr server_error;
        std::thread server_thread{[&]
                                  {
                                      try
                                      {
                                          server.run();
                                      }
                                      catch (...)
                                      {
                                          server_error = std::current_exception();
                                      }
                                  }};

        for (std::size_t close_index = 0; close_index < 2; ++close_index)
        {
            {
                const auto client = connect_client(server.getPort());
            }

            const auto deadline = std::chrono::steady_clock::now() + 1s;
            while (ingress.distinctConnectionCloseCount() < close_index + 1 && std::chrono::steady_clock::now() < deadline)
            {
                std::this_thread::sleep_for(1ms);
            }
            assert(ingress.distinctConnectionCloseCount() >= close_index + 1);
        }

        const auto rejected_client = connect_client(server.getPort());
        receive_until_closed(rejected_client.getDescriptor());

        runtime_completion.notifyDrained(snf::runtime::RuntimeId::Logic);
        server.requestStop();
        server_thread.join();

        assert(server_error == nullptr);
        assert(server.getStats().accepted_connections == 2);
        assert(server.getStats().closed_connections == 2);
        assert(server.getStats().connection_lifecycle_rejections == 1);
        assert(server.getStats().pending_connection_closes_high_water_mark == 2);
    }

    void test_shutdown_waits_for_reactor_control_state_after_logic_drains()
    {
        RecordingFrameIngress ingress;
        const auto outbound_event = make_eventfd();
        snf::server::OutboundChannel outbound{
            snf::server::OutboundChannelConfig{.capacity = 2, .max_slots_per_connection = 2}, outbound_event.getDescriptor()
        };
        snf::runtime::RuntimeCompletionCoordinator runtime_completion{
            snf::runtime::runtimeMask(snf::runtime::RuntimeId::Logic), outbound_event.getDescriptor()
        };
        std::atomic<bool> control_drained{false};
        snf::server::TcpServer server{
            snf::server::TcpServerConfig{
                .port = 0,
                .shutdown_grace_period = 500ms,
                .max_pending_send_bytes = snf::net::MAX_PENDING_SEND_BYTES,
                .client_send_buffer_size = std::nullopt,
                .connection_lifecycle_capacity = 2,
                .metrics_report_interval = 0ms,
                .on_metrics_interval = {},
                .on_control_wake = {},
                .is_control_drained =
                    [&control_drained]
                {
                    return control_drained.load(std::memory_order_acquire);
                },
            },
            ingress,
            outbound,
            runtime_completion,
            outbound_event.getDescriptor()
        };

        std::exception_ptr server_error;
        std::promise<void> finished;
        auto finished_future = finished.get_future();
        std::thread server_thread{[&]
                                  {
                                      try
                                      {
                                          server.run();
                                      }
                                      catch (...)
                                      {
                                          server_error = std::current_exception();
                                      }
                                      finished.set_value();
                                  }};

        runtime_completion.notifyDrained(snf::runtime::RuntimeId::Logic);
        server.requestStop();
        assert(finished_future.wait_for(30ms) == std::future_status::timeout);

        control_drained.store(true, std::memory_order_release);
        constexpr std::uint64_t wake = 1;
        assert(::write(outbound_event.getDescriptor(), &wake, sizeof(wake)) == static_cast<ssize_t>(sizeof(wake)));
        assert(finished_future.wait_for(1s) == std::future_status::ready);
        server_thread.join();
        assert(server_error == nullptr);
    }
}

int main()
{
    const auto run = [](const char* name, auto&& test)
    {
        std::cerr << "[ run  ] " << name << '\n';
        test();
        std::cerr << "[  ok  ] " << name << '\n';
    };

    run("test_returns_pong_for_ping", test_returns_pong_for_ping);
    run("test_authenticates_one_session_and_allows_reconnect_after_passivation",
        test_authenticates_one_session_and_allows_reconnect_after_passivation);
    run("test_reconnect_waits_while_the_previous_session_is_closing", test_reconnect_waits_while_the_previous_session_is_closing);
    run("test_live_purchase_is_memory_authoritative_and_flushes", test_live_purchase_is_memory_authoritative_and_flushes);
    run("test_two_players_kill_a_boss_and_are_told_without_asking", test_two_players_kill_a_boss_and_are_told_without_asking);
    run("test_participant_defeat_reports_its_reason_and_returns_the_player_to_the_zone",
        test_participant_defeat_reports_its_reason_and_returns_the_player_to_the_zone);
    run("test_authenticated_player_enters_moves_and_leaves_a_zone", test_authenticated_player_enters_moves_and_leaves_a_zone);
    run("test_authenticated_player_handoffs_between_zones_and_publishes_target_route",
        test_authenticated_player_handoffs_between_zones_and_publishes_target_route);
    run("test_zone_position_survives_disconnect_save_and_reconnect", test_zone_position_survives_disconnect_save_and_reconnect);
    run("test_collects_baseline_saturation_metrics_for_a_round_trip", test_collects_baseline_saturation_metrics_for_a_round_trip);
    run("test_saturated_outbound_answers_every_request_and_still_drains", test_saturated_outbound_answers_every_request_and_still_drains);
    run("test_reports_metrics_periodically_while_running", test_reports_metrics_periodically_while_running);
    run("test_peer_disconnect_evicts_the_player_actor", test_peer_disconnect_evicts_the_player_actor);
    run("test_decodes_ping_sent_one_byte_at_a_time", test_decodes_ping_sent_one_byte_at_a_time);
    run("test_decodes_multiple_pings_from_one_send", test_decodes_multiple_pings_from_one_send);
    run("test_survives_client_close_during_partial_frame", test_survives_client_close_during_partial_frame);
    run("test_closes_connection_for_an_unregistered_message", test_closes_connection_for_an_unregistered_message);
    run("test_overflowed_actor_queue_closes_only_that_connection", test_overflowed_actor_queue_closes_only_that_connection);
    run("test_request_stop_closes_listener_and_active_sessions", test_request_stop_closes_listener_and_active_sessions);
    run("test_closes_slow_client_when_send_queue_exceeds_limit", test_closes_slow_client_when_send_queue_exceeds_limit);
    run("test_slow_battle_client_is_closed_while_healthy_participants_clear_the_room",
        test_slow_battle_client_is_closed_while_healthy_participants_clear_the_room);
    run("test_shutdown_forces_slow_client_closed_after_grace_period", test_shutdown_forces_slow_client_closed_after_grace_period);
    run("test_termination_signal_stops_server(SIGINT)",
        []
        {
            test_termination_signal_stops_server(SIGINT);
        });
    run("test_termination_signal_stops_server(SIGTERM)",
        []
        {
            test_termination_signal_stops_server(SIGTERM);
        });
    run("test_actor_runtime_failure_aborts_without_waiting_for_grace_period", test_actor_runtime_failure_aborts_without_waiting_for_grace_period);
    run("test_retries_a_full_connection_closed_post_without_duplicate_after_acceptance",
        test_retries_a_full_connection_closed_post_without_duplicate_after_acceptance);
    run("test_bounds_pending_connection_closes_and_rejects_new_connections_at_capacity",
        test_bounds_pending_connection_closes_and_rejects_new_connections_at_capacity);
    run("test_shutdown_waits_for_reactor_control_state_after_logic_drains", test_shutdown_waits_for_reactor_control_state_after_logic_drains);
}
