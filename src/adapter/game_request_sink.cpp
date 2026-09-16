#include "snf/adapter/game_request_sink.hpp"

#include "snf/adapter/game_payloads.hpp"
#include "snf/game/arena.hpp"

#include <algorithm>
#include <cassert>

namespace snf::adapter
{
    void GameRequestSink::cancelConnectionCloseRetries() noexcept
    {
        assertOwnerThread();
        _cancelled_closes.fetch_add(_pending_closes, std::memory_order_relaxed);
        _records = {};
        _sessions.clear();
        _live_sessions.store(0, std::memory_order_relaxed);
        _pending_closes = 0;
        _pending_closes_atomic.store(0, std::memory_order_relaxed);
        _next_retry_deadline.reset();
        _retry_cursor = 0;
    }

    namespace
    {
        constexpr std::size_t PLAYER_ID_WIRE_SIZE = 8;

        // Big-endian player id, the same wire form the legacy dispatcher decoded.
        // Zero is not a player, and a payload of any other length is malformed.
        [[nodiscard]] std::optional<snf::server::PlayerId> decodePlayerId(const std::vector<std::byte>& payload)
        {
            if (payload.size() != PLAYER_ID_WIRE_SIZE)
            {
                return std::nullopt;
            }

            std::uint64_t value = 0;
            for (const std::byte byte : payload)
            {
                value = (value << 8U) | std::to_integer<std::uint64_t>(byte);
            }
            if (value == 0)
            {
                return std::nullopt;
            }
            return snf::server::PlayerId{.value = value};
        }

        [[nodiscard]] snf::worker::ActorKey playerKey(const snf::server::PlayerId player) noexcept
        {
            return snf::worker::ActorKey{.kind = snf::worker::ActorKind::Player, .entity = player.value};
        }

        // Big-endian unsigned integer over an exact byte range.
        template <class Value>
        [[nodiscard]] Value decodeBigEndian(const std::vector<std::byte>& payload, const std::size_t offset, const std::size_t size) noexcept
        {
            Value value = 0;
            for (std::size_t index = offset; index < offset + size; ++index)
            {
                value = static_cast<Value>((value << 8U) | std::to_integer<Value>(payload[index]));
            }
            return value;
        }

        constexpr std::size_t PURCHASE_WIRE_SIZE = 12;
        constexpr std::size_t EQUIP_SKILL_WIRE_SIZE = 4;

        // 8-byte idempotency key then a 4-byte product id, both non-zero. Same
        // wire form the legacy dispatcher decoded.
        [[nodiscard]] std::optional<snf::server::PlayerCommand> decodePurchase(const std::vector<std::byte>& payload)
        {
            if (payload.size() != PURCHASE_WIRE_SIZE)
            {
                return std::nullopt;
            }
            const auto key = decodeBigEndian<std::uint64_t>(payload, 0, 8);
            const auto product = decodeBigEndian<std::uint32_t>(payload, 8, 4);
            if (key == 0 || product == 0)
            {
                return std::nullopt;
            }
            return snf::server::PlayerCommand{snf::server::PurchaseCommand{
                .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = key},
                .product = snf::server::ProductId{.value = product},
            }};
        }

        [[nodiscard]] std::optional<snf::server::PlayerCommand> decodeEquipSkill(const std::vector<std::byte>& payload)
        {
            if (payload.size() != EQUIP_SKILL_WIRE_SIZE)
            {
                return std::nullopt;
            }
            const auto skill_id = decodeBigEndian<std::uint32_t>(payload, 0, EQUIP_SKILL_WIRE_SIZE);
            if (skill_id == 0)
            {
                return std::nullopt;
            }
            return snf::server::PlayerCommand{snf::server::EquipSkillCommand{
                .skill_id = snf::server::SkillId{.value = skill_id},
            }};
        }

        constexpr std::size_t ENTER_ZONE_WIRE_SIZE = 16;
        constexpr std::size_t MOVE_WIRE_SIZE = 8;

        [[nodiscard]] std::optional<ZoneRequest> decodeEnterZone(const std::vector<std::byte>& payload)
        {
            if (payload.size() != ENTER_ZONE_WIRE_SIZE)
            {
                return std::nullopt;
            }
            const auto zone = decodeBigEndian<std::uint64_t>(payload, 0, 8);
            const auto x = decodeBigEndian<std::uint32_t>(payload, 8, 4);
            const auto y = decodeBigEndian<std::uint32_t>(payload, 12, 4);
            return ZoneRequest{EnterZoneRequest{
                .zone = snf::server::ZoneId{.value = zone},
                .position =
                    snf::server::ZonePosition{
                        .x = static_cast<std::int32_t>(x),
                        .y = static_cast<std::int32_t>(y),
                    },
            }};
        }

        [[nodiscard]] std::optional<ZoneRequest> decodeMove(const std::vector<std::byte>& payload)
        {
            if (payload.size() != MOVE_WIRE_SIZE)
            {
                return std::nullopt;
            }
            const auto x = decodeBigEndian<std::uint32_t>(payload, 0, 4);
            const auto y = decodeBigEndian<std::uint32_t>(payload, 4, 4);
            return ZoneRequest{MoveRequest{
                .position =
                    snf::server::ZonePosition{
                        .x = static_cast<std::int32_t>(x),
                        .y = static_cast<std::int32_t>(y),
                    },
            }};
        }

        [[nodiscard]] std::optional<ZoneRequest> decodeLeave(const std::vector<std::byte>& payload)
        {
            if (!payload.empty())
            {
                return std::nullopt;
            }
            return ZoneRequest{LeaveRequest{}};
        }

        constexpr std::size_t ROOM_JOIN_WIRE_SIZE = 8;
        constexpr std::size_t BATTLE_START_WIRE_SIZE = 8;
        constexpr std::size_t USE_SKILL_WIRE_SIZE = 20;
        constexpr std::size_t SET_MOVE_INTENT_WIRE_SIZE = 17;

        [[nodiscard]] std::optional<RoomRequest> decodeRoomJoin(const std::vector<std::byte>& payload)
        {
            if (payload.size() != ROOM_JOIN_WIRE_SIZE)
            {
                return std::nullopt;
            }
            const auto room = decodeBigEndian<std::uint64_t>(payload, 0, 8);
            return RoomRequest{RoomJoinRequest{.room = snf::server::RoomId{.value = room}}};
        }

        [[nodiscard]] std::optional<RoomRequest> decodeBattleStart(const std::vector<std::byte>& payload)
        {
            if (payload.size() != BATTLE_START_WIRE_SIZE)
            {
                return std::nullopt;
            }
            const auto room = decodeBigEndian<std::uint64_t>(payload, 0, 8);
            return RoomRequest{BattleStartRequest{.room = snf::server::RoomId{.value = room}}};
        }

        [[nodiscard]] std::optional<RoomRequest> decodeRoomLeave(const std::vector<std::byte>& payload)
        {
            if (!payload.empty())
            {
                return std::nullopt;
            }
            return RoomRequest{RoomLeaveRequest{}};
        }

        [[nodiscard]] std::optional<RoomRequest> decodeUseSkill(const std::vector<std::byte>& payload)
        {
            if (payload.size() != USE_SKILL_WIRE_SIZE)
            {
                return std::nullopt;
            }
            const auto room = decodeBigEndian<std::uint64_t>(payload, 0, 8);
            const auto skill = decodeBigEndian<std::uint32_t>(payload, 8, 4);
            const auto seq = decodeBigEndian<std::uint64_t>(payload, 12, 8);
            if (skill == 0 || seq == 0)
            {
                return std::nullopt;
            }
            return RoomRequest{UseSkillRequest{
                .room = snf::server::RoomId{.value = room},
                .skill_id = snf::server::SkillId{.value = skill},
                .request_sequence = seq,
            }};
        }

        [[nodiscard]] std::optional<RoomRequest> decodeSetMoveIntent(const std::vector<std::byte>& payload)
        {
            if (payload.size() != SET_MOVE_INTENT_WIRE_SIZE)
            {
                return std::nullopt;
            }
            const auto room = decodeBigEndian<std::uint64_t>(payload, 0, 8);
            const auto dir_byte = std::to_integer<std::uint8_t>(payload[8]);
            const auto seq = decodeBigEndian<std::uint64_t>(payload, 9, 8);
            if (!snf::server::isValidMoveDirection(dir_byte) || seq == 0)
            {
                return std::nullopt;
            }
            return RoomRequest{SetMoveIntentRequest{
                .room = snf::server::RoomId{.value = room},
                .direction = static_cast<snf::server::MoveDirection>(dir_byte),
                .request_sequence = seq,
            }};
        }
    }

    void GameRequestSink::assertOwnerThread() noexcept
    {
        const auto current = std::this_thread::get_id();
        if (_owner_thread == std::thread::id{})
        {
            _owner_thread = current;
            return;
        }
        assert(_owner_thread == current && "GameRequestSink is shared between Worker threads");
    }

    std::optional<snf::server::PlayerId> GameRequestSink::playerFor(const snf::worker::ConnectionRef connection) const
    {
        const auto session = _sessions.find(connection.id.value);
        if (session == _sessions.end() || session->second.generation != connection.generation)
        {
            return std::nullopt;
        }
        return session->second.player;
    }

    snf::worker::RequestPostResult GameRequestSink::postAuthenticate(const snf::worker::ConnectionRef connection, const snf::protocol::Frame& frame)
    {
        const auto player = decodePlayerId(frame.payload);
        if (!player)
        {
            return snf::worker::RequestPostResult::Invalid;
        }

        // Re-authentication on the same connection. The legacy directory answered
        // AlreadyAttached for the same player and ConnectionConflict for a
        // different one, and only the first of those was allowed to proceed.
        const auto session_it = _sessions.find(connection.id.value);
        if (session_it != _sessions.end())
        {
            if (session_it->second.generation != connection.generation)
            {
                return snf::worker::RequestPostResult::Invalid;
            }

            if (session_it->second.player != *player)
            {
                return snf::worker::RequestPostResult::Invalid;
            }

            auto envelope = GameActorPayloadRegistry::create(PlayerCommandMessage{
                .connection = connection,
                .request_id = frame.request_id,
                .command = snf::server::PlayerCommand{snf::server::AuthenticateCommand{.player = *player}},
            });
            if (_worker->tell(playerKey(*player), std::move(envelope)) != snf::worker::DeliveryResult::Accepted)
            {
                return snf::worker::RequestPostResult::Rejected;
            }
            return snf::worker::RequestPostResult::Accepted;
        }

        // Do not register a new session if the exact full ConnectionRef is still Closing.
        for (std::size_t i = 0; i < MAX_TRACKED_SESSIONS; ++i)
        {
            if (_records[i].state == SessionRecordState::Closing && _records[i].connection == connection)
            {
                return snf::worker::RequestPostResult::Invalid;
            }
        }

        // Find a free record slot. Capacity is strictly bounded to MAX_TRACKED_SESSIONS.
        std::optional<std::size_t> free_index;
        for (std::size_t i = 0; i < MAX_TRACKED_SESSIONS; ++i)
        {
            if (_records[i].state == SessionRecordState::Free)
            {
                free_index = i;
                break;
            }
        }

        if (!free_index.has_value())
        {
            return snf::worker::RequestPostResult::Rejected;
        }

        const std::size_t record_idx = *free_index;

        // Reserve record and session map entry before telling the actor.
        _records[record_idx] = SessionRecord{
            .state = SessionRecordState::Tracked,
            .connection = connection,
            .player = *player,
        };
        _sessions.insert_or_assign(
            connection.id.value,
            Session{
                .generation = connection.generation,
                .player = *player,
                .record_index = record_idx,
            }
        );
        _live_sessions.fetch_add(1, std::memory_order_relaxed);

        auto envelope = GameActorPayloadRegistry::create(PlayerCommandMessage{
            .connection = connection,
            .request_id = frame.request_id,
            .command = snf::server::PlayerCommand{snf::server::AuthenticateCommand{.player = *player}},
        });

        // The actor decides whether this player is already bound to another live
        // connection, so the session entry is rolled back if rejected.
        if (_worker->tell(playerKey(*player), std::move(envelope)) != snf::worker::DeliveryResult::Accepted)
        {
            _sessions.erase(connection.id.value);
            _records[record_idx] = SessionRecord{};
            _live_sessions.fetch_sub(1, std::memory_order_relaxed);
            return snf::worker::RequestPostResult::Rejected;
        }

        return snf::worker::RequestPostResult::Accepted;
    }

    snf::worker::RequestPostResult GameRequestSink::postPing(const snf::worker::ConnectionRef connection, snf::protocol::Frame&& frame)
    {
        // Ping is served before authentication, so it must not create a Player
        // actor: an unauthenticated connection would otherwise be able to
        // allocate actor slots. The legacy path routed it to a provisional actor
        // keyed by connection, which then blocked that connection from ever
        // authenticating. That side effect is not preserved deliberately.
        const auto player = playerFor(connection);
        if (!player)
        {
            const auto request_id = frame.request_id;
            snf::protocol::Frame pong{
                .type = snf::protocol::MessageType::Pong,
                .request_id = request_id,
                .payload = std::move(frame.payload),
            };
            static_cast<void>(_worker->send(connection, std::move(pong), false));
            return snf::worker::RequestPostResult::Accepted;
        }

        auto envelope = GameActorPayloadRegistry::create(PingMessage{
            .connection = connection,
            .request_id = frame.request_id,
            .payload = std::move(frame.payload),
        });
        return _worker->tell(playerKey(*player), std::move(envelope)) == snf::worker::DeliveryResult::Accepted
                   ? snf::worker::RequestPostResult::Accepted
                   : snf::worker::RequestPostResult::Rejected;
    }

    // Commands that belong to the authenticated player itself. The session gives
    // the route, so there is no lookup beyond it.
    snf::worker::RequestPostResult GameRequestSink::postPlayerCommand(
        const snf::worker::ConnectionRef connection,
        const snf::protocol::Frame& frame,
        const PlayerCommandDecoder decoder
    )
    {
        // Frame order: these need a persistent player, which is what the legacy
        // requires_persistent_player check enforced before routing.
        const auto player = playerFor(connection);
        if (!player)
        {
            return snf::worker::RequestPostResult::Invalid;
        }

        auto command = decoder(frame.payload);
        if (!command)
        {
            return snf::worker::RequestPostResult::Invalid;
        }

        auto envelope = GameActorPayloadRegistry::create(PlayerCommandMessage{
            .connection = connection,
            .request_id = frame.request_id,
            .command = std::move(*command),
        });
        // A full mailbox is overload, not a domain failure, so it stays Rejected.
        // Domain outcomes such as a refused purchase travel back as a response
        // frame from the actor turn instead.
        return _worker->tell(playerKey(*player), std::move(envelope)) == snf::worker::DeliveryResult::Accepted
                   ? snf::worker::RequestPostResult::Accepted
                   : snf::worker::RequestPostResult::Rejected;
    }

    snf::worker::RequestPostResult GameRequestSink::postZoneRequest(
        const snf::worker::ConnectionRef connection,
        const snf::protocol::Frame& frame,
        const ZoneRequestDecoder decoder
    )
    {
        const auto player = playerFor(connection);
        if (!player)
        {
            return snf::worker::RequestPostResult::Invalid;
        }

        auto request = decoder(frame.payload);
        if (!request)
        {
            return snf::worker::RequestPostResult::Invalid;
        }

        auto envelope = GameActorPayloadRegistry::create(PlayerZoneRequestMessage{
            .connection = connection,
            .request_id = frame.request_id,
            .request = std::move(*request),
        });
        return _worker->tell(playerKey(*player), std::move(envelope)) == snf::worker::DeliveryResult::Accepted
                   ? snf::worker::RequestPostResult::Accepted
                   : snf::worker::RequestPostResult::Rejected;
    }

    snf::worker::RequestPostResult GameRequestSink::postRoomRequest(
        const snf::worker::ConnectionRef connection,
        const snf::protocol::Frame& frame,
        const RoomRequestDecoder decoder
    )
    {
        const auto player = playerFor(connection);
        if (!player)
        {
            return snf::worker::RequestPostResult::Invalid;
        }

        auto request = decoder(frame.payload);
        if (!request)
        {
            return snf::worker::RequestPostResult::Invalid;
        }

        auto envelope = GameActorPayloadRegistry::create(PlayerRoomRequestMessage{
            .connection = connection,
            .request_id = frame.request_id,
            .request = std::move(*request),
        });
        return _worker->tell(playerKey(*player), std::move(envelope)) == snf::worker::DeliveryResult::Accepted
                   ? snf::worker::RequestPostResult::Accepted
                   : snf::worker::RequestPostResult::Rejected;
    }

    snf::worker::RequestPostResult GameRequestSink::tryPost(const snf::worker::ConnectionRef connection, snf::protocol::Frame&& frame)
    {
        assert(_worker != nullptr);
        assertOwnerThread();

        switch (frame.type)
        {
        case snf::protocol::MessageType::Authenticate:
            return postAuthenticate(connection, frame);
        case snf::protocol::MessageType::Ping:
            return postPing(connection, std::move(frame));
        case snf::protocol::MessageType::Purchase:
            return postPlayerCommand(connection, frame, decodePurchase);
        case snf::protocol::MessageType::EquipSkill:
            return postPlayerCommand(connection, frame, decodeEquipSkill);
        case snf::protocol::MessageType::EnterZone:
            return postZoneRequest(connection, frame, decodeEnterZone);
        case snf::protocol::MessageType::Move:
            return postZoneRequest(connection, frame, decodeMove);
        case snf::protocol::MessageType::LeaveZone:
            return postZoneRequest(connection, frame, decodeLeave);
        case snf::protocol::MessageType::RoomJoin:
            return postRoomRequest(connection, frame, decodeRoomJoin);
        case snf::protocol::MessageType::BattleStart:
            return postRoomRequest(connection, frame, decodeBattleStart);
        case snf::protocol::MessageType::RoomLeave:
            return postRoomRequest(connection, frame, decodeRoomLeave);
        case snf::protocol::MessageType::UseSkill:
            return postRoomRequest(connection, frame, decodeUseSkill);
        case snf::protocol::MessageType::SetMoveIntent:
            return postRoomRequest(connection, frame, decodeSetMoveIntent);
        default:
            break;
        }

        // Frames whose routing is not implemented yet are refused the same way the
        // previous revision refused them, which also keeps every one of them
        // outside the pre-auth boundary.
        return snf::worker::RequestPostResult::Invalid;
    }

    void GameRequestSink::onConnectionClosed(const snf::worker::ConnectionRef connection, const snf::worker::CloseReason)
    {
        assertOwnerThread();
        if (_worker == nullptr || connection.owner != _worker->id())
        {
            return;
        }

        const auto session_it = _sessions.find(connection.id.value);
        if (session_it == _sessions.end() || session_it->second.generation != connection.generation)
        {
            return;
        }

        const std::size_t record_idx = session_it->second.record_index;
        if (record_idx >= MAX_TRACKED_SESSIONS)
        {
            return;
        }

        auto& record = _records[record_idx];
        if (record.state != SessionRecordState::Tracked || record.connection != connection || record.player != session_it->second.player)
        {
            return;
        }

        // 1. Transition record to Closing and configure pending count and deadline first.
        record.state = SessionRecordState::Closing;
        ++_pending_closes;
        _pending_closes_atomic.fetch_add(1, std::memory_order_release);

        const auto now = std::chrono::steady_clock::now();
        const auto pass_deadline = now + std::chrono::milliseconds(10);
        if (!_next_retry_deadline.has_value())
        {
            _next_retry_deadline = pass_deadline;
        }
        else
        {
            _next_retry_deadline = std::min(*_next_retry_deadline, pass_deadline);
        }

        // 2. Remove open session map entry and decrement live session count.
        const snf::server::PlayerId player = record.player;
        const snf::worker::ConnectionRef orig_connection = record.connection;
        _sessions.erase(session_it);
        _live_sessions.fetch_sub(1, std::memory_order_relaxed);

        // 3. Attempt first notification via notifyActorConnectionClosed.
        auto envelope = GameActorPayloadRegistry::create(PlayerConnectionClosedMessage{
            .connection = orig_connection,
        });
        static_cast<void>(_worker->notifyActorConnectionClosed(playerKey(player), orig_connection, std::move(envelope)));
    }

    void GameRequestSink::onActorConnectionClosedReceipt(
        const snf::worker::ActorKey key,
        const snf::worker::ConnectionRef connection,
        const snf::worker::ActorConnectionClosedResult result
    )
    {
        assertOwnerThread();
        if (_worker == nullptr || connection.owner != _worker->id())
        {
            return;
        }

        if (result != snf::worker::ActorConnectionClosedResult::MailboxAccepted && result != snf::worker::ActorConnectionClosedResult::ActorAbsent)
        {
            return;
        }

        for (std::size_t i = 0; i < MAX_TRACKED_SESSIONS; ++i)
        {
            auto& rec = _records[i];
            if (rec.state == SessionRecordState::Closing && rec.connection == connection && playerKey(rec.player) == key)
            {
                rec.state = SessionRecordState::Free;
                rec.connection = snf::worker::ConnectionRef{};
                rec.player = snf::server::PlayerId{};
                assert(_pending_closes > 0);
                --_pending_closes;
                _pending_closes_atomic.fetch_sub(1, std::memory_order_release);
                if (_pending_closes == 0)
                {
                    _next_retry_deadline = std::nullopt;
                }
                break;
            }
        }
    }

    std::optional<std::chrono::steady_clock::time_point> GameRequestSink::nextActorConnectionCloseRetryDeadline() const noexcept
    {
        if (_pending_closes == 0)
        {
            return std::nullopt;
        }
        return _next_retry_deadline;
    }

    void GameRequestSink::retryActorConnectionClosed(const std::chrono::steady_clock::time_point now, const snf::worker::CountTimeBudget& budget)
    {
        assertOwnerThread();
        if (_worker == nullptr || _pending_closes == 0)
        {
            return;
        }

        if (!_next_retry_deadline.has_value() || *_next_retry_deadline > now)
        {
            return;
        }

        const std::size_t scan_limit = std::min(budget.max_count, MAX_TRACKED_SESSIONS);
        std::size_t scanned = 0;
        const auto start_time = std::chrono::steady_clock::now();

        if (budget.max_count > 0 && budget.max_duration > std::chrono::nanoseconds{0})
        {
            while (scanned < scan_limit && _pending_closes > 0)
            {
                if (scanned > 0 && (std::chrono::steady_clock::now() - start_time >= budget.max_duration))
                {
                    break;
                }

                const std::size_t idx = _retry_cursor;
                _retry_cursor = (_retry_cursor + 1) % MAX_TRACKED_SESSIONS;
                ++scanned;

                if (_records[idx].state == SessionRecordState::Closing)
                {
                    const snf::worker::ConnectionRef conn = _records[idx].connection;
                    const snf::server::PlayerId player = _records[idx].player;

                    auto envelope = GameActorPayloadRegistry::create(PlayerConnectionClosedMessage{
                        .connection = conn,
                    });
                    static_cast<void>(_worker->notifyActorConnectionClosed(playerKey(player), conn, std::move(envelope)));
                }
            }
        }

        const auto finished_at = std::chrono::steady_clock::now();
        if (_pending_closes > 0)
        {
            _next_retry_deadline = std::max(now, finished_at) + std::chrono::milliseconds(10);
        }
        else
        {
            _next_retry_deadline = std::nullopt;
        }
    }
}
