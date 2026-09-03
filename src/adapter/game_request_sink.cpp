#include "snf/adapter/game_request_sink.hpp"

#include "snf/adapter/game_payloads.hpp"

#include <cassert>

namespace snf::adapter
{
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
        if (const auto bound = playerFor(connection))
        {
            if (*bound != *player)
            {
                return snf::worker::RequestPostResult::Invalid;
            }
        }

        auto envelope = GameActorPayloadRegistry::create(PlayerCommandMessage{
            .connection = connection,
            .request_id = frame.request_id,
            .command = snf::server::PlayerCommand{snf::server::AuthenticateCommand{.player = *player}},
        });

        // The actor decides whether this player is already bound to another live
        // connection, so the session entry is only written once the command is
        // accepted. A rejected delivery must not leave the connection looking
        // authenticated.
        if (_worker->tell(playerKey(*player), std::move(envelope)) != snf::worker::DeliveryResult::Accepted)
        {
            return snf::worker::RequestPostResult::Rejected;
        }

        const auto inserted = _sessions.insert_or_assign(connection.id.value, Session{.generation = connection.generation, .player = *player});
        if (inserted.second)
        {
            _live_sessions.fetch_add(1, std::memory_order_relaxed);
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
        const auto session = _sessions.find(connection.id.value);
        if (session == _sessions.end() || session->second.generation != connection.generation)
        {
            return;
        }

        // Release the other half of the identity too. Without this the actor keeps
        // the dead ConnectionRef and refuses this player's next connection as a
        // conflict, so a disconnected player could never reconnect. A refused
        // delivery is ignored: it means the actor is already gone, which releases
        // the binding anyway.
        const snf::server::PlayerId player = session->second.player;
        _sessions.erase(session);
        _live_sessions.fetch_sub(1, std::memory_order_relaxed);
        static_cast<void>(_worker->tell(playerKey(player), GameActorPayloadRegistry::create(PlayerConnectionClosedMessage{.connection = connection}))
        );
    }
}
