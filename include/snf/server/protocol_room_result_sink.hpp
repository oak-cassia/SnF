#pragma once

#include "snf/game/room_result.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_session_directory.hpp"
#include "snf/server/room_inbound_command.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace snf::server
{
    struct ProtocolRoomResultSinkStats
    {
        std::uint64_t oversized_battle_digests{0};
    };

    // Translates game results into request replies and unsolicited party facts. It
    // never decides combat and never writes a socket directly.
    class ProtocolRoomResultSink
    {
    public:
        ProtocolRoomResultSink(OutboundSink& outbound, const PlayerSessionDirectory& sessions) noexcept;

        void accept(const RoomInboundCommand& command, const RoomResult& result);
        [[nodiscard]] ProtocolRoomResultSinkStats stats() const noexcept;

    private:
        void publishReply(const RoomInboundCommand& command, const RoomResult& result);
        void publishDigest(const RoomResult& result);
        void publishClear(const RoomResult& result);
        void publishFailure(const RoomResult& result);
        [[nodiscard]] std::optional<std::size_t> digestPayloadSize(const BattleDigest& digest) const noexcept;
        [[nodiscard]] std::vector<std::byte> digestPayload(const RoomResult& result) const;
        [[nodiscard]] bool send(snf::net::ConnectionId connection, snf::protocol::Frame frame);

        OutboundSink& _outbound;
        const PlayerSessionDirectory& _sessions;
        std::atomic<std::uint64_t> _oversized_battle_digests{0};
    };
}
