#pragma once

#include "snf/game/room_result.hpp"
#include "snf/game/zone_command.hpp"
#include "snf/game/zone_id.hpp"
#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_session_directory.hpp"
#include "snf/server/room_inbound_command.hpp"

#include <atomic>
#include <cstddef>
#include <optional>
#include <vector>

namespace snf::server
{
    struct ProtocolRoomResultSinkStats
    {
        std::uint64_t oversized_battle_digests{0};
        std::uint64_t battle_digest_frames{0};
        std::uint64_t battle_digest_fanout_bytes{0};
    };

    // Thread safety: ProtocolRoomResultSink is shared between the logic thread and the reactor thread.
    // Reply and failure reporting methods do not mutate internal sink state; stats counters are std::atomic,
    // and referenced dependencies (PlayerSessionDirectory, OutboundSink) are internally synchronized.
    class ProtocolRoomResultSink
    {
    public:
        ProtocolRoomResultSink(OutboundSink& outbound, const PlayerSessionDirectory& sessions) noexcept;

        void accept(const RoomInboundCommand& command, const RoomResult& result);
        void replyJoined(snf::net::ConnectionId connection, std::uint32_t request_id, RoomId room, RoomCommandStatus status, RoomPhase phase);
        void replyReturnedToZone(snf::net::ConnectionId connection, ZoneId zone, ZonePosition position);
        void reportAdmissionFailure(snf::net::ConnectionId connection) noexcept;
        [[nodiscard]] ProtocolRoomResultSinkStats stats() const noexcept;

    private:
        void publishReply(const RoomInboundCommand& command, const RoomResult& result);
        void publishDigest(const RoomResult& result);
        void publishClear(const RoomResult& result);
        void publishFailure(const RoomResult& result);
        [[nodiscard]] bool send(snf::net::ConnectionId connection, snf::protocol::Frame frame);

        OutboundSink& _outbound;
        const PlayerSessionDirectory& _sessions;
        std::atomic<std::uint64_t> _oversized_battle_digests{0};
        std::atomic<std::uint64_t> _battle_digest_frames{0};
        std::atomic<std::uint64_t> _battle_digest_fanout_bytes{0};
    };
}
