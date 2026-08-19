#pragma once

#include "snf/server/outbound_sink.hpp"
#include "snf/server/player_session_directory.hpp"
#include "snf/server/room_inbound_command.hpp"
#include "snf/server/room_result.hpp"

namespace snf::server
{
    // The Room's protocol boundary. It carries two kinds of frame: the answer to a
    // request, and the notification a clear produces on the Room's own timer.
    //
    // The second is why this sink needs the session directory. A Room names its
    // participants by PlayerId and knows nothing about connections, which is the
    // property that keeps it a game model; resolving one to the other is a server
    // concern and belongs here.
    class ProtocolRoomResultSink
    {
    public:
        ProtocolRoomResultSink(OutboundSink& outbound, const PlayerSessionDirectory& sessions) noexcept;

        void accept(const RoomInboundCommand& command, const RoomResult& result);

    private:
        void publishReply(const RoomInboundCommand& command, const RoomResult& result);
        void publishClear(const RoomResult& result);
        [[nodiscard]] bool send(snf::net::ConnectionId connection, snf::protocol::Frame frame);

        OutboundSink& _outbound;
        const PlayerSessionDirectory& _sessions;
    };
}
