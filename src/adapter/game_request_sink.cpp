#include "snf/adapter/game_request_sink.hpp"

#include "snf/adapter/game_payloads.hpp"

#include <cassert>

namespace snf::adapter
{
    snf::worker::RequestPostResult GameRequestSink::tryPost(
        const snf::worker::ConnectionRef connection,
        snf::protocol::Frame&& frame
    )
    {
        assert(_worker != nullptr);

        if (frame.type == snf::protocol::MessageType::Ping)
        {
            const auto req_id = frame.request_id;
            const snf::worker::ActorKey key{
                .kind = snf::worker::ActorKind::Player,
                .entity = connection.id.value,
            };

            auto envelope = GameActorPayloadRegistry::create(PingMessage{
                .connection = connection,
                .request_id = req_id,
                .payload = std::move(frame.payload),
            });

            const auto result = _worker->tell(key, std::move(envelope));
            if (result == snf::worker::DeliveryResult::Accepted)
            {
                return snf::worker::RequestPostResult::Accepted;
            }
            return snf::worker::RequestPostResult::Rejected;
        }

        return snf::worker::RequestPostResult::Rejected;
    }
}
