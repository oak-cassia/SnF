#include "snf/server/protocol_player_follow_up_sink.hpp"

#include <type_traits>
#include <utility>
#include <variant>

namespace
{
    template <typename> inline constexpr bool always_false_v = false;
}

namespace snf::server
{
    ProtocolPlayerFollowUpSink::ProtocolPlayerFollowUpSink(OutboundSink& outbound) noexcept
        : _outbound(outbound)
    {
    }

    std::size_t ProtocolPlayerFollowUpSink::requiredSlots(const PlayerResult& result) const noexcept
    {
        std::size_t slots = 0;
        for (const FollowUpAction& follow_up : result.follow_ups)
        {
            if (std::holds_alternative<SendResponse>(follow_up))
            {
                ++slots;
            }
        }
        return slots;
    }

    bool ProtocolPlayerFollowUpSink::applyFollowUps(const snf::net::ConnectionId connection,
                                                    PlayerResult result,
                                                    OutboundReservation& reservation)
    {
        for (const FollowUpAction& follow_up : result.follow_ups)
        {
            const bool emitted = std::visit(
                [this, connection, &reservation](auto value) -> bool
                {
                    using Action = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<Action, SendResponse>)
                    {
                        return _outbound.commit(reservation,
                                                SendFrame{
                                                    .connection = connection,
                                                    .frame = _response_mapper.map(value.response),
                                                });
                    }
                    else
                    {
                        static_assert(always_false_v<Action>,
                                      "Unhandled FollowUpAction alternative");
                    }
                },
                follow_up);

            if (!emitted)
            {
                return false;
            }
        }

        return true;
    }
}
