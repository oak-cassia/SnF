#include "snf/server/protocol_player_effect_sink.hpp"

#include <type_traits>
#include <utility>
#include <variant>

namespace
{
    template <typename> inline constexpr bool always_false_v = false;
}

namespace snf::server
{
    ProtocolPlayerEffectSink::ProtocolPlayerEffectSink(OutboundSink& outbound) noexcept
        : _outbound(outbound)
    {
    }

    std::size_t ProtocolPlayerEffectSink::requiredSlots(const PlayerResult& result) const noexcept
    {
        // Every effect currently maps onto exactly one outbound action. An effect that
        // emits more than one has to be priced here, not at the reservation site.
        return result.effects.size();
    }

    bool ProtocolPlayerEffectSink::commit(const snf::net::ConnectionId connection,
                                          PlayerResult result,
                                          OutboundReservation& reservation)
    {
        for (const PlayerEffect& effect : result.effects)
        {
            const bool emitted = std::visit(
                [this, connection, &reservation](const auto& value) -> bool
                {
                    using Effect = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<Effect, SendResponse>)
                    {
                        return _outbound.commit(reservation,
                                                SendFrame{
                                                    .connection = connection,
                                                    .frame = _response_mapper.map(value.response),
                                                });
                    }
                    else
                    {
                        static_assert(always_false_v<Effect>, "Unhandled PlayerEffect alternative");
                    }
                },
                effect);

            if (!emitted)
            {
                return false;
            }
        }

        return true;
    }
}
