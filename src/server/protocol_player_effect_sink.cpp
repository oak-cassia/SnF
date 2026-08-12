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
    ProtocolPlayerEffectSink::ProtocolPlayerEffectSink(OutboundSink& outbound,
                                                       PlayerDomainEventSink* events) noexcept
        : _outbound(outbound)
        , _events(events)
    {
    }

    std::size_t ProtocolPlayerEffectSink::requiredSlots(const PlayerResult& result) const noexcept
    {
        std::size_t slots = 0;
        for (const PlayerEffect& effect : result.effects)
        {
            if (std::holds_alternative<SendResponse>(effect))
            {
                ++slots;
            }
        }
        return slots;
    }

    bool ProtocolPlayerEffectSink::commit(const snf::net::ConnectionId connection,
                                          PlayerResult result,
                                          OutboundReservation& reservation)
    {
        for (const PlayerEffect& effect : result.effects)
        {
            const bool emitted = std::visit(
                [this, connection, &reservation](auto value) -> bool
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
                    else if constexpr (std::is_same_v<Effect, PublishPlayerEvent>)
                    {
                        if (_events == nullptr)
                        {
                            return false;
                        }
                        const PlayerEventPublishResult status =
                            _events->publish(std::move(value.event));
                        return status == PlayerEventPublishResult::Published ||
                               status == PlayerEventPublishResult::Duplicate;
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
