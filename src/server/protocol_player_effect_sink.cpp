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

    bool ProtocolPlayerEffectSink::apply(const snf::net::ConnectionId connection,
                                         PlayerResult result,
                                         const std::stop_token stop_token)
    {
        for (const PlayerEffect& effect : result.effects)
        {
            const bool applied = std::visit(
                [this, connection, stop_token](const auto& value) -> bool
                {
                    using Effect = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<Effect, SendResponse>)
                    {
                        return _outbound.publish(
                            SendFrame{
                                .connection = connection,
                                .frame = _response_mapper.map(value.response),
                            },
                            stop_token);
                    }
                    else
                    {
                        static_assert(always_false_v<Effect>, "Unhandled PlayerEffect alternative");
                    }
                },
                effect);

            if (!applied)
            {
                return false;
            }
        }

        return true;
    }
}
