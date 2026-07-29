#include "snf/server/protocol_player_effect_sink.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stop_token>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    class RecordingOutboundSink final : public snf::server::OutboundSink
    {
    public:
        [[nodiscard]] bool publish(snf::server::OutboundAction action,
                                   const std::stop_token stop_token) override
        {
            if (stop_token.stop_requested() || published.size() == fail_after)
            {
                return false;
            }

            published.push_back(std::move(action));
            return true;
        }

        std::size_t fail_after{static_cast<std::size_t>(-1)};
        std::vector<snf::server::OutboundAction> published;
    };

    snf::server::PlayerResult pong_result(const std::uint32_t request_id,
                                          std::vector<std::byte> payload = {})
    {
        return snf::server::PlayerResult{
            .effects =
                {
                    snf::server::SendResponse{
                        .response =
                            snf::server::PongResponse{
                                .request_id = request_id,
                                .payload = std::move(payload),
                            },
                    },
                },
        };
    }

    void test_maps_send_response_to_a_frame_for_the_same_connection()
    {
        RecordingOutboundSink outbound;
        snf::server::ProtocolPlayerEffectSink effects{outbound};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};

        assert(effects.apply(connection, pong_result(42, {std::byte{0xAB}}), {}));
        assert(outbound.published.size() == 1);
        const auto* sent = std::get_if<snf::server::SendFrame>(&outbound.published.front());
        assert(sent != nullptr);
        assert(sent->connection == connection);
        assert(sent->frame.type == snf::protocol::MessageType::Pong);
        assert(sent->frame.request_id == 42);
        assert(sent->frame.payload == std::vector<std::byte>{std::byte{0xAB}});
    }

    void test_preserves_effect_order_and_request_ids()
    {
        RecordingOutboundSink outbound;
        snf::server::ProtocolPlayerEffectSink effects{outbound};
        auto result = pong_result(1);
        result.effects.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.request_id = 2, .payload = {}},
        });

        assert(effects.apply({.descriptor = 4, .generation = 9}, std::move(result), {}));
        assert(outbound.published.size() == 2);
        assert(std::get<snf::server::SendFrame>(outbound.published[0]).frame.request_id == 1);
        assert(std::get<snf::server::SendFrame>(outbound.published[1]).frame.request_id == 2);
    }

    void test_stop_or_outbound_failure_rejects_the_result()
    {
        RecordingOutboundSink outbound;
        snf::server::ProtocolPlayerEffectSink effects{outbound};
        std::stop_source stopped;
        stopped.request_stop();
        assert(!effects.apply(
            {.descriptor = 4, .generation = 9}, pong_result(1), stopped.get_token()));
        assert(outbound.published.empty());

        outbound.fail_after = 0;
        assert(!effects.apply({.descriptor = 4, .generation = 9}, pong_result(2), {}));
    }

    void test_keeps_already_published_effects_when_a_later_effect_fails()
    {
        RecordingOutboundSink outbound;
        outbound.fail_after = 1;
        snf::server::ProtocolPlayerEffectSink effects{outbound};
        auto result = pong_result(1);
        result.effects.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.request_id = 2, .payload = {}},
        });

        assert(!effects.apply({.descriptor = 4, .generation = 9}, std::move(result), {}));
        assert(outbound.published.size() == 1);
        assert(std::get<snf::server::SendFrame>(outbound.published.front()).frame.request_id == 1);
    }
}

void run_player_effect_sink_tests()
{
    test_maps_send_response_to_a_frame_for_the_same_connection();
    test_preserves_effect_order_and_request_ids();
    test_stop_or_outbound_failure_rejects_the_result();
    test_keeps_already_published_effects_when_a_later_effect_fails();
}
