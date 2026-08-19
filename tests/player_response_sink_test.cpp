#include "outbound_reservation_test_support.hpp"
#include "snf/server/protocol_player_response_sink.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    // The real channel, because a reservation can only come from one: a follow-up sink
    // consumes capacity it did not create, and a stub that mints capacity would test
    // the wrong contract.
    struct ResponseSinkFixture
    {
        explicit ResponseSinkFixture(const std::size_t capacity)
            : wake(snf::test::make_wake_descriptor())
            , channel(snf::server::OutboundChannelConfig{.capacity = capacity, .max_slots_per_connection = capacity}, wake.getDescriptor())
            , response_sink(channel)
        {
        }

        [[nodiscard]] snf::server::OutboundReservation reserve(const snf::net::ConnectionId connection, const std::size_t slots)
        {
            auto reservation = channel.tryReserve(connection, slots);
            assert(reservation);
            return std::move(*reservation);
        }

        [[nodiscard]] std::uint32_t popRequestId()
        {
            const auto posted = channel.tryPop();
            assert(posted.has_value());
            const auto* send = std::get_if<snf::server::SendFrame>(&posted->action);
            assert(send != nullptr);
            return send->frame.request_id;
        }

        snf::net::UniqueFileDescriptor wake;
        snf::server::OutboundChannel channel;
        snf::server::ProtocolPlayerResponseSink response_sink;
    };

    snf::server::PlayerResult pong_result(const std::uint32_t request_id, std::vector<std::byte> payload = {})
    {
        return snf::server::PlayerResult{
            .responses =
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

    void test_prices_a_result_by_the_actions_it_emits()
    {
        ResponseSinkFixture fixture{4};
        auto result = pong_result(1);
        assert(fixture.response_sink.requiredSlots(result) == 1);

        result.responses.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.request_id = 2, .payload = {}},
        });
        assert(fixture.response_sink.requiredSlots(result) == 2);
        assert(fixture.response_sink.requiredSlots(snf::server::PlayerResult{}) == 0);
    }

    void test_maps_send_response_to_a_frame_for_the_same_connection()
    {
        ResponseSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto reservation = fixture.reserve(connection, 1);

        assert(fixture.response_sink.applyResponses(connection, pong_result(42, {std::byte{0xAB}}), reservation));
        assert(reservation.remainingSlots() == 0);

        const auto posted = fixture.channel.tryPop();
        assert(posted.has_value());
        const auto* sent = std::get_if<snf::server::SendFrame>(&posted->action);
        assert(sent != nullptr);
        assert(sent->connection == connection);
        assert(sent->frame.type == snf::protocol::MessageType::Pong);
        assert(sent->frame.request_id == 42);
        assert(sent->frame.payload == std::vector<std::byte>{std::byte{0xAB}});
    }

    void test_preserves_follow_up_order_and_request_ids()
    {
        ResponseSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto result = pong_result(1);
        result.responses.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.request_id = 2, .payload = {}},
        });

        auto reservation = fixture.reserve(connection, fixture.response_sink.requiredSlots(result));
        assert(fixture.response_sink.applyResponses(connection, std::move(result), reservation));
        assert(fixture.popRequestId() == 1);
        assert(fixture.popRequestId() == 2);
    }

    void test_an_empty_result_consumes_no_capacity()
    {
        ResponseSinkFixture fixture{1};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto reservation = fixture.reserve(connection, 0);

        assert(fixture.response_sink.applyResponses(connection, snf::server::PlayerResult{}, reservation));
        assert(fixture.channel.size() == 0);
        assert(fixture.channel.reservedSlotCount() == 0);
    }

    void test_a_cancelled_channel_rejects_the_emission()
    {
        ResponseSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto reservation = fixture.reserve(connection, 1);
        static_cast<void>(fixture.channel.cancel());

        assert(!fixture.response_sink.applyResponses(connection, pong_result(1), reservation));
        assert(fixture.channel.size() == 0);
    }

    void test_an_underpriced_reservation_keeps_the_responses_it_already_applied()
    {
        ResponseSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto result = pong_result(1);
        result.responses.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.request_id = 2, .payload = {}},
        });

        // One slot for two follow-ups: application is not a transaction, so the first
        // stays emitted and the shortfall surfaces as a broken invariant rather than a
        // silently dropped response.
        auto reservation = fixture.reserve(connection, 1);
        bool shortfall_reported = false;
        try
        {
            static_cast<void>(fixture.response_sink.applyResponses(connection, std::move(result), reservation));
        }
        catch (const std::logic_error&)
        {
            shortfall_reported = true;
        }

        assert(shortfall_reported);
        assert(fixture.channel.size() == 1);
        assert(fixture.popRequestId() == 1);
    }
}

void run_player_response_sink_tests()
{
    test_prices_a_result_by_the_actions_it_emits();
    test_maps_send_response_to_a_frame_for_the_same_connection();
    test_preserves_follow_up_order_and_request_ids();
    test_an_empty_result_consumes_no_capacity();
    test_a_cancelled_channel_rejects_the_emission();
    test_an_underpriced_reservation_keeps_the_responses_it_already_applied();
}
