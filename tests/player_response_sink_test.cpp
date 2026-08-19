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

        [[nodiscard]] std::vector<std::byte> popPayload()
        {
            const auto posted = channel.tryPop();
            assert(posted.has_value());
            const auto* send = std::get_if<snf::server::SendFrame>(&posted->action);
            assert(send != nullptr);
            return send->frame.payload;
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

    snf::server::PlayerResult pong_result(std::vector<std::byte> payload = {})
    {
        return snf::server::PlayerResult{
            .responses =
                {
                    snf::server::SendResponse{
                        .response =
                            snf::server::PongResponse{
                                .payload = std::move(payload),
                            },
                    },
                },
        };
    }

    void test_prices_a_result_by_the_actions_it_emits()
    {
        ResponseSinkFixture fixture{4};
        auto result = pong_result();
        assert(fixture.response_sink.requiredSlots(result) == 1);

        result.responses.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.payload = {}},
        });
        assert(fixture.response_sink.requiredSlots(result) == 2);
        assert(fixture.response_sink.requiredSlots(snf::server::PlayerResult{}) == 0);
    }

    void test_maps_send_response_to_a_frame_for_the_same_connection()
    {
        ResponseSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto reservation = fixture.reserve(connection, 1);

        assert(fixture.response_sink.applyResponses(connection, 42, pong_result({std::byte{0xAB}}), reservation));
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

    void test_every_response_answers_the_same_request_in_order()
    {
        ResponseSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto result = pong_result({std::byte{0x01}});
        result.responses.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.payload = {std::byte{0x02}}},
        });

        auto reservation = fixture.reserve(connection, fixture.response_sink.requiredSlots(result));
        // One command, one request id: every response it produced answers the same
        // frame, so the payloads are what tell them apart.
        assert(fixture.response_sink.applyResponses(connection, 7, std::move(result), reservation));
        assert((fixture.popPayload() == std::vector<std::byte>{std::byte{0x01}}));
        assert((fixture.popPayload() == std::vector<std::byte>{std::byte{0x02}}));
    }

    void test_an_empty_result_consumes_no_capacity()
    {
        ResponseSinkFixture fixture{1};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto reservation = fixture.reserve(connection, 0);

        assert(fixture.response_sink.applyResponses(connection, 1, snf::server::PlayerResult{}, reservation));
        assert(fixture.channel.size() == 0);
        assert(fixture.channel.reservedSlotCount() == 0);
    }

    void test_a_cancelled_channel_rejects_the_emission()
    {
        ResponseSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto reservation = fixture.reserve(connection, 1);
        static_cast<void>(fixture.channel.cancel());

        assert(!fixture.response_sink.applyResponses(connection, 1, pong_result(), reservation));
        assert(fixture.channel.size() == 0);
    }

    void test_an_underpriced_reservation_keeps_the_responses_it_already_applied()
    {
        ResponseSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto result = pong_result();
        result.responses.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.payload = {}},
        });

        // One slot for two follow-ups: application is not a transaction, so the first
        // stays emitted and the shortfall surfaces as a broken invariant rather than a
        // silently dropped response.
        auto reservation = fixture.reserve(connection, 1);
        bool shortfall_reported = false;
        try
        {
            static_cast<void>(fixture.response_sink.applyResponses(connection, 1, std::move(result), reservation));
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
    test_every_response_answers_the_same_request_in_order();
    test_an_empty_result_consumes_no_capacity();
    test_a_cancelled_channel_rejects_the_emission();
    test_an_underpriced_reservation_keeps_the_responses_it_already_applied();
}
