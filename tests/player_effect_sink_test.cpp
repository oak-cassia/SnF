#include "outbound_reservation_test_support.hpp"
#include "snf/server/protocol_player_effect_sink.hpp"
#include "snf/server/ranking_projection.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    // The real channel, because a reservation can only come from one: an effect sink
    // consumes capacity it did not create, and a stub that mints capacity would test
    // the wrong contract.
    struct EffectSinkFixture
    {
        explicit EffectSinkFixture(const std::size_t capacity,
                                   const std::size_t event_capacity = 16)
            : wake(snf::test::make_wake_descriptor())
            , channel(snf::server::OutboundChannelConfig{.capacity = capacity,
                                                         .max_slots_per_connection = capacity},
                      wake.getDescriptor())
            , events(event_capacity)
            , effects(channel, &events)
        {
        }

        [[nodiscard]] snf::server::OutboundReservation
        reserve(const snf::net::ConnectionId connection, const std::size_t slots)
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
        snf::server::InMemoryRankingEventPipeline events;
        snf::server::ProtocolPlayerEffectSink effects;
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

    snf::server::PlayerResult score_result(const snf::server::PlayerId player,
                                           const std::uint64_t sequence,
                                           const std::uint64_t score)
    {
        return snf::server::PlayerResult{
            .effects =
                {
                    snf::server::PublishPlayerEvent{
                        .event =
                            snf::server::PlayerScoreChanged{
                                .player = player,
                                .sequence = sequence,
                                .score = score,
                            },
                    },
                },
        };
    }

    void test_prices_a_result_by_the_actions_it_emits()
    {
        EffectSinkFixture fixture{4};
        auto result = pong_result(1);
        assert(fixture.effects.requiredSlots(result) == 1);

        result.effects.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.request_id = 2, .payload = {}},
        });
        assert(fixture.effects.requiredSlots(result) == 2);
        assert(fixture.effects.requiredSlots(snf::server::PlayerResult{}) == 0);
    }

    void test_maps_send_response_to_a_frame_for_the_same_connection()
    {
        EffectSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto reservation = fixture.reserve(connection, 1);

        assert(fixture.effects.commit(connection, pong_result(42, {std::byte{0xAB}}), reservation));
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

    void test_preserves_effect_order_and_request_ids()
    {
        EffectSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto result = pong_result(1);
        result.effects.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.request_id = 2, .payload = {}},
        });

        auto reservation = fixture.reserve(connection, fixture.effects.requiredSlots(result));
        assert(fixture.effects.commit(connection, std::move(result), reservation));
        assert(fixture.popRequestId() == 1);
        assert(fixture.popRequestId() == 2);
    }

    void test_an_empty_result_consumes_no_capacity()
    {
        EffectSinkFixture fixture{1};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto reservation = fixture.reserve(connection, 0);

        assert(fixture.effects.commit(connection, snf::server::PlayerResult{}, reservation));
        assert(fixture.channel.size() == 0);
        assert(fixture.channel.reservedSlotCount() == 0);
    }

    void test_event_effect_uses_no_outbound_slot_and_advances_projection()
    {
        EffectSinkFixture fixture{1};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        const snf::server::PlayerId player{.value = 72};
        auto result = score_result(player, 1, 50);
        assert(fixture.effects.requiredSlots(result) == 0);
        auto reservation = fixture.reserve(connection, 0);

        assert(fixture.effects.commit(connection, std::move(result), reservation));
        assert(fixture.channel.size() == 0);
        assert(fixture.channel.reservedSlotCount() == 0);
        assert((fixture.events.standings() ==
                std::vector<snf::server::RankingEntry>{
                    {.player = player, .score = 50, .last_sequence = 1}}));
    }

    void test_event_projection_rejection_is_not_silently_accepted()
    {
        EffectSinkFixture fixture{1, 1};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        const snf::server::PlayerId player{.value = 73};
        auto reservation = fixture.reserve(connection, 0);
        assert(fixture.effects.commit(connection, score_result(player, 1, 10), reservation));

        auto second_reservation = fixture.reserve(connection, 0);
        assert(
            !fixture.effects.commit(connection, score_result(player, 2, 20), second_reservation));
        assert(fixture.events.stats().rejected == 1);
        assert(fixture.events.stats().event_count == 1);
    }

    void test_a_cancelled_channel_rejects_the_emission()
    {
        EffectSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto reservation = fixture.reserve(connection, 1);
        static_cast<void>(fixture.channel.cancel());

        assert(!fixture.effects.commit(connection, pong_result(1), reservation));
        assert(fixture.channel.size() == 0);
    }

    void test_an_underpriced_reservation_keeps_the_effects_it_already_emitted()
    {
        EffectSinkFixture fixture{4};
        const snf::net::ConnectionId connection{.descriptor = 4, .generation = 9};
        auto result = pong_result(1);
        result.effects.push_back(snf::server::SendResponse{
            .response = snf::server::PongResponse{.request_id = 2, .payload = {}},
        });

        // One slot for two effects: emission is not a transaction, so the first effect
        // stays emitted and the shortfall surfaces as a broken invariant rather than a
        // silently dropped response.
        auto reservation = fixture.reserve(connection, 1);
        bool shortfall_reported = false;
        try
        {
            static_cast<void>(fixture.effects.commit(connection, std::move(result), reservation));
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

void run_player_effect_sink_tests()
{
    test_prices_a_result_by_the_actions_it_emits();
    test_maps_send_response_to_a_frame_for_the_same_connection();
    test_preserves_effect_order_and_request_ids();
    test_an_empty_result_consumes_no_capacity();
    test_event_effect_uses_no_outbound_slot_and_advances_projection();
    test_event_projection_rejection_is_not_silently_accepted();
    test_a_cancelled_channel_rejects_the_emission();
    test_an_underpriced_reservation_keeps_the_effects_it_already_emitted();
}
