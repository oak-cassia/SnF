#pragma once

#include "snf/adapter/game_payloads.hpp"
#include "snf/game/player_result.hpp"
#include "snf/game/room_result.hpp"
#include "snf/game/zone_result.hpp"
#include "snf/worker/actor.hpp"
#include "snf/worker/timer_queue.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace snf::adapter
{
    constexpr std::size_t MAX_ROOM_PARTICIPANTS = 4;
    constexpr std::size_t MAX_ROOM_EFFECTS = 16;
    static_assert(MAX_ROOM_EFFECTS <= snf::worker::EffectBatch::MAX_EFFECTS);

    struct PlayerTurnContext
    {
        std::optional<snf::worker::ConnectionRef> connection{std::nullopt};
        std::uint32_t request_id{0};
        std::optional<snf::server::PlayerId> player_id{std::nullopt};
        std::chrono::steady_clock::time_point now{};
    };

    struct ZoneTurnContext
    {
        std::optional<snf::worker::ConnectionRef> connection{std::nullopt};
        std::uint32_t request_id{0};
        std::chrono::steady_clock::time_point now{};
        bool zone_empty{false};
    };

    struct PreparedRoomTimer
    {
        std::chrono::steady_clock::time_point deadline{};
        snf::worker::ActorEnvelope message{};
        snf::worker::TimerReservation reservation{};
    };

    struct RoomAudienceRoute
    {
        snf::server::PlayerId player;
        std::optional<snf::worker::ConnectionRef> connection{std::nullopt};
    };

    struct RoomTurnContext
    {
        std::optional<snf::worker::ConnectionRef> connection{std::nullopt};
        std::uint32_t request_id{0};
        std::chrono::steady_clock::time_point now{};
        std::vector<RoomAudienceRoute> audience_routes{};
    };

    [[nodiscard]] snf::worker::EffectBatch toEffects(
        const PlayerTurnContext& context,
        const snf::server::PlayerResult& result
    );

    [[nodiscard]] snf::worker::EffectBatch toEffects(
        const ZoneTurnContext& context,
        const snf::server::ZoneResult& result
    );

    [[nodiscard]] snf::worker::EffectBatch toEffects(
        const RoomTurnContext& context,
        const snf::server::RoomResult& result,
        std::optional<PreparedRoomTimer> prepared_timer = std::nullopt
    );
}
