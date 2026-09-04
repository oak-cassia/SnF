#pragma once

#include "snf/adapter/game_payloads.hpp"
#include "snf/game/player.hpp"
#include "snf/game/player_record.hpp"
#include "snf/game/room_id.hpp"
#include "snf/game/zone_id.hpp"
#include "snf/worker/actor.hpp"
#include "snf/worker/timer_queue.hpp"

#include <chrono>
#include <variant>
#include <vector>

namespace snf::adapter
{
    struct StableRoute
    {
        std::optional<snf::server::ZoneId> zone{std::nullopt};
    };

    struct EnteringRoute
    {
        snf::server::RoomId target_room{0};
        snf::server::ZoneId source_zone{0};
        std::uint64_t source_epoch{0};
        snf::server::ZonePosition return_position{0, 0};
        std::uint32_t request_id{0};
        std::uint64_t correlation_id{0};
        WorkflowStep step{WorkflowStep::RoomJoinStep1_JoinRoom};
    };

    struct InRoomRoute
    {
        snf::server::RoomId room{0};
        snf::server::ZoneId return_zone{0};
        snf::server::ZonePosition return_position{0, 0};
    };

    struct ReturningRoute
    {
        snf::server::RoomId source_room{0};
        snf::server::ZoneId return_zone{0};
        snf::server::ZonePosition return_position{0, 0};
        std::uint64_t return_epoch{0};
        std::uint32_t request_id{0};
        std::uint64_t correlation_id{0};
        WorkflowStep step{WorkflowStep::RoomReturnStep1_ZoneEnter};
    };

    using WorkflowState = std::variant<StableRoute, EnteringRoute, InRoomRoute, ReturningRoute>;

    struct PendingZoneOperation
    {
        std::uint64_t correlation_id{0};
        WorkflowStep step{WorkflowStep::None};
        snf::server::ZoneId target_zone{0};
        std::uint64_t target_epoch{0};
        std::uint32_t request_id{0};
    };

    class PlayerActorAdapter final : public snf::worker::ActorInstance
    {
    public:
        explicit PlayerActorAdapter(
            std::optional<snf::server::PlayerId> player_id = std::nullopt,
            snf::worker::TimerAdmission* timer_admission = nullptr
        )
            : _player(player_id)
            , _timer_admission(timer_admission)
        {
        }

        // Built from persisted state after an activation load.
        PlayerActorAdapter(
            const snf::server::PlayerId player_id,
            const snf::server::PlayerRecord& record,
            snf::worker::TimerAdmission* timer_admission = nullptr
        )
            : _player(player_id)
            , _timer_admission(timer_admission)
        {
            _player.restore(record);
        }

        [[nodiscard]] snf::server::Player& player() noexcept
        {
            return _player;
        }

        [[nodiscard]] const snf::server::Player& player() const noexcept
        {
            return _player;
        }

        void setSaveInterval(const std::chrono::milliseconds interval) noexcept
        {
            _save_interval = interval;
        }

        [[nodiscard]] std::uint64_t committedSaves() const noexcept
        {
            return _committed_saves;
        }

        [[nodiscard]] std::uint64_t unknownCommits() const noexcept
        {
            return _unknown_commits;
        }

        [[nodiscard]] snf::worker::TurnResult dispatch(snf::worker::ActorEnvelope&& envelope, const snf::worker::ActorTurnContext& context) override;

        // Called from the save continuation once the database has answered. Public
        // because the continuation is a free coroutine, not a member.
        void onSaveCompleted(const snf::worker::DbResult& result, snf::server::PlayerStateComponentMask cleared);

        // The connection this player is authenticated on, if any.
        [[nodiscard]] std::optional<snf::worker::ConnectionRef> boundConnection() const noexcept
        {
            return _bound_connection;
        }

        [[nodiscard]] std::uint64_t authenticationConflicts() const noexcept
        {
            return _authentication_conflicts;
        }

        [[nodiscard]] std::optional<snf::server::ZoneId> currentZone() const noexcept
        {
            if (std::holds_alternative<StableRoute>(_workflow_state))
            {
                return std::get<StableRoute>(_workflow_state).zone;
            }
            return std::nullopt;
        }

        [[nodiscard]] std::uint64_t routeEpoch() const noexcept
        {
            return _route_epoch;
        }

        [[nodiscard]] const WorkflowState& workflowState() const noexcept
        {
            return _workflow_state;
        }

        [[nodiscard]] bool isInRoom() const noexcept
        {
            return std::holds_alternative<InRoomRoute>(_workflow_state);
        }

        [[nodiscard]] std::optional<snf::server::RoomId> currentRoom() const noexcept
        {
            if (std::holds_alternative<InRoomRoute>(_workflow_state))
            {
                return std::get<InRoomRoute>(_workflow_state).room;
            }
            return std::nullopt;
        }

    private:
        void scheduleSaveIfDirty(snf::worker::EffectBatch& effects, std::chrono::steady_clock::time_point now);
        [[nodiscard]] std::optional<snf::worker::EffectBatch> rejectConflictingAuthentication(
            const std::optional<snf::worker::ConnectionRef>& connection
        );
        [[nodiscard]] snf::worker::TurnResult handleZoneRequest(PlayerZoneRequestMessage&& msg, const snf::worker::ActorTurnContext& context);
        [[nodiscard]] snf::worker::TurnResult handleRoomRequest(PlayerRoomRequestMessage&& msg, const snf::worker::ActorTurnContext& context);
        [[nodiscard]] snf::worker::TurnResult handleZoneOutcome(ZoneOutcomeMessage&& msg, const snf::worker::ActorTurnContext& context);
        [[nodiscard]] snf::worker::TurnResult handleRoomOutcome(RoomOutcomeMessage&& msg, const snf::worker::ActorTurnContext& context);
        [[nodiscard]] snf::worker::TurnResult handleWorkflowTimeout(PlayerWorkflowTimeoutMessage&& msg, const snf::worker::ActorTurnContext& context);

        snf::server::Player _player;
        snf::worker::TimerAdmission* _timer_admission{nullptr};
        // player -> connection. The sink owns connection -> player.
        std::optional<snf::worker::ConnectionRef> _bound_connection{std::nullopt};
        WorkflowState _workflow_state{StableRoute{}};
        std::uint64_t _route_epoch{0};
        std::uint64_t _correlation_sequence{0};
        static constexpr std::size_t MAX_PENDING_ZONE_OPS = 16;
        std::vector<PendingZoneOperation> _pending_zone_ops{};
        std::uint64_t _authentication_conflicts{0};
        std::chrono::milliseconds _save_interval{std::chrono::seconds{5}};
        // Only one save timer may be outstanding. The actor is Suspended for the
        // duration of the await, so a second save cannot overlap the first.
        bool _save_scheduled{false};
        std::uint64_t _committed_saves{0};
        std::uint64_t _unknown_commits{0};
    };
}
