#pragma once

#include "snf/game/room.hpp"
#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/distribution.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/room_inbound_command.hpp"
#include "snf/server/room_join_tell.hpp"

#include <chrono>
#include <functional>
#include <memory>

namespace snf::server
{
    struct RoomActorBindingConfig
    {
        RoomConfig actor;
        // Result delivery is intentionally a binding concern. Production routing
        // can replace this callback with a typed sink without putting connection
        // or protocol state inside Room.
        std::function<void(const RoomInboundCommand&, const RoomResult&)> on_result;
    };

    struct RoomActorBindingStats
    {
        snf::runtime::DistributionSnapshot command_execution_nanoseconds;
    };

    class RoomActorBinding final : public snf::runtime::ActorBinding
    {
    public:
        explicit RoomActorBinding(RoomActorBindingConfig config = {});
        RoomActorBinding(RoomActorBindingConfig config, CommandLifecycleSink& lifecycle);

        [[nodiscard]] snf::runtime::ActorKind kind() const noexcept override;
        [[nodiscard]] snf::runtime::ActorSubmission makeCommand(RoomInboundCommand command) const;
        [[nodiscard]] snf::runtime::ActorSubmission makePassivate(RoomId room) const;
        [[nodiscard]] RoomActorBindingStats stats() const noexcept;

    protected:
        [[nodiscard]] std::unique_ptr<snf::runtime::ActorState> activate(snf::runtime::EntityId entity) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult
        dispatch(snf::runtime::ActorState& state, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, std::stop_token stop_token) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult resume(snf::runtime::ActorState& state, snf::runtime::ActorContext& context, std::stop_token stop_token) override;
        // Called on the sending Player's Worker, so this stays a read-only
        // transform: no cache, no counter, nothing another Worker could race with.
        [[nodiscard]] std::optional<snf::runtime::ActorSubmission> makeTell(snf::runtime::ActorKey target, snf::runtime::TellPayload payload) override;

    private:
        struct RoomActorState;
        struct CommandPayload;
        struct PassivatePayload;

        RoomConfig _actor_config;
        std::function<void(const RoomInboundCommand&, const RoomResult&)> _on_result;
        CommandLifecycleSink* _lifecycle{nullptr};
        snf::runtime::Distribution _command_execution_nanoseconds;
    };
}
