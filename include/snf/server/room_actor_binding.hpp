#pragma once

#include "snf/runtime/actor_runtime.hpp"
#include "snf/runtime/distribution.hpp"
#include "snf/server/command_terminal.hpp"
#include "snf/server/room.hpp"
#include "snf/server/room_inbound_command.hpp"

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
        [[nodiscard]] std::unique_ptr<snf::runtime::ActorSlot> activate(snf::runtime::EntityId entity) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult
        dispatch(snf::runtime::ActorSlot& slot, const snf::runtime::ActorSubmission& submission, snf::runtime::ActorContext& context, std::stop_token stop_token) override;
        [[nodiscard]] snf::runtime::ActorDispatchResult resume(snf::runtime::ActorSlot& slot, snf::runtime::ActorContext& context, std::stop_token stop_token) override;

    private:
        struct RoomActorSlot;
        struct CommandPayload;
        struct PassivatePayload;

        RoomConfig _actor_config;
        std::function<void(const RoomInboundCommand&, const RoomResult&)> _on_result;
        CommandLifecycleSink* _lifecycle{nullptr};
        snf::runtime::Distribution _command_execution_nanoseconds;
    };
}
