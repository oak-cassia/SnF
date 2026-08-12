#pragma once

#include "snf/runtime/actor_task.hpp"
#include "snf/server/player_actor_id.hpp"
#include "snf/server/player_command.hpp"
#include "snf/server/player_record.hpp"
#include "snf/server/player_result.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    // The state is intentionally only mutable by PlayerActor. More player data is
    // added here as persistent-player work is introduced in a later milestone.
    class PlayerState
    {
    public:
        [[nodiscard]] PlayerActorId identity() const noexcept;
        [[nodiscard]] std::uint64_t handledCommandCount() const noexcept;
        [[nodiscard]] std::optional<PlayerLocation> lastLocation() const noexcept;
        [[nodiscard]] std::uint64_t currencyBalance() const noexcept;
        [[nodiscard]] std::uint64_t purchasedItemCount() const noexcept;

    private:
        friend class PlayerActor;

        PlayerActorId _identity;
        std::uint64_t _handled_command_count{0};
        std::optional<PlayerLocation> _last_location;
        std::uint64_t _currency_balance{INITIAL_CURRENCY_BALANCE};
        std::uint64_t _purchased_item_count{0};
    };

    class PlayerActor
    {
    public:
        PlayerActor() = default;
        explicit PlayerActor(PlayerActorId identity) noexcept;

        PlayerActor(const PlayerActor&) = delete;
        PlayerActor& operator=(const PlayerActor&) = delete;
        PlayerActor(PlayerActor&&) noexcept = default;
        PlayerActor& operator=(PlayerActor&&) noexcept = default;

        // Only a const view escapes the actor, but it is safe to read only on the
        // owning Worker. Cross-thread queries must use an immutable snapshot or a
        // command; const does not provide synchronization.
        [[nodiscard]] const PlayerState& state() const noexcept;
        void restore(const PlayerRecord& record);
        void setLastLocation(std::optional<PlayerLocation> location) noexcept;
        [[nodiscard]] PlayerRecord snapshot() const;

        // The caller must keep the command alive until the returned task
        // completes, not merely until this call returns: the task is lazy, so the
        // body has not run yet, and it may later suspend. Passing a temporary
        // therefore dangles. In the server the runtime owns the submission for
        // exactly that long, which is why this takes a reference instead of
        // copying the payload on every command.
        //
        // PING has nothing to await, so this task always completes on its first
        // resume. The first handler that actually suspends arrives with the
        // outbound reservation awaiter.
        [[nodiscard]] snf::runtime::ActorTask<PlayerResult> handle(const PlayerCommand& command);

    private:
        [[nodiscard]] PlayerResult handleCommand(const PingCommand& command);
        [[nodiscard]] PlayerResult handleCommand(const AuthenticateCommand& command);

        PlayerState _state;
    };
}
