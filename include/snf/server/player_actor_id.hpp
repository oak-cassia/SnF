#pragma once

#include "snf/runtime/actor_key.hpp"
#include "snf/server/player_id.hpp"
#include "snf/server/provisional_actor_id.hpp"

#include <cstdint>
#include <optional>

namespace snf::server
{
    // A typed route target for either the pre-authentication actor namespace or
    // the persistent Player namespace. Construction from the two strong ids is
    // intentionally implicit so aggregate route initialization stays concise.
    class PlayerActorId
    {
    public:
        constexpr PlayerActorId() noexcept = default;

        constexpr PlayerActorId(const ProvisionalActorId actor) noexcept
            : value(actor.value)
            , _kind(snf::runtime::ActorKind::ProvisionalPlayer)
        {
        }

        constexpr PlayerActorId(const PlayerId player) noexcept
            : value(player.value)
            , _kind(snf::runtime::ActorKind::Player)
        {
        }

        [[nodiscard]] constexpr snf::runtime::ActorKind kind() const noexcept
        {
            return _kind;
        }

        [[nodiscard]] constexpr std::optional<PlayerId> playerId() const noexcept
        {
            if (_kind != snf::runtime::ActorKind::Player)
            {
                return std::nullopt;
            }

            return PlayerId{.value = value};
        }

        [[nodiscard]] constexpr bool operator==(const PlayerActorId&) const noexcept = default;

        [[nodiscard]] constexpr bool operator==(const ProvisionalActorId actor) const noexcept
        {
            return _kind == snf::runtime::ActorKind::ProvisionalPlayer && value == actor.value;
        }

        [[nodiscard]] constexpr bool operator==(const PlayerId player) const noexcept
        {
            return _kind == snf::runtime::ActorKind::Player && value == player.value;
        }

        std::uint64_t value{0};

    private:
        snf::runtime::ActorKind _kind{snf::runtime::ActorKind::ProvisionalPlayer};
    };
}
