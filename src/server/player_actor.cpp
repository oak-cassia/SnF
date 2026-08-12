#include "snf/server/player_actor.hpp"

#include <stdexcept>
#include <variant>

namespace snf::server
{
    PlayerActorId PlayerState::identity() const noexcept
    {
        return _identity;
    }

    std::uint64_t PlayerState::handledCommandCount() const noexcept
    {
        return _handled_command_count;
    }

    std::optional<PlayerLocation> PlayerState::lastLocation() const noexcept
    {
        return _last_location;
    }

    std::uint64_t PlayerState::currencyBalance() const noexcept
    {
        return _currency_balance;
    }

    std::uint64_t PlayerState::purchasedItemCount() const noexcept
    {
        return _purchased_item_count;
    }

    const PlayerState& PlayerActor::state() const noexcept
    {
        return _state;
    }

    void PlayerActor::restore(const PlayerRecord& record)
    {
        if (_state._identity != record.player)
        {
            throw std::invalid_argument{"Player record identity does not match the Actor"};
        }

        _state._handled_command_count = record.handled_command_count;
        _state._last_location = record.last_location;
        _state._currency_balance = record.currency_balance;
        _state._purchased_item_count = record.purchased_item_count;
    }

    void PlayerActor::setLastLocation(std::optional<PlayerLocation> location) noexcept
    {
        _state._last_location = location;
    }

    PlayerRecord PlayerActor::snapshot() const
    {
        const auto player = _state._identity.playerId();
        if (!player)
        {
            throw std::logic_error{"A provisional Player actor has no persistent snapshot"};
        }

        return PlayerRecord{
            .player = *player,
            .handled_command_count = _state._handled_command_count,
            .last_location = _state._last_location,
            .currency_balance = _state._currency_balance,
            .purchased_item_count = _state._purchased_item_count,
        };
    }

    PlayerActor::PlayerActor(const PlayerActorId identity) noexcept
    {
        _state._identity = identity;
    }

    snf::runtime::ActorTask<PlayerResult> PlayerActor::handle(const PlayerCommand& command)
    {
        PlayerResult result =
            std::visit([this](const auto& value) { return handleCommand(value); }, command);
        ++_state._handled_command_count;
        co_return result;
    }

    PlayerResult PlayerActor::handleCommand(const PingCommand& command)
    {
        return PlayerResult{
            .effects =
                {
                    SendResponse{
                        .response =
                            PongResponse{
                                .request_id = command.request_id,
                                .payload = command.payload,
                            },
                    },
                },
        };
    }

    PlayerResult PlayerActor::handleCommand(const AuthenticateCommand& command)
    {
        if (_state._identity != command.player)
        {
            throw std::logic_error{"AuthenticateCommand reached a different Player actor"};
        }

        return PlayerResult{
            .effects =
                {
                    SendResponse{
                        .response =
                            AuthenticatedResponse{
                                .request_id = command.request_id,
                                .player = command.player,
                            },
                    },
                },
        };
    }
}
