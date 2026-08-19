#include "snf/game/player.hpp"

#include "snf/game/product_catalog.hpp"

#include <limits>
#include <stdexcept>
#include <utility>
#include <variant>

namespace snf::server
{
    std::optional<PlayerId> PlayerState::identity() const noexcept
    {
        return _session.identity;
    }

    std::uint64_t PlayerState::handledCommandCount() const noexcept
    {
        return _session.handled_command_count;
    }

    std::optional<PlayerLocation> PlayerState::lastLocation() const noexcept
    {
        return _session.last_location;
    }

    std::uint64_t PlayerState::currencyBalance() const noexcept
    {
        return _economy.currency_balance;
    }

    std::uint64_t PlayerState::purchasedItemCount() const noexcept
    {
        return _economy.purchased_item_count;
    }

    std::uint64_t PlayerState::streetExperience() const noexcept
    {
        return _progression.street_experience;
    }

    PlayerStateComponentMask PlayerState::dirtyComponents() const noexcept
    {
        return _dirty_components;
    }

    const PlayerState& Player::state() const noexcept
    {
        return _state;
    }

    void Player::restore(const PlayerRecord& record)
    {
        if (_state._session.identity != std::optional{record.player})
        {
            throw std::invalid_argument{"Player record identity does not match the Actor"};
        }

        _state._session.handled_command_count = record.handled_command_count;
        _state._session.last_location = record.last_location;
        _state._economy.currency_balance = record.currency_balance;
        _state._economy.purchased_item_count = record.purchased_item_count;
        _state._progression.street_experience = record.street_experience;
        _state._dirty_components = 0;
        _purchase_evidence.clear();
    }

    void Player::setLastLocation(std::optional<PlayerLocation> location) noexcept
    {
        _state._session.last_location = std::move(location);
        _state._dirty_components |= componentMask(PlayerStateComponent::Session);
    }

    void Player::grantStreetExperience(const std::uint64_t experience) noexcept
    {
        std::uint64_t& total = _state._progression.street_experience;
        total = experience > std::numeric_limits<std::uint64_t>::max() - total ? std::numeric_limits<std::uint64_t>::max() : total + experience;
        _state._dirty_components |= componentMask(PlayerStateComponent::Progression);
    }

    bool Player::hasFlushableDirtyState() const noexcept
    {
        constexpr PlayerStateComponentMask flushable = componentMask(PlayerStateComponent::Economy) | componentMask(PlayerStateComponent::Progression);
        return (_state._dirty_components & flushable) != 0;
    }

    PlayerStateComponentMask Player::dirtyComponents() const noexcept
    {
        return _state._dirty_components;
    }

    std::optional<PlayerRecord> Player::takeDirtySnapshot(PlayerStateComponentMask* const cleared_components)
    {
        if (!hasFlushableDirtyState())
        {
            if (cleared_components != nullptr)
            {
                *cleared_components = 0;
            }
            return std::nullopt;
        }

        const PlayerStateComponentMask components = _state._dirty_components;
        PlayerRecord record = snapshot();
        _state._dirty_components = 0;
        if (cleared_components != nullptr)
        {
            *cleared_components = components;
        }
        return record;
    }

    void Player::restoreDirtyComponents(const PlayerStateComponentMask components) noexcept
    {
        _state._dirty_components |= components;
    }

    PlayerRecord Player::snapshot() const
    {
        const auto player = _state._session.identity;
        if (!player)
        {
            throw std::logic_error{"A provisional Player actor has no persistent snapshot"};
        }

        return PlayerRecord{
            .player = *player,
            .handled_command_count = _state._session.handled_command_count,
            .last_location = _state._session.last_location,
            .currency_balance = _state._economy.currency_balance,
            .purchased_item_count = _state._economy.purchased_item_count,
            .street_experience = _state._progression.street_experience,
        };
    }

    Player::Player(std::optional<PlayerId> identity) noexcept
    {
        _state._session.identity = identity;
    }

    Player::Player(std::optional<PlayerId> identity, const std::size_t max_purchase_idempotency_records)
        : _max_purchase_idempotency_records(max_purchase_idempotency_records)
    {
        if (_max_purchase_idempotency_records == 0)
        {
            throw std::invalid_argument{"Purchase idempotency capacity must be positive"};
        }
        _state._session.identity = identity;
    }

    PlayerResult Player::handle(const PlayerCommand& command)
    {
        PlayerResult result = std::visit([this](const auto& value) { return handleCommand(value); }, command);
        ++_state._session.handled_command_count;
        return result;
    }

    PlayerResult Player::handleCommand(const PingCommand& command)
    {
        return PlayerResult{
            .responses =
                {
                    SendResponse{
                        .response =
                            PongResponse{
                                .payload = command.payload,
                            },
                    },
                },
        };
    }

    PlayerResult Player::handleCommand(const AuthenticateCommand& command)
    {
        if (_state._session.identity != std::optional{command.player})
        {
            throw std::logic_error{"AuthenticateCommand reached a different Player actor"};
        }

        return PlayerResult{
            .responses =
                {
                    SendResponse{
                        .response =
                            AuthenticatedResponse{
                                .player = command.player,
                            },
                    },
                },
        };
    }

    PlayerResult Player::handleCommand(const PurchaseCommand& command)
    {
        const auto player = _state._session.identity;
        if (!player)
        {
            throw std::logic_error{"PurchaseCommand reached a provisional Player actor"};
        }

        PurchaseTransactionResult result{
            .status = PurchaseStatus::ProductNotFound,
            .player = *player,
            .idempotency_key = command.idempotency_key,
            .product = command.product,
            .currency_balance = _state._economy.currency_balance,
            .purchased_item_count = _state._economy.purchased_item_count,
            .replayed = false,
        };

        if (const auto existing = _purchase_evidence.find(command.idempotency_key.value); existing != _purchase_evidence.end())
        {
            if (existing->second.product != command.product)
            {
                result.status = PurchaseStatus::IdempotencyConflict;
            }
            else
            {
                result = existing->second.result;
                result.currency_balance = _state._economy.currency_balance;
                result.purchased_item_count = _state._economy.purchased_item_count;
                result.replayed = true;
            }
        }
        else if (const auto definition = findProduct(command.product); !definition)
        {
            // Unknown products are rejected before any repository operation.
        }
        else if (_purchase_evidence.size() >= _max_purchase_idempotency_records)
        {
            result.status = PurchaseStatus::IdempotencyCapacityExceeded;
        }
        else
        {
            result.status = PurchaseStatus::Committed;
            std::uint64_t next_balance = _state._economy.currency_balance;
            std::uint64_t next_item_count = _state._economy.purchased_item_count;
            if (next_balance < definition->price)
            {
                result.status = PurchaseStatus::InsufficientFunds;
            }
            else if (next_item_count > std::numeric_limits<std::uint64_t>::max() - definition->grant_count)
            {
                result.status = PurchaseStatus::InventoryCapacityExceeded;
            }
            else
            {
                next_balance -= definition->price;
                next_item_count += definition->grant_count;
            }

            result.currency_balance = next_balance;
            result.purchased_item_count = next_item_count;
            _purchase_evidence.emplace(command.idempotency_key.value,
                                       PurchaseEvidence{
                                           .product = command.product,
                                           .result = result,
                                       });
            if (result.status == PurchaseStatus::Committed)
            {
                _state._economy.currency_balance = next_balance;
                _state._economy.purchased_item_count = next_item_count;
                _state._dirty_components |= componentMask(PlayerStateComponent::Economy);
            }
        }

        return PlayerResult{
            .responses =
                {
                    SendResponse{
                        .response =
                            PurchaseResponse{
                                .result = std::move(result),
                            },
                    },
                },
        };
    }
}
