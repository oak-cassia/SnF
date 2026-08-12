#include "snf/server/player_actor.hpp"

#include <limits>
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

    std::uint64_t PlayerState::rankingScore() const noexcept
    {
        return _ranking_score;
    }

    std::uint64_t PlayerState::lastDomainEventSequence() const noexcept
    {
        return _last_domain_event_sequence;
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
        _state._ranking_score = record.ranking_score;
        _state._last_domain_event_sequence = record.last_domain_event_sequence;
    }

    void PlayerActor::setLastLocation(std::optional<PlayerLocation> location) noexcept
    {
        _state._last_location = location;
    }

    PlayerResult PlayerActor::completePurchase(const PurchaseCommand& command,
                                               PurchaseTransactionResult result)
    {
        const auto player = _state._identity.playerId();
        if (!player || result.player != *player ||
            result.idempotency_key != command.idempotency_key || result.product != command.product)
        {
            throw std::logic_error{"Purchase completion does not match the Player command"};
        }

        if (result.status == PurchaseStatus::Unavailable)
        {
            // Queue admission failure has no authoritative storage snapshot. Keep
            // the Actor state and report its current values instead of applying zeros.
            result.currency_balance = _state._currency_balance;
            result.purchased_item_count = _state._purchased_item_count;
        }
        else
        {
            _state._currency_balance = result.currency_balance;
            _state._purchased_item_count = result.purchased_item_count;
        }
        ++_state._handled_command_count;

        return PlayerResult{
            .effects =
                {
                    SendResponse{
                        .response =
                            PurchaseResponse{
                                .request_id = command.request_id,
                                .result = std::move(result),
                            },
                    },
                },
        };
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
            .ranking_score = _state._ranking_score,
            .last_domain_event_sequence = _state._last_domain_event_sequence,
        };
    }

    PlayerResult PlayerActor::completeRankingAward(const AwardRankingScoreCommand& command,
                                                   RankingAwardTransactionResult result)
    {
        const auto player = _state._identity.playerId();
        if (!player || result.player != *player || result.award_id != command.award_id ||
            result.score_delta != command.score_delta)
        {
            throw std::logic_error{"Ranking award completion does not match the Player command"};
        }
        if (result.status != RankingAwardStatus::Committed)
        {
            throw std::runtime_error{"Ranking award transaction did not commit"};
        }
        if (result.event_sequence == 0 || result.global_offset == 0 ||
            result.authoritative_sequence < result.event_sequence ||
            result.authoritative_score < result.event_score ||
            result.authoritative_sequence < _state._last_domain_event_sequence ||
            result.authoritative_score < _state._ranking_score)
        {
            throw std::logic_error{"Ranking award completion would regress Player state"};
        }
        if ((!result.replayed && (result.authoritative_sequence != result.event_sequence ||
                                  result.authoritative_score != result.event_score)) ||
            (result.authoritative_sequence == result.event_sequence &&
             result.authoritative_score != result.event_score))
        {
            throw std::logic_error{"Ranking award completion has inconsistent event state"};
        }
        if (result.authoritative_sequence == _state._last_domain_event_sequence &&
            result.authoritative_score != _state._ranking_score)
        {
            throw std::logic_error{"Ranking award reused a Player sequence with a new score"};
        }
        if (result.authoritative_sequence > _state._last_domain_event_sequence &&
            result.authoritative_score <= _state._ranking_score)
        {
            throw std::logic_error{"Ranking award advanced sequence without advancing score"};
        }

        _state._ranking_score = result.authoritative_score;
        _state._last_domain_event_sequence = result.authoritative_sequence;
        ++_state._handled_command_count;
        return PlayerResult{};
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

    PlayerResult PlayerActor::handleCommand(const PurchaseCommand&)
    {
        throw std::logic_error{"PurchaseCommand must be completed by PlayerActorBinding"};
    }

    PlayerResult PlayerActor::handleCommand(const AwardRankingScoreCommand& command)
    {
        static_cast<void>(command);
        throw std::logic_error{"AwardRankingScoreCommand must be completed by PlayerActorBinding"};
    }
}
