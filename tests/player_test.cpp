#include "snf/game/player.hpp"

#include "snf/game/street_progression.hpp"

#include <cassert>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <variant>

namespace
{
    snf::server::PlayerCommand make_ping()
    {
        return snf::server::PingCommand{
            .payload = {},
        };
    }

    void test_player_actor_owns_state_and_dispatches_ping()
    {
        snf::server::Player actor;
        assert(actor.state().handledCommandCount() == 0);

        const auto first_command = make_ping();
        const auto second_command = make_ping();
        const auto first = actor.handle(first_command);
        const auto second = actor.handle(second_command);

        assert(first.responses.size() == 1);
        assert(second.responses.size() == 1);
        const auto* first_send = &first.responses.front();
        const auto* second_send = &second.responses.front();
        assert(first_send != nullptr);
        assert(second_send != nullptr);
        const auto* first_pong = std::get_if<snf::server::PongResponse>(&first_send->response);
        const auto* second_pong = std::get_if<snf::server::PongResponse>(&second_send->response);
        assert(first_pong != nullptr);
        assert(second_pong != nullptr);
        assert(actor.state().handledCommandCount() == 2);
    }

    void test_persistent_player_actor_acknowledges_its_identity()
    {
        const snf::server::PlayerId player{.value = 77};
        snf::server::Player actor{player};
        const snf::server::PlayerCommand command = snf::server::AuthenticateCommand{
            .player = player,
        };

        const auto result = actor.handle(command);
        assert(actor.state().identity() == player);
        assert(result.responses.size() == 1);
        const auto* send = &result.responses.front();
        assert(send != nullptr);
        const auto* authenticated = std::get_if<snf::server::AuthenticatedResponse>(&send->response);
        assert(authenticated != nullptr);
        assert(authenticated->player == player);
    }

    void test_persistent_player_actor_restores_and_snapshots_state()
    {
        const snf::server::PlayerId player{.value = 88};
        snf::server::Player actor{player};
        actor.restore(snf::server::PlayerRecord{
            .player = player,
            .handled_command_count = 10,
            .last_location =
                snf::server::PlayerLocation{
                    .zone = snf::server::ZoneId{.value = 5},
                    .position = {.x = 3, .y = -4},
                },
            .currency_balance = 700,
            .purchased_item_count = 3,
        });

        const auto command = make_ping();
        static_cast<void>(actor.handle(command));
        const auto record = actor.snapshot();
        assert(record.player == player);
        assert(record.handled_command_count == 11);
        assert(record.currency_balance == 700);
        assert(record.purchased_item_count == 3);
        assert(
            (record.last_location ==
             snf::server::PlayerLocation{
                 .zone = snf::server::ZoneId{.value = 5},
                 .position = {.x = 3, .y = -4},
             })
        );

        actor.setLastLocation(snf::server::PlayerLocation{
            .zone = snf::server::ZoneId{.value = 6},
            .position = {.x = 8, .y = 9},
        });
        assert(
            (actor.snapshot().last_location ==
             snf::server::PlayerLocation{
                 .zone = snf::server::ZoneId{.value = 6},
                 .position = {.x = 8, .y = 9},
             })
        );
    }

    void test_street_experience_grant_marks_progression_dirty()
    {
        const snf::server::PlayerId player{.value = 91};
        snf::server::Player actor{player};
        assert(!actor.hasFlushableDirtyState());

        actor.grantStreetExperience(300);
        assert(actor.state().streetExperience() == 300);
        // Progression owns a persisted value, so a grant on its own is reason enough
        // to hand a snapshot to the persistence queue.
        assert(actor.hasFlushableDirtyState());

        snf::server::PlayerStateComponentMask cleared = 0;
        const auto record = actor.takeDirtySnapshot(&cleared);
        assert(record && record->street_experience == 300);
        assert((cleared & componentMask(snf::server::PlayerStateComponent::Progression)) != 0);
        assert(!actor.hasFlushableDirtyState());
    }

    void test_street_experience_grants_accumulate_and_saturate()
    {
        const snf::server::PlayerId player{.value = 92};
        snf::server::Player actor{player};

        actor.grantStreetExperience(300);
        actor.grantStreetExperience(400);
        // A grant adds to the total rather than replacing it.
        assert(actor.state().streetExperience() == 700);

        actor.grantStreetExperience(std::numeric_limits<std::uint64_t>::max());
        assert(actor.state().streetExperience() == std::numeric_limits<std::uint64_t>::max());
        // Already at the ceiling, so a further grant must not wrap back to nearly zero.
        actor.grantStreetExperience(1);
        assert(actor.state().streetExperience() == std::numeric_limits<std::uint64_t>::max());
    }

    void test_street_experience_survives_the_record_round_trip()
    {
        const snf::server::PlayerId player{.value = 93};
        snf::server::Player actor{player};
        actor.restore(snf::server::PlayerRecord{
            .player = player,
            .street_experience = 29500,
        });

        assert(actor.state().streetExperience() == 29500);
        // A restore is not a change, so it must not leave the actor asking to be saved.
        assert(!actor.hasFlushableDirtyState());
        assert(actor.snapshot().street_experience == 29500);
    }

    void test_street_experience_keeps_accumulating_past_the_level_cap()
    {
        const snf::server::PlayerId player{.value = 94};
        snf::server::Player actor{player};

        const std::uint64_t at_cap = snf::server::EXPERIENCE_PER_STREET_LEVEL * (snf::server::MAX_STREET_LEVEL - 1);
        actor.grantStreetExperience(at_cap + 5000);

        // The level is clamped, the stored experience is not: raising the cap later
        // has to grant the levels that were already earned, with no backfill.
        assert(snf::server::streetLevel(actor.state().streetExperience()) == snf::server::MAX_STREET_LEVEL);
        assert(actor.state().streetExperience() == at_cap + 5000);
        assert(actor.snapshot().street_experience == at_cap + 5000);
    }

    void test_live_purchase_is_memory_authoritative_and_bounded()
    {
        const snf::server::PlayerId player{.value = 89};
        snf::server::Player actor{player, 1};
        actor.restore(snf::server::PlayerRecord{
            .player = player,
            .last_location = std::nullopt,
            .currency_balance = 1000,
            .purchased_item_count = 0,
        });

        const snf::server::PurchaseCommand first{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 1},
            .product = snf::server::BASIC_PRODUCT,
        };
        const auto committed = actor.handle(first);
        const auto* committed_send = &committed.responses.front();
        assert(committed_send != nullptr);
        const auto* committed_response = std::get_if<snf::server::PurchaseResponse>(&committed_send->response);
        assert(committed_response != nullptr);
        assert(committed_response->result.status == snf::server::PurchaseStatus::Committed);
        assert(actor.state().currencyBalance() == 900);
        assert(actor.state().purchasedItemCount() == 1);
        assert((actor.dirtyComponents() & snf::server::componentMask(snf::server::PlayerStateComponent::Economy)) != 0);

        snf::server::PlayerStateComponentMask cleared = 0;
        const auto dirty_snapshot = actor.takeDirtySnapshot(&cleared);
        assert(dirty_snapshot.has_value());
        // The snapshot has to carry what the handler just decided, not the balance it
        // started the turn with.
        assert(dirty_snapshot->currency_balance == 900);
        assert(dirty_snapshot->purchased_item_count == 1);
        assert((cleared & snf::server::componentMask(snf::server::PlayerStateComponent::Economy)) != 0);
        assert(actor.dirtyComponents() == 0);

        const auto replay = actor.handle(first);
        const auto* replay_send = &replay.responses.front();
        assert(replay_send != nullptr);
        const auto* replay_response = std::get_if<snf::server::PurchaseResponse>(&replay_send->response);
        assert(replay_response != nullptr);
        assert(replay_response->result.replayed);
        assert(actor.state().currencyBalance() == 900);
        assert(actor.state().purchasedItemCount() == 1);

        const snf::server::PurchaseCommand capacity{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 2},
            .product = snf::server::BASIC_PRODUCT,
        };
        const auto rejected = actor.handle(capacity);
        const auto* rejected_send = &rejected.responses.front();
        assert(rejected_send != nullptr);
        const auto* rejected_response = std::get_if<snf::server::PurchaseResponse>(&rejected_send->response);
        assert(rejected_response != nullptr);
        assert(rejected_response->result.status == snf::server::PurchaseStatus::IdempotencyCapacityExceeded);
        assert(actor.state().currencyBalance() == 900);
    }

    void test_live_purchase_rejects_unknown_and_reports_insufficient_funds()
    {
        const snf::server::PlayerId player{.value = 890};
        snf::server::Player actor{player};
        actor.restore(snf::server::PlayerRecord{
            .player = player,
            .last_location = std::nullopt,
            .currency_balance = 50,
        });

        const snf::server::PurchaseCommand insufficient{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 3},
            .product = snf::server::BASIC_PRODUCT,
        };
        const auto first = actor.handle(insufficient);
        const auto* first_send = &first.responses.front();
        assert(first_send != nullptr);
        const auto* first_response = std::get_if<snf::server::PurchaseResponse>(&first_send->response);
        assert(first_response != nullptr);
        assert(first_response->result.status == snf::server::PurchaseStatus::InsufficientFunds);
        assert(!actor.hasFlushableDirtyState());

        const auto replay = actor.handle(insufficient);
        const auto* replay_send = &replay.responses.front();
        assert(replay_send != nullptr);
        const auto* replay_response = std::get_if<snf::server::PurchaseResponse>(&replay_send->response);
        assert(replay_response != nullptr);
        assert(replay_response->result.replayed);

        const snf::server::PurchaseCommand unknown{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 4},
            .product = snf::server::ProductId{.value = 999},
        };
        const auto missing = actor.handle(unknown);
        const auto* missing_send = &missing.responses.front();
        assert(missing_send != nullptr);
        const auto* missing_response = std::get_if<snf::server::PurchaseResponse>(&missing_send->response);
        assert(missing_response != nullptr);
        assert(missing_response->result.status == snf::server::PurchaseStatus::ProductNotFound);
    }
}

void test_a_room_join_carries_stats_derived_from_experience()
{
    const snf::server::PlayerId player{.value = 95};
    snf::server::Player actor{player};
    actor.grantStreetExperience(1000);

    const snf::server::PlayerCommand command = snf::server::JoinRoomRequest{
        .room = snf::server::RoomId{.value = 7},
    };
    const auto result = actor.handle(command);

    // Nothing answers the client here: the Room decides whether the join lands.
    assert(result.responses.empty());
    assert(result.room_join);
    assert(result.room_join->room == snf::server::RoomId{.value = 7});
    // 1000 experience is level 2, which is one step of growth off the base.
    assert((result.room_join->stats == snf::server::CombatStats{.attack = 11, .health = 110}));
}

void test_a_provisional_player_cannot_join_a_room()
{
    snf::server::Player actor;
    const snf::server::PlayerCommand command = snf::server::JoinRoomRequest{
        .room = snf::server::RoomId{.value = 7},
    };

    bool refused = false;
    try
    {
        static_cast<void>(actor.handle(command));
    }
    catch (const std::logic_error&)
    {
        refused = true;
    }
    // A Room reward is persistent state, so an unauthenticated actor has no
    // business entering one.
    assert(refused);
}
void run_player_tests()
{
    test_a_room_join_carries_stats_derived_from_experience();
    test_a_provisional_player_cannot_join_a_room();
    test_player_actor_owns_state_and_dispatches_ping();
    test_persistent_player_actor_acknowledges_its_identity();
    test_persistent_player_actor_restores_and_snapshots_state();
    test_street_experience_grant_marks_progression_dirty();
    test_street_experience_grants_accumulate_and_saturate();
    test_street_experience_survives_the_record_round_trip();
    test_street_experience_keeps_accumulating_past_the_level_cap();
    test_live_purchase_is_memory_authoritative_and_bounded();
    test_live_purchase_rejects_unknown_and_reports_insufficient_funds();
}
