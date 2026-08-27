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
        assert(actor.state().streetExperience() == 700);

        actor.grantStreetExperience(std::numeric_limits<std::uint64_t>::max());
        assert(actor.state().streetExperience() == std::numeric_limits<std::uint64_t>::max());
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
        assert(!actor.hasFlushableDirtyState());
        assert(actor.snapshot().street_experience == 29500);
    }

    void test_street_experience_keeps_accumulating_past_the_level_cap()
    {
        const snf::server::PlayerId player{.value = 94};
        snf::server::Player actor{player};

        const std::uint64_t at_cap = snf::server::EXPERIENCE_PER_STREET_LEVEL * (snf::server::MAX_STREET_LEVEL - 1);
        actor.grantStreetExperience(at_cap + 5000);

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

    void test_skill_purchase_and_equip_are_atomic_player_state_changes()
    {
        const snf::server::PlayerId player{.value = 891};
        snf::server::Player actor{player};

        assert(actor.state().getSkillLoadout().getEquippedSkillId() == snf::server::SLASH_SKILL_ID);
        assert(!actor.state().getSkillLoadout().hasOwnedSkillId(snf::server::ARCANE_BOLT_SKILL_ID));

        const auto purchased = actor.handle(snf::server::PurchaseCommand{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 10},
            .product = snf::server::ARCANE_BOLT_PRODUCT,
        });
        const auto* purchase_response = std::get_if<snf::server::PurchaseResponse>(&purchased.responses.front().response);
        assert(purchase_response != nullptr);
        assert(purchase_response->result.status == snf::server::PurchaseStatus::Committed);
        assert(purchase_response->result.currency_balance == 500);
        assert(purchase_response->result.purchased_item_count == 0);
        assert(actor.state().getSkillLoadout().hasOwnedSkillId(snf::server::ARCANE_BOLT_SKILL_ID));
        assert(actor.state().getSkillLoadout().getEquippedSkillId() == snf::server::SLASH_SKILL_ID);
        assert((actor.dirtyComponents() & snf::server::componentMask(snf::server::PlayerStateComponent::Economy)) != 0);
        assert((actor.dirtyComponents() & snf::server::componentMask(snf::server::PlayerStateComponent::Skills)) != 0);

        const auto replay = actor.handle(snf::server::PurchaseCommand{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 10},
            .product = snf::server::ARCANE_BOLT_PRODUCT,
        });
        const auto* replay_response = std::get_if<snf::server::PurchaseResponse>(&replay.responses.front().response);
        assert(replay_response != nullptr);
        assert(replay_response->result.status == snf::server::PurchaseStatus::Committed);
        assert(replay_response->result.replayed);
        assert(replay_response->result.currency_balance == 500);

        const auto conflict = actor.handle(snf::server::PurchaseCommand{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 10},
            .product = snf::server::BASIC_PRODUCT,
        });
        const auto* conflict_response = std::get_if<snf::server::PurchaseResponse>(&conflict.responses.front().response);
        assert(conflict_response != nullptr);
        assert(conflict_response->result.status == snf::server::PurchaseStatus::IdempotencyConflict);
        assert(actor.state().currencyBalance() == 500);

        const auto duplicate_purchase = actor.handle(snf::server::PurchaseCommand{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 11},
            .product = snf::server::ARCANE_BOLT_PRODUCT,
        });
        const auto* duplicate_response = std::get_if<snf::server::PurchaseResponse>(&duplicate_purchase.responses.front().response);
        assert(duplicate_response != nullptr);
        assert(duplicate_response->result.status == snf::server::PurchaseStatus::AlreadyOwned);
        assert(duplicate_response->result.currency_balance == 500);

        snf::server::PlayerStateComponentMask purchase_components = 0;
        assert(actor.takeDirtySnapshot(&purchase_components).has_value());
        assert((purchase_components & snf::server::componentMask(snf::server::PlayerStateComponent::Economy)) != 0);
        assert((purchase_components & snf::server::componentMask(snf::server::PlayerStateComponent::Skills)) != 0);

        const auto equipped = actor.handle(snf::server::EquipSkillCommand{.skill_id = snf::server::ARCANE_BOLT_SKILL_ID});
        const auto* equip_response = std::get_if<snf::server::EquipSkillResponse>(&equipped.responses.front().response);
        assert(equip_response != nullptr);
        assert(equip_response->status == snf::server::EquipSkillStatus::Equipped);
        assert(equip_response->equipped_skill_id == snf::server::ARCANE_BOLT_SKILL_ID);
        assert(actor.dirtyComponents() == snf::server::componentMask(snf::server::PlayerStateComponent::Skills));

        const snf::server::PlayerRecord snapshot = actor.snapshot();
        snf::server::Player restored{player};
        restored.restore(snapshot);
        assert(restored.state().getSkillLoadout() == actor.state().getSkillLoadout());

        const auto room_join = restored.handle(snf::server::JoinRoomRequest{.room = snf::server::RoomId{.value = 8}});
        assert(room_join.room_join);
        assert(room_join.room_join->equipped_skill_id == snf::server::ARCANE_BOLT_SKILL_ID);
    }

    void test_equip_skill_reports_unknown_unowned_and_already_equipped()
    {
        const snf::server::PlayerId player{.value = 892};
        snf::server::Player actor{player};

        const auto already_equipped = actor.handle(snf::server::EquipSkillCommand{.skill_id = snf::server::SLASH_SKILL_ID});
        const auto* already_response = std::get_if<snf::server::EquipSkillResponse>(&already_equipped.responses.front().response);
        assert(already_response != nullptr && already_response->status == snf::server::EquipSkillStatus::AlreadyEquipped);

        const auto unowned = actor.handle(snf::server::EquipSkillCommand{.skill_id = snf::server::ARCANE_BOLT_SKILL_ID});
        const auto* unowned_response = std::get_if<snf::server::EquipSkillResponse>(&unowned.responses.front().response);
        assert(unowned_response != nullptr && unowned_response->status == snf::server::EquipSkillStatus::SkillNotOwned);

        const auto unknown = actor.handle(snf::server::EquipSkillCommand{.skill_id = snf::server::SkillId{.value = 999}});
        const auto* unknown_response = std::get_if<snf::server::EquipSkillResponse>(&unknown.responses.front().response);
        assert(unknown_response != nullptr && unknown_response->status == snf::server::EquipSkillStatus::UnknownSkill);
        assert(!actor.hasFlushableDirtyState());
    }

    void test_skill_purchase_checks_ownership_before_funds_and_rejects_insufficient_funds_atomically()
    {
        const snf::server::PlayerId owned_player{.value = 893};
        snf::server::Player owned{owned_player};
        owned.restore(snf::server::PlayerRecord{
            .player = owned_player,
            .currency_balance = 0,
            .skill_loadout = snf::server::SkillLoadout{
                {snf::server::SLASH_SKILL_ID, snf::server::ARCANE_BOLT_SKILL_ID}, snf::server::SLASH_SKILL_ID
            },
        });
        const auto already_owned = owned.handle(snf::server::PurchaseCommand{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 20},
            .product = snf::server::ARCANE_BOLT_PRODUCT,
        });
        const auto* owned_response = std::get_if<snf::server::PurchaseResponse>(&already_owned.responses.front().response);
        assert(owned_response != nullptr);
        assert(owned_response->result.status == snf::server::PurchaseStatus::AlreadyOwned);
        assert(owned.state().currencyBalance() == 0);
        assert(!owned.hasFlushableDirtyState());

        const snf::server::PlayerId poor_player{.value = 894};
        snf::server::Player poor{poor_player};
        poor.restore(snf::server::PlayerRecord{.player = poor_player, .currency_balance = 499});
        const auto insufficient = poor.handle(snf::server::PurchaseCommand{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 21},
            .product = snf::server::ARCANE_BOLT_PRODUCT,
        });
        const auto* poor_response = std::get_if<snf::server::PurchaseResponse>(&insufficient.responses.front().response);
        assert(poor_response != nullptr);
        assert(poor_response->result.status == snf::server::PurchaseStatus::InsufficientFunds);
        assert(!poor.state().getSkillLoadout().hasOwnedSkillId(snf::server::ARCANE_BOLT_SKILL_ID));
        assert(poor.state().currencyBalance() == 499);
        assert(!poor.hasFlushableDirtyState());
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

    assert(result.responses.empty());
    assert(result.room_join);
    assert(result.room_join->room == snf::server::RoomId{.value = 7});
    assert((result.room_join->stats == snf::server::CombatStats{.attack = 11, .health = 110}));
    assert(result.room_join->equipped_skill_id == snf::server::SLASH_SKILL_ID);
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
    test_skill_purchase_and_equip_are_atomic_player_state_changes();
    test_equip_skill_reports_unknown_unowned_and_already_equipped();
    test_skill_purchase_checks_ownership_before_funds_and_rejects_insufficient_funds_atomically();
}
