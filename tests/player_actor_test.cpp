#include "snf/server/player_actor.hpp"

#include "snf/server/street_progression.hpp"

#include <cassert>
#include <cstdint>
#include <limits>
#include <variant>

namespace
{
    snf::server::PlayerCommand make_ping(const std::uint32_t request_id)
    {
        return snf::server::PingCommand{
            .payload = {},
        };
    }

    // The command has to outlive the task, so it is always a named local here.
    // Handing handle() a temporary would leave the lazy body reading a destroyed
    // command on its first resume.
    snf::server::PlayerResult run_handler(snf::server::PlayerActor& actor, const snf::server::PlayerCommand& command)
    {
        auto task = actor.handle(command);
        assert(task.resume() == snf::runtime::ActorTaskStatus::Completed);
        return task.takeResult();
    }

    void test_player_actor_owns_state_and_dispatches_ping()
    {
        snf::server::PlayerActor actor;
        assert(actor.state().handledCommandCount() == 0);

        const auto first_command = make_ping(100);
        const auto second_command = make_ping(101);
        const auto first = run_handler(actor, first_command);
        const auto second = run_handler(actor, second_command);

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

    // A lazy task is what lets the scheduler own the first resume. Without it a
    // handler would start running on whichever thread happened to create the task.
    void test_handler_body_does_not_run_before_the_first_resume()
    {
        snf::server::PlayerActor actor;
        const auto command = make_ping(1);

        auto task = actor.handle(command);
        assert(task.valid());
        assert(actor.state().handledCommandCount() == 0);

        assert(task.resume() == snf::runtime::ActorTaskStatus::Completed);
        assert(actor.state().handledCommandCount() == 1);
    }

    void test_persistent_player_actor_acknowledges_its_identity()
    {
        const snf::server::PlayerId player{.value = 77};
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}};
        const snf::server::PlayerCommand command = snf::server::AuthenticateCommand{
            .player = player,
        };

        const auto result = run_handler(actor, command);
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
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}};
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

        const auto command = make_ping(20);
        static_cast<void>(run_handler(actor, command));
        const auto record = actor.snapshot();
        assert(record.player == player);
        assert(record.handled_command_count == 11);
        assert(record.currency_balance == 700);
        assert(record.purchased_item_count == 3);
        assert((record.last_location == snf::server::PlayerLocation{
                                            .zone = snf::server::ZoneId{.value = 5},
                                            .position = {.x = 3, .y = -4},
                                        }));

        actor.setLastLocation(snf::server::PlayerLocation{
            .zone = snf::server::ZoneId{.value = 6},
            .position = {.x = 8, .y = 9},
        });
        assert((actor.snapshot().last_location == snf::server::PlayerLocation{
                                                      .zone = snf::server::ZoneId{.value = 6},
                                                      .position = {.x = 8, .y = 9},
                                                  }));
    }

    void test_street_experience_grant_marks_progression_dirty()
    {
        const snf::server::PlayerId player{.value = 91};
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}};
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
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}};

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
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}};
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
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}};

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
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}, 1};
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
        const auto committed = run_handler(actor, first);
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
        assert((cleared & snf::server::componentMask(snf::server::PlayerStateComponent::Economy)) != 0);
        assert(actor.dirtyComponents() == 0);

        const auto replay = run_handler(actor, first);
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
        const auto rejected = run_handler(actor, capacity);
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
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}};
        actor.restore(snf::server::PlayerRecord{
            .player = player,
            .last_location = std::nullopt,
            .currency_balance = 50,
        });

        const snf::server::PurchaseCommand insufficient{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 3},
            .product = snf::server::BASIC_PRODUCT,
        };
        const auto first = run_handler(actor, insufficient);
        const auto* first_send = &first.responses.front();
        assert(first_send != nullptr);
        const auto* first_response = std::get_if<snf::server::PurchaseResponse>(&first_send->response);
        assert(first_response != nullptr);
        assert(first_response->result.status == snf::server::PurchaseStatus::InsufficientFunds);
        assert(!actor.hasFlushableDirtyState());

        const auto replay = run_handler(actor, insufficient);
        const auto* replay_send = &replay.responses.front();
        assert(replay_send != nullptr);
        const auto* replay_response = std::get_if<snf::server::PurchaseResponse>(&replay_send->response);
        assert(replay_response != nullptr);
        assert(replay_response->result.replayed);

        const snf::server::PurchaseCommand unknown{
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 4},
            .product = snf::server::ProductId{.value = 999},
        };
        const auto missing = run_handler(actor, unknown);
        const auto* missing_send = &missing.responses.front();
        assert(missing_send != nullptr);
        const auto* missing_response = std::get_if<snf::server::PurchaseResponse>(&missing_send->response);
        assert(missing_response != nullptr);
        assert(missing_response->result.status == snf::server::PurchaseStatus::ProductNotFound);
    }
}

void run_player_actor_tests()
{
    test_player_actor_owns_state_and_dispatches_ping();
    test_handler_body_does_not_run_before_the_first_resume();
    test_persistent_player_actor_acknowledges_its_identity();
    test_persistent_player_actor_restores_and_snapshots_state();
    test_street_experience_grant_marks_progression_dirty();
    test_street_experience_grants_accumulate_and_saturate();
    test_street_experience_survives_the_record_round_trip();
    test_street_experience_keeps_accumulating_past_the_level_cap();
    test_live_purchase_is_memory_authoritative_and_bounded();
    test_live_purchase_rejects_unknown_and_reports_insufficient_funds();
}
