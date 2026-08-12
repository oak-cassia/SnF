#include "snf/server/player_actor.hpp"

#include <cassert>
#include <cstdint>
#include <variant>

namespace
{
    snf::server::PlayerCommand make_ping(const std::uint32_t request_id)
    {
        return snf::server::PingCommand{
            .request_id = request_id,
            .payload = {},
        };
    }

    // The command has to outlive the task, so it is always a named local here.
    // Handing handle() a temporary would leave the lazy body reading a destroyed
    // command on its first resume.
    snf::server::PlayerResult run_handler(snf::server::PlayerActor& actor,
                                          const snf::server::PlayerCommand& command)
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

        assert(first.effects.size() == 1);
        assert(second.effects.size() == 1);
        const auto* first_send = std::get_if<snf::server::SendResponse>(&first.effects.front());
        const auto* second_send = std::get_if<snf::server::SendResponse>(&second.effects.front());
        assert(first_send != nullptr);
        assert(second_send != nullptr);
        const auto* first_pong = std::get_if<snf::server::PongResponse>(&first_send->response);
        const auto* second_pong = std::get_if<snf::server::PongResponse>(&second_send->response);
        assert(first_pong != nullptr);
        assert(second_pong != nullptr);
        assert(first_pong->request_id == 100);
        assert(second_pong->request_id == 101);
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
            .request_id = 9,
            .player = player,
        };

        const auto result = run_handler(actor, command);
        assert(actor.state().identity() == player);
        assert(result.effects.size() == 1);
        const auto* send = std::get_if<snf::server::SendResponse>(&result.effects.front());
        assert(send != nullptr);
        const auto* authenticated =
            std::get_if<snf::server::AuthenticatedResponse>(&send->response);
        assert(authenticated != nullptr);
        assert(authenticated->request_id == 9);
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
            .ranking_score = 40,
            .last_domain_event_sequence = 2,
        });

        const auto command = make_ping(20);
        static_cast<void>(run_handler(actor, command));
        const auto record = actor.snapshot();
        assert(record.player == player);
        assert(record.handled_command_count == 11);
        assert(record.currency_balance == 700);
        assert(record.purchased_item_count == 3);
        assert(record.ranking_score == 40);
        assert(record.last_domain_event_sequence == 2);
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

    void test_purchase_completion_updates_authoritative_state_and_preserves_it_on_unavailable()
    {
        const snf::server::PlayerId player{.value = 90};
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}};
        const snf::server::PurchaseCommand command{
            .request_id = 30,
            .idempotency_key = snf::server::PurchaseIdempotencyKey{.value = 7},
            .product = snf::server::BASIC_PRODUCT,
        };

        const auto committed =
            actor.completePurchase(command,
                                   snf::server::PurchaseTransactionResult{
                                       .status = snf::server::PurchaseStatus::Committed,
                                       .player = player,
                                       .idempotency_key = command.idempotency_key,
                                       .product = command.product,
                                       .currency_balance = 900,
                                       .purchased_item_count = 1,
                                       .replayed = false,
                                   });
        assert(actor.state().currencyBalance() == 900);
        assert(actor.state().purchasedItemCount() == 1);
        const auto* send = std::get_if<snf::server::SendResponse>(&committed.effects.front());
        assert(send != nullptr);
        const auto* response = std::get_if<snf::server::PurchaseResponse>(&send->response);
        assert(response != nullptr);
        assert(response->result.currency_balance == 900);

        const auto unavailable =
            actor.completePurchase(command,
                                   snf::server::PurchaseTransactionResult{
                                       .status = snf::server::PurchaseStatus::Unavailable,
                                       .player = player,
                                       .idempotency_key = command.idempotency_key,
                                       .product = command.product,
                                       .currency_balance = 0,
                                       .purchased_item_count = 0,
                                       .replayed = false,
                                   });
        assert(actor.state().currencyBalance() == 900);
        assert(actor.state().purchasedItemCount() == 1);
        const auto* unavailable_send =
            std::get_if<snf::server::SendResponse>(&unavailable.effects.front());
        assert(unavailable_send != nullptr);
        const auto* unavailable_response =
            std::get_if<snf::server::PurchaseResponse>(&unavailable_send->response);
        assert(unavailable_response != nullptr);
        assert(unavailable_response->result.currency_balance == 900);
        assert(unavailable_response->result.purchased_item_count == 1);
        assert(actor.state().handledCommandCount() == 2);
    }

    void test_trusted_score_award_updates_state_and_emits_an_absolute_event()
    {
        const snf::server::PlayerId player{.value = 91};
        snf::server::PlayerActor actor{snf::server::PlayerActorId{player}};
        actor.restore(snf::server::PlayerRecord{
            .player = player,
            .handled_command_count = 4,
            .last_location = std::nullopt,
            .currency_balance = 800,
            .purchased_item_count = 2,
            .ranking_score = 100,
            .last_domain_event_sequence = 2,
        });
        const snf::server::PlayerCommand command = snf::server::AwardRankingScoreCommand{
            .request_id = 40,
            .score_delta = 25,
        };

        const auto result = run_handler(actor, command);
        assert(result.effects.size() == 1);
        const auto* publish = std::get_if<snf::server::PublishPlayerEvent>(&result.effects.front());
        assert(publish != nullptr);
        assert((publish->event == snf::server::PlayerDomainEvent{snf::server::PlayerScoreChanged{
                                      .player = player,
                                      .sequence = 3,
                                      .score = 125,
                                  }}));
        assert(actor.state().rankingScore() == 125);
        assert(actor.state().lastDomainEventSequence() == 3);
        assert(actor.state().handledCommandCount() == 5);
        assert(actor.snapshot().ranking_score == 125);
        assert(actor.snapshot().last_domain_event_sequence == 3);
    }
}

void run_player_actor_tests()
{
    test_player_actor_owns_state_and_dispatches_ping();
    test_handler_body_does_not_run_before_the_first_resume();
    test_persistent_player_actor_acknowledges_its_identity();
    test_persistent_player_actor_restores_and_snapshots_state();
    test_purchase_completion_updates_authoritative_state_and_preserves_it_on_unavailable();
    test_trusted_score_award_updates_state_and_emits_an_absolute_event();
}
