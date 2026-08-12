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
}

void run_player_actor_tests()
{
    test_player_actor_owns_state_and_dispatches_ping();
    test_handler_body_does_not_run_before_the_first_resume();
    test_persistent_player_actor_acknowledges_its_identity();
}
