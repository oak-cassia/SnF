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

    void test_player_actor_owns_state_and_dispatches_ping()
    {
        snf::server::PlayerActor actor;
        assert(actor.state().handledCommandCount() == 0);

        const auto first = actor.handle(make_ping(100));
        const auto second = actor.handle(make_ping(101));

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
}

void run_player_actor_tests()
{
    test_player_actor_owns_state_and_dispatches_ping();
}
