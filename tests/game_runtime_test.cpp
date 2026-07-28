#include "snf/net/unique_file_descriptor.hpp"
#include "snf/runtime/bounded_queue.hpp"
#include "snf/server/game_runtime.hpp"

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <sys/eventfd.h>
#include <utility>
#include <variant>
#include <vector>

namespace
{
    snf::net::UniqueFileDescriptor make_eventfd()
    {
        const int descriptor = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
        assert(descriptor != -1);
        return snf::net::UniqueFileDescriptor{descriptor};
    }

    snf::server::InboundCommand make_ping(const std::uint32_t request_id,
                                          std::vector<std::byte> payload = {})
    {
        return snf::server::InboundCommand{
            .connection = snf::server::ConnectionId{
                .descriptor = 42,
                .generation = 7,
            },
            .frame = snf::protocol::Frame{
                .type = snf::protocol::MessageType::Ping,
                .request_id = request_id,
                .payload = std::move(payload),
            },
        };
    }

    std::vector<snf::server::NetworkAction>
    run_commands(std::vector<snf::server::InboundCommand> commands)
    {
        snf::runtime::BoundedQueue<snf::server::InboundCommand> inbound{8};
        snf::runtime::BoundedQueue<snf::server::NetworkAction> outbound{8};
        const auto event = make_eventfd();

        for (auto& command : commands)
        {
            assert(inbound.tryPush(std::move(command)));
        }
        inbound.close();

        snf::server::GameRuntime runtime{inbound, outbound, event.getDescriptor()};
        runtime.run();

        std::vector<snf::server::NetworkAction> actions;
        while (auto action = outbound.tryPop())
        {
            actions.push_back(std::move(*action));
        }
        return actions;
    }

    void test_turns_ping_into_identical_pong()
    {
        const auto actions = run_commands({make_ping(
            100, {std::byte{0xAA}, std::byte{0xBB}})});

        assert(actions.size() == 2);
        const auto* send = std::get_if<snf::server::SendFrame>(&actions[0]);
        const snf::server::ConnectionId expected_connection{
            .descriptor = 42,
            .generation = 7,
        };
        assert(send != nullptr);
        assert(send->connection == expected_connection);
        assert(send->frame.type == snf::protocol::MessageType::Pong);
        assert(send->frame.request_id == 100);
        assert(send->frame.payload == std::vector<std::byte>({std::byte{0xAA}, std::byte{0xBB}}));
        assert(std::holds_alternative<snf::server::GameRuntimeDrained>(actions[1]));
    }

    void test_preserves_command_order()
    {
        const auto actions = run_commands({make_ping(1), make_ping(2), make_ping(3)});

        assert(actions.size() == 4);
        for (std::uint32_t index = 0; index < 3; ++index)
        {
            const auto* send = std::get_if<snf::server::SendFrame>(&actions[index]);
            assert(send != nullptr);
            assert(send->frame.request_id == index + 1);
        }
        assert(std::holds_alternative<snf::server::GameRuntimeDrained>(actions.back()));
    }

    void test_requests_close_for_an_unregistered_message()
    {
        auto command = make_ping(1);
        command.frame.type = snf::protocol::MessageType::Pong;
        const auto actions = run_commands({std::move(command)});

        assert(actions.size() == 2);
        const auto* close = std::get_if<snf::server::CloseConnection>(&actions[0]);
        assert(close != nullptr);
        assert(close->reason == snf::server::CloseReason::ProtocolError);
        assert(std::holds_alternative<snf::server::GameRuntimeDrained>(actions[1]));
    }

    void test_notifies_when_an_empty_closed_queue_is_drained()
    {
        const auto actions = run_commands({});
        assert(actions.size() == 1);
        assert(std::holds_alternative<snf::server::GameRuntimeDrained>(actions.front()));
    }
}

void run_game_runtime_tests()
{
    test_turns_ping_into_identical_pong();
    test_preserves_command_order();
    test_requests_close_for_an_unregistered_message();
    test_notifies_when_an_empty_closed_queue_is_drained();
}
