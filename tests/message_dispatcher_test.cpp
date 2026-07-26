#include "snf/server/message_dispatcher.hpp"

#include <cassert>
#include <cstddef>
#include <vector>

namespace
{
    void test_dispatches_ping_to_the_registered_handler()
    {
        snf::server::MessageDispatcher dispatcher;
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Ping,
            .request_id = 7,
            .payload = {std::byte{0xAA}, std::byte{0xBB}},
        };

        const auto result = dispatcher.dispatch(request);

        assert(result.handled());
        assert(result.responses.size() == 1);
        assert(result.responses[0].type == snf::protocol::MessageType::Pong);
        assert(result.responses[0].request_id == request.request_id);
        assert(result.responses[0].payload == request.payload);
    }

    void test_reports_a_missing_handler()
    {
        const snf::server::MessageDispatcher dispatcher;
        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 1,
            .payload = {},
        };

        const auto result = dispatcher.dispatch(request);

        assert(!result.handled());
        assert(result.status == snf::server::DispatchStatus::HandlerNotFound);
        assert(result.responses.empty());
    }

    void test_registers_an_additional_handler()
    {
        snf::server::MessageDispatcher dispatcher;
        const bool registered = dispatcher.registerHandler(
            snf::protocol::MessageType::Pong,
            [](const snf::protocol::Frame&) { return std::vector<snf::protocol::Frame>{}; });

        const snf::protocol::Frame request{
            .type = snf::protocol::MessageType::Pong,
            .request_id = 1,
            .payload = {},
        };
        const auto result = dispatcher.dispatch(request);

        assert(registered);
        assert(result.handled());
        assert(result.responses.empty());
    }

    void test_rejects_a_duplicate_handler()
    {
        snf::server::MessageDispatcher dispatcher;

        const bool registered = dispatcher.registerHandler(
            snf::protocol::MessageType::Ping,
            [](const snf::protocol::Frame&) { return std::vector<snf::protocol::Frame>{}; });

        assert(!registered);
    }
}

void run_message_dispatcher_tests()
{
    test_dispatches_ping_to_the_registered_handler();
    test_reports_a_missing_handler();
    test_registers_an_additional_handler();
    test_rejects_a_duplicate_handler();
}
